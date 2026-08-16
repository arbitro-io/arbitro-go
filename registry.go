package arbitro

import (
	"bytes"
	"sync"

	"github.com/arbitro-io/arbitro-go/internal/ackstore"
)

// Subject routing for delivered frames.
//
// The broker sends ONE wire copy per (connection, consumer) and stamps it
// with the subscription id that won the round. Several subscriptions may
// share a consumer, each with its own filter, so the client fans that copy
// out locally to every sibling whose filter matches the subject — and each
// sibling acks its own id, because the broker opens one pending per
// subscription.
//
// Lookup is one map hit keyed by the delivered sub id. The siblings hang off
// the same route as a contiguous slice, so the fan-out is a walk over shared
// memory with no second lookup per sibling.

// matchKind picks the cheapest test a filter allows.
type matchKind uint8

const (
	// matchAll: no filter — every subject belongs to this subscription.
	matchAll matchKind = iota
	// matchExact: no wildcard — one bytes.Equal.
	matchExact
	// matchPattern: contains `*` or `>` — token walk.
	matchPattern
)

// matcher decides whether a subject belongs to a subscription. The filter is
// classified once at subscribe time so the hot path branches on a byte, not
// on a re-scan for wildcards.
type matcher struct {
	kind    matchKind
	pattern []byte
}

func newMatcher(filter []byte) matcher {
	if len(filter) == 0 {
		return matcher{kind: matchAll}
	}
	if bytes.IndexByte(filter, '*') < 0 && bytes.IndexByte(filter, '>') < 0 {
		return matcher{kind: matchExact, pattern: filter}
	}
	return matcher{kind: matchPattern, pattern: filter}
}

// accepts reports whether subject belongs to this subscription. Byte
// comparison only — the client never sees the broker's subject hash, so
// there is nothing to compare but the bytes themselves.
func (m *matcher) accepts(subject []byte) bool {
	switch m.kind {
	case matchAll:
		return true
	case matchExact:
		return bytes.Equal(m.pattern, subject)
	default:
		return patternMatch(m.pattern, subject)
	}
}

// patternMatch walks pattern and subject token by token. `*` takes exactly
// one token, `>` takes the rest and must be last. Allocation-free: both
// sides are sliced in place, never split into a slice of tokens.
func patternMatch(pattern, subject []byte) bool {
	for {
		if len(pattern) == 0 {
			return len(subject) == 0
		}
		pTok, pRest := nextToken(pattern)
		if len(pTok) == 1 && pTok[0] == '>' {
			return true
		}
		if len(subject) == 0 {
			return false
		}
		sTok, sRest := nextToken(subject)
		if !(len(pTok) == 1 && pTok[0] == '*') && !bytes.Equal(pTok, sTok) {
			return false
		}
		pattern, subject = pRest, sRest
	}
}

// nextToken splits off the leading dot-delimited token.
func nextToken(b []byte) (tok, rest []byte) {
	if i := bytes.IndexByte(b, '.'); i >= 0 {
		return b[:i], b[i+1:]
	}
	return b, nil
}

// route is what a delivered sub id resolves to: the consumer it belongs to
// and every subscription sharing that consumer on this connection.
type route struct {
	consumerID uint32
	fanout     bool
	// idx is this route's own position in subs — the queue-mode target,
	// resolved without searching.
	idx  int
	slot ackstore.SlotRef
	// subs is shared by every sibling route. Rebuilt on register/remove
	// (cold path) so the hot path reads an immutable slice.
	subs []*Subscription
}

// registry maps delivered subscription ids to their routes.
//
// A plain map under an RWMutex rather than sync.Map: the hot path is
// read-only and uncontended (one reader goroutine), while sync.Map pays an
// atomic load per access and boxes every value in an interface.
type registry struct {
	mu      sync.RWMutex
	bySub   map[uint32]*route
	byConsu map[uint32][]*Subscription
}

func newRegistry() *registry {
	return &registry{
		bySub:   make(map[uint32]*route),
		byConsu: make(map[uint32][]*Subscription),
	}
}

// register adds a subscription and rebuilds the shared sibling slice for
// its consumer. Cold path — one allocation per subscribe, none per message.
func (r *registry) register(sub *Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()

	siblings := append(r.byConsu[sub.consumerID], sub)
	r.byConsu[sub.consumerID] = siblings
	r.rebuild(sub.consumerID, siblings)
}

// setFanout flips the delivery mode for every subscription of a consumer.
// The broker reports it on the Subscribe reply, after the routes exist.
func (r *registry) setFanout(consumerID uint32, fanout bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, sub := range r.byConsu[consumerID] {
		if rt := r.bySub[sub.id]; rt != nil {
			rt.fanout = fanout
		}
	}
}

// remove drops one subscription. When it was the last of its consumer the
// consumer entry goes too, so nothing outlives its subscriptions.
func (r *registry) remove(subID uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rt, ok := r.bySub[subID]
	if !ok {
		return
	}
	delete(r.bySub, subID)

	siblings := r.byConsu[rt.consumerID]
	kept := siblings[:0]
	for _, s := range siblings {
		if s.id != subID {
			kept = append(kept, s)
		}
	}
	if len(kept) == 0 {
		delete(r.byConsu, rt.consumerID)
		return
	}
	r.byConsu[rt.consumerID] = kept
	r.rebuild(rt.consumerID, kept)
}

// rebuild re-points every sibling route of a consumer at the current slice.
// Caller holds the write lock.
func (r *registry) rebuild(consumerID uint32, siblings []*Subscription) {
	fanout := false
	if len(siblings) > 0 {
		if rt := r.bySub[siblings[0].id]; rt != nil {
			fanout = rt.fanout
		}
	}
	for i, s := range siblings {
		r.bySub[s.id] = &route{
			consumerID: consumerID,
			fanout:     fanout,
			idx:        i,
			slot:       s.slot,
			subs:       siblings,
		}
	}
}

// lookup resolves a delivered sub id. Returns the route and, for queue mode,
// the single subscription that owns the copy.
func (r *registry) lookup(subID uint32) *route {
	r.mu.RLock()
	rt := r.bySub[subID]
	r.mu.RUnlock()
	return rt
}

// consumerSubs returns the siblings of a consumer. Used by the reconnect
// supervisor and the ackstore cursor replay, both cold paths.
func (r *registry) consumerSubs(consumerID uint32) []*Subscription {
	r.mu.RLock()
	subs := r.byConsu[consumerID]
	r.mu.RUnlock()
	return subs
}

// each walks every live subscription. Cold path: reconnect and teardown.
func (r *registry) each(f func(*Subscription) bool) {
	r.mu.RLock()
	snapshot := make([]*Subscription, 0, len(r.bySub))
	for _, subs := range r.byConsu {
		snapshot = append(snapshot, subs...)
	}
	r.mu.RUnlock()

	for _, s := range snapshot {
		if !f(s) {
			return
		}
	}
}

// consumers returns one entry per distinct consumer. Cold path.
func (r *registry) consumers() []uint32 {
	r.mu.RLock()
	out := make([]uint32, 0, len(r.byConsu))
	for cid := range r.byConsu {
		out = append(out, cid)
	}
	r.mu.RUnlock()
	return out
}
