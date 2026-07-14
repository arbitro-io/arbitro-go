// Package ackrel implements the client-side ack-reliability hot tier (G01):
// per-consumer deferred-ack tracking so an Ack that fails to reach the wire
// (write-channel backpressure, mid-flight disconnect) is retried instead of
// silently lost, plus a bounded redelivery-dedup cache (G18).
//
// Mirrors arbitro-client-tokio's ackrel::{AckRelay, ConsumerPending, SeenCache}.
//
// Intentionally out of scope: the Rust cold tier (ackrel/cold.rs, SQLite WAL
// persistence for ack survival across process restart) is not ported here.
// Consumers requiring restart durability should design idempotent handlers.
package ackrel

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// consumerState tracks the deferred (not-yet-broker-confirmed) seqs for one
// consumer, plus the generation counter bumped on reconnect. A generation
// change wipes the pending set wholesale — deferred acks don't survive a
// session change (matches Rust AckRelay::ensure's purge invariant).
//
// oldestTs is the insertion time of the oldest currently-pending seq — used
// by the TTL sweep (ExpireOlderThan) exactly like the Rust ConsumerPending's
// oldest_ts_ms: rather than tracking a per-seq timestamp, the whole set is
// considered expired together once its oldest entry crosses the TTL.
type consumerState struct {
	mu         sync.Mutex
	generation uint32
	pending    map[uint64]struct{}
	oldestTs   time.Time
}

// Relay is one instance per Client (NOT per Connection — it must survive
// reconnects so deferred acks recorded against the old connection are
// replayed against the new one).
type Relay struct {
	mu   sync.RWMutex
	byID map[uint32]*consumerState

	// send is invoked by the sweep goroutine to flush a consumer's deferred
	// seqs as an AckBatch frame. Callers (Connection dispatch, reconnect
	// replay) may also invoke it directly.
	send func(consumerID, generation uint32, seqs []uint64)

	sweepInterval time.Duration
	expireTTL     atomic.Int64 // time.Duration; 0 disables the TTL sweep
	stopCh        chan struct{}
	stopOnce      sync.Once

	// Metrics (G14) — cumulative counters read by Client.Metrics().
	confirmedTotal atomic.Uint64
	expiredTotal   atomic.Uint64
}

// NewRelay creates a Relay. send transmits an AckBatch frame for
// (consumerID, generation, seqs) — normally a closure over the client's
// current Connection. sweepInterval <= 0 disables the background sweep
// goroutine (tests can drive Record/Confirm directly without a ticker).
func NewRelay(sweepInterval time.Duration, send func(consumerID, generation uint32, seqs []uint64)) *Relay {
	r := &Relay{
		byID:          make(map[uint32]*consumerState),
		send:          send,
		sweepInterval: sweepInterval,
		stopCh:        make(chan struct{}),
	}
	if sweepInterval > 0 {
		go r.sweepLoop()
	}
	return r
}

func (r *Relay) state(consumerID uint32) *consumerState {
	r.mu.RLock()
	st, ok := r.byID[consumerID]
	r.mu.RUnlock()
	if ok {
		return st
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if st, ok := r.byID[consumerID]; ok {
		return st
	}
	st = &consumerState{pending: make(map[uint64]struct{})}
	r.byID[consumerID] = st
	return st
}

func (r *Relay) stateIfExists(consumerID uint32) *consumerState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byID[consumerID]
}

// Record marks seq as deferred (not yet broker-confirmed) for consumerID.
// Called by the AckBatcher when a flush's Send() fails, so the ack isn't
// silently dropped — the sweep goroutine (or reconnect replay) retries it.
func (r *Relay) Record(consumerID uint32, seq uint64) {
	st := r.state(consumerID)
	st.mu.Lock()
	if _, exists := st.pending[seq]; !exists {
		st.pending[seq] = struct{}{}
		if st.oldestTs.IsZero() {
			st.oldestTs = time.Now()
		}
	}
	st.mu.Unlock()
}

// Confirm removes exactly the given seqs from the deferred set for
// consumerID (used when the broker's confirmation isn't a simple cumulative
// cursor).
func (r *Relay) Confirm(consumerID uint32, seqs []uint64) {
	st := r.stateIfExists(consumerID)
	if st == nil {
		return
	}
	st.mu.Lock()
	removed := 0
	for _, s := range seqs {
		if _, ok := st.pending[s]; ok {
			delete(st.pending, s)
			removed++
		}
	}
	if len(st.pending) == 0 {
		st.oldestTs = time.Time{}
	}
	st.mu.Unlock()
	if removed > 0 {
		r.confirmedTotal.Add(uint64(removed))
	}
}

// ConfirmUpTo purges every deferred seq <= newCursor for consumerID —
// mirrors AckRelay::purge_up_to's cumulative-cursor semantics, used by the
// AckBatchResp/AckStateRep dispatch handlers. Returns the number removed.
func (r *Relay) ConfirmUpTo(consumerID uint32, newCursor uint64) int {
	st := r.stateIfExists(consumerID)
	if st == nil {
		return 0
	}
	st.mu.Lock()
	removed := 0
	for s := range st.pending {
		if s <= newCursor {
			delete(st.pending, s)
			removed++
		}
	}
	if len(st.pending) == 0 {
		st.oldestTs = time.Time{}
	}
	st.mu.Unlock()
	if removed > 0 {
		r.confirmedTotal.Add(uint64(removed))
	}
	return removed
}

// PendingSeqs returns every deferred seq for consumerID, ascending — used
// for reconnect replay (AckStateReq flow) and by the sweep goroutine.
func (r *Relay) PendingSeqs(consumerID uint32) []uint64 {
	st := r.stateIfExists(consumerID)
	if st == nil {
		return nil
	}
	st.mu.Lock()
	out := make([]uint64, 0, len(st.pending))
	for s := range st.pending {
		out = append(out, s)
	}
	st.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Generation returns the current generation counter for consumerID (0 if it
// has never recorded a deferred ack or had its generation set).
func (r *Relay) Generation(consumerID uint32) uint32 {
	st := r.stateIfExists(consumerID)
	if st == nil {
		return 0
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.generation
}

// EnsureGeneration reconciles the local generation counter against the
// broker's authoritative value (from an AckStateRep reply). A mismatch wipes
// the pending set wholesale, matching AckRelay::ensure.
func (r *Relay) EnsureGeneration(consumerID uint32, generation uint32) {
	st := r.state(consumerID)
	st.mu.Lock()
	if st.generation != generation {
		st.generation = generation
		st.pending = make(map[uint64]struct{})
		st.oldestTs = time.Time{}
	}
	st.mu.Unlock()
}

// BumpGeneration increments the generation counter for consumerID and wipes
// its deferred-ack set. Called by the reconnect supervisor: deferred acks
// don't survive a session change.
func (r *Relay) BumpGeneration(consumerID uint32) uint32 {
	st := r.state(consumerID)
	st.mu.Lock()
	st.generation++
	st.pending = make(map[uint64]struct{})
	st.oldestTs = time.Time{}
	gen := st.generation
	st.mu.Unlock()
	return gen
}

// ActiveConsumers returns the consumer IDs with at least one deferred ack —
// feeds the reconnect replay flow (one AckStateReq per active consumer).
func (r *Relay) ActiveConsumers() []uint32 {
	r.mu.RLock()
	ids := make([]uint32, 0, len(r.byID))
	for cid := range r.byID {
		ids = append(ids, cid)
	}
	r.mu.RUnlock()

	out := make([]uint32, 0, len(ids))
	for _, cid := range ids {
		st := r.stateIfExists(cid)
		if st == nil {
			continue
		}
		st.mu.Lock()
		n := len(st.pending)
		st.mu.Unlock()
		if n > 0 {
			out = append(out, cid)
		}
	}
	return out
}

// HotCount returns the total number of deferred-ack entries across every
// consumer — feeds the acks_deferred metrics gauge (G14).
func (r *Relay) HotCount() uint64 {
	r.mu.RLock()
	ids := make([]uint32, 0, len(r.byID))
	for cid := range r.byID {
		ids = append(ids, cid)
	}
	r.mu.RUnlock()

	var total uint64
	for _, cid := range ids {
		st := r.stateIfExists(cid)
		if st == nil {
			continue
		}
		st.mu.Lock()
		total += uint64(len(st.pending))
		st.mu.Unlock()
	}
	return total
}

// ConfirmedTotal returns the cumulative count of hot-tier entries purged by
// Confirm/ConfirmUpTo (broker confirmation) — feeds the acks_confirmed
// metrics counter (G14).
func (r *Relay) ConfirmedTotal() uint64 {
	return r.confirmedTotal.Load()
}

// ExpiredTotal returns the cumulative count of hot-tier entries dropped by
// the TTL sweep without ever being broker-confirmed — feeds the
// acks_expired metrics counter (G14).
func (r *Relay) ExpiredTotal() uint64 {
	return r.expiredTotal.Load()
}

// SetExpireTTL enables (ttl > 0) or disables (ttl <= 0) the TTL sweep: a
// consumer's entire deferred set is dropped, uncounted as confirmed, once
// its oldest entry has been pending longer than ttl. Mirrors the Rust hot
// tier's ConsumerPending::expire_older_than (pending.rs) — same
// whole-set-at-once semantics, since only the oldest timestamp is tracked
// per consumer, not per seq.
func (r *Relay) SetExpireTTL(ttl time.Duration) {
	r.expireTTL.Store(int64(ttl))
}

// ExpireOlderThan drops the entire deferred set for any consumer whose
// oldest pending entry is older than ttl, and returns the total number of
// entries dropped. Safe to call directly (e.g. from tests) independent of
// the sweep goroutine's periodic invocation.
func (r *Relay) ExpireOlderThan(ttl time.Duration) int {
	if ttl <= 0 {
		return 0
	}
	cutoff := time.Now().Add(-ttl)

	r.mu.RLock()
	ids := make([]uint32, 0, len(r.byID))
	for cid := range r.byID {
		ids = append(ids, cid)
	}
	r.mu.RUnlock()

	total := 0
	for _, cid := range ids {
		st := r.stateIfExists(cid)
		if st == nil {
			continue
		}
		st.mu.Lock()
		if !st.oldestTs.IsZero() && st.oldestTs.Before(cutoff) {
			removed := len(st.pending)
			st.pending = make(map[uint64]struct{})
			st.oldestTs = time.Time{}
			total += removed
		}
		st.mu.Unlock()
	}
	if total > 0 {
		r.expiredTotal.Add(uint64(total))
	}
	return total
}

// Close stops the sweep goroutine. Safe to call multiple times.
func (r *Relay) Close() {
	r.stopOnce.Do(func() { close(r.stopCh) })
}

func (r *Relay) sweepLoop() {
	ticker := time.NewTicker(r.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.sweepOnce()
			if ttl := time.Duration(r.expireTTL.Load()); ttl > 0 {
				r.ExpireOlderThan(ttl)
			}
		}
	}
}

func (r *Relay) sweepOnce() {
	r.mu.RLock()
	ids := make([]uint32, 0, len(r.byID))
	for cid := range r.byID {
		ids = append(ids, cid)
	}
	r.mu.RUnlock()

	for _, cid := range ids {
		seqs := r.PendingSeqs(cid)
		if len(seqs) == 0 {
			continue
		}
		if r.send == nil {
			continue
		}
		gen := r.Generation(cid)
		r.send(cid, gen, seqs)
	}
}
