package arbitro

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/arbitro-io/arbitro-go/internal/ackstore"
	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// subChanCap is the buffer size of the Subscription delivery channel.
// Matches the Rust client's mpsc::channel(4096) in state/subscriptions.rs —
// small caps (previously 256) caused the reader goroutine to block on
// deliverToSub whenever a consumer fell one batch behind, serializing acks
// (Go-HP4 audit finding).
const subChanCap = 4096

// Subscription represents an active message subscription.
//
// `id` is chosen by this client, unique per connection, and is what the
// broker stamps on deliveries and expects back on acks. Several
// subscriptions may share one consumer, each with its own filter; the
// broker sends a single wire copy and this client fans it out to the
// siblings whose filter matches.
type Subscription struct {
	client     *Client
	id         uint32
	consumerID uint32
	match      matcher
	ch         chan *Msg
	handler    func(*Msg)
	closed     chan struct{}
	closeOnce  sync.Once
	slot       ackstore.SlotRef // redelivery-dedup handle (nil if dedup disabled)
}

// ID returns the subscription id this client assigned.
func (s *Subscription) ID() uint32 { return s.id }

// Messages returns the delivery channel. Range over it for push-mode consumption.
// The channel is closed when the subscription is closed or the connection drops.
func (s *Subscription) Messages() <-chan *Msg {
	return s.ch
}

// Fetch pulls up to count messages with a timeout. Returns partial results on timeout.
func (s *Subscription) Fetch(ctx context.Context, count int) ([]*Msg, error) {
	msgs := make([]*Msg, 0, count)
	for i := 0; i < count; i++ {
		select {
		case msg, ok := <-s.ch:
			if !ok {
				return msgs, nil
			}
			msgs = append(msgs, msg)
		case <-ctx.Done():
			return msgs, ctx.Err()
		}
	}
	return msgs, nil
}

// Close stops the subscription and unsubscribes from the broker.
func (s *Subscription) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.client.unregisterSubscription(s.id)
		// Send Unsubscribe frame
		seq := s.client.getConn().NextSeq()
		frame, _ := proto.EncodeUnsubscribe(seq, s.consumerID)
		_ = s.client.getConn().TrySend(frame)
		s.client.activeSubs.Add(^uint64(0)) // decrement
		close(s.ch)
	})
}

// closeLocal terminates the subscription without a network round trip. Used
// by the reconnect supervisor (G02) when the client is permanently dead
// (reconnect disabled, or max retries exhausted) — there is no live
// connection to send an Unsubscribe frame over.
func (s *Subscription) closeLocal() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.client.unregisterSubscription(s.id)
		s.client.activeSubs.Add(^uint64(0)) // decrement
		close(s.ch)
	})
}

// ReplyToMagic is the leading byte of an encoded reply_to field.
const ReplyToMagic = 0xFF

// Msg represents a delivered message. Zero-copy: Subject/Data are slices into the frame buffer.
type Msg struct {
	frame      []byte
	consumerID uint32
	// subID is the subscription this copy belongs to — not necessarily the
	// one the broker stamped on the wire, since a fanned-out sibling carries
	// its own id. It is what the ack echoes back.
	subID      uint32
	seq        uint64
	subjectOff int
	subjectLen int
	replyToOff int
	replyToLen int
	payloadOff int
	payloadLen int
	client     *Client
	slot       ackstore.SlotRef // dedup handle for this message's consumer (may be nil)
	acked      bool
}

// Subject returns the message subject as a string.
func (m *Msg) Subject() string {
	return string(m.frame[m.subjectOff : m.subjectOff+m.subjectLen])
}

// SubjectBytes returns the raw subject bytes (no allocation).
func (m *Msg) SubjectBytes() []byte {
	return m.frame[m.subjectOff : m.subjectOff+m.subjectLen]
}

// Data returns the message payload (zero-copy slice into frame buffer).
func (m *Msg) Data() []byte {
	return m.frame[m.payloadOff : m.payloadOff+m.payloadLen]
}

// ReplyTo returns the raw reply_to bytes (empty if none).
func (m *Msg) ReplyTo() []byte {
	if m.replyToLen == 0 {
		return nil
	}
	return m.frame[m.replyToOff : m.replyToOff+m.replyToLen]
}

// reply sends a response to the requester by decoding the reply_to field.
// Format: [0xFF][stream_id LE u32][subject bytes].
// No-op if there is no reply_to or the format is invalid.
//
// Unexported on purpose: this is how the service dispatcher ships a
// handler's return value, not something callers drive themselves. Only
// Service.Request attaches a reply_to, so on any other delivery this can
// do nothing at all — and a method that silently does nothing is worse
// than no method. Handlers answer by returning bytes; the framework then
// pairs the reply with exactly one ack or nack, which hand-rolled
// request/reply cannot guarantee.
func (m *Msg) reply(payload []byte) {
	rt := m.ReplyTo()
	if len(rt) < 6 || rt[0] != ReplyToMagic {
		return
	}
	targetStreamID := binary.LittleEndian.Uint32(rt[1:5])
	replySubject := rt[5:]
	seq := m.client.getConn().NextSeq()
	frame := proto.EncodePublish(seq, targetStreamID, replySubject, nil, payload, proto.FlagAckReq)
	_ = m.client.getConn().TrySend(frame)
}

// Seq returns the delivery sequence number.
func (m *Msg) Seq() uint64 {
	return m.seq
}

// ConsumerID returns the consumer that received this message.
func (m *Msg) ConsumerID() uint32 {
	return m.consumerID
}

// Dup returns true if this is a redelivery.
func (m *Msg) Dup() bool {
	hdr := proto.DecodeHeader(m.frame)
	return hdr.Flags&proto.FlagDup != 0
}

// Ack acknowledges the message (batched for throughput — flushed on drain).
// Also records the seq in the redelivery-dedup store (G18) so a future
// redelivery of this message is recognized and skipped. The store write is a
// buffered append; a background goroutine flushes it durably.
func (m *Msg) Ack() {
	if m.acked {
		return
	}
	m.acked = true
	m.client.getConn().AckBatch.Ack(m.consumerID, m.subID, m.seq)
	m.client.acksSent.Add(1)
	if m.slot != nil {
		_ = m.slot.Record(m.seq)
	}
}

// Nack negatively acknowledges — broker requeues immediately.
func (m *Msg) Nack() {
	if m.acked {
		return
	}
	m.acked = true
	seq := m.client.getConn().NextSeq()
	frame := proto.EncodeNack(seq, m.consumerID, m.subID, m.seq)
	_ = m.client.getConn().TrySend(frame)
	m.client.nacksSent.Add(1)
}

// NackDelay negatively acknowledges with a redelivery delay.
func (m *Msg) NackDelay(d time.Duration) {
	if m.acked {
		return
	}
	m.acked = true
	seq := m.client.getConn().NextSeq()
	entry := proto.NackEntry{
		Seq:     m.seq,
		SubID:   m.subID,
		DelayMs: uint32(d.Milliseconds()),
	}
	frame := proto.EncodeBatchNack(seq, m.consumerID, []proto.NackEntry{entry})
	_ = m.client.getConn().TrySend(frame)
	m.client.nacksSent.Add(1)
}

// Copy creates a long-lived copy of the message data (escapes sync.Pool lifecycle).
func (m *Msg) Copy() MsgCopy {
	subj := make([]byte, m.subjectLen)
	copy(subj, m.frame[m.subjectOff:m.subjectOff+m.subjectLen])
	data := make([]byte, m.payloadLen)
	copy(data, m.frame[m.payloadOff:m.payloadOff+m.payloadLen])
	return MsgCopy{
		Subject: string(subj),
		Data:    data,
		Seq:     m.seq,
	}
}

// MsgCopy is a heap-allocated copy safe to hold indefinitely.
type MsgCopy struct {
	Subject string
	Data    []byte
	Seq     uint64
}

// QueueOptions tunes a durable work queue. The zero value is the common
// case: Group defaults to the stream name, no subject filter, broker-default
// redelivery deadline, unlimited in-flight.
//
// AckPolicy is deliberately absent — a work queue is always AckExplicit.
// Letting a caller pick AckNone would build a queue that silently drops jobs
// when a worker dies, which is the one thing a queue exists to prevent.
type QueueOptions struct {
	// Queue identity. Workers sharing it share one round-robin queue; a
	// different value is a separate, independent durable queue. Empty means
	// the stream name.
	//
	// This single value becomes both the durable consumer name and the queue
	// group — they always move together, so two workers can never disagree
	// on the consumer config.
	Group string

	// Subject filter for this subscription. Empty means every subject in the
	// stream. Applied per-subscription, so workers on one queue may each
	// narrow to a different slice without colliding.
	Filter string

	// How long the broker waits for an ack before handing the job back to
	// the queue. Zero keeps the broker default. Raise it for handlers that
	// legitimately run long, otherwise a slow success is indistinguishable
	// from a crashed worker.
	AckWait time.Duration

	// Cap on delivered-but-unacked messages across the queue — the
	// backpressure valve. Zero is unlimited.
	MaxInflight uint16

	// Where a brand-new queue starts reading. Ignored on later joins, when
	// the durable cursor already exists and the queue resumes from it.
	//
	// uint8 because that is the width of the wire field -- a wider value
	// would fail serde decode broker-side and surface as a generic
	// InternalError with no clue as to the cause.
	DeliverPolicy uint8
	StartSeq      uint64

	// Per-subject in-flight caps. Each distinct subject keeps its own count.
	MaxSubjectInflights []SubjectLimit
}

// QueueSubscribe joins a durable work queue on stream in one call. Every
// worker calls it with the same Group and the broker load-balances between
// them, so each message is delivered to exactly ONE member of the queue.
//
// A DIFFERENT Group is a separate, independent durable queue over the same
// stream — its own cursor, its own copy of the messages.
//
// The queue is durable and explicit-ack: it survives broker restarts and
// worker disconnects, and redelivers anything a worker took but never acked.
// Messages must be acked.
//
//	sub, err := c.QueueSubscribe(ctx, "orders",
//	    arbitro.QueueOptions{Filter: "orders.>"},
//	    arbitro.WithHandler(func(m *arbitro.Msg) { process(m); m.Ack() }))
func (c *Client) QueueSubscribe(ctx context.Context, stream string, q QueueOptions, opts ...SubscribeOption) (*Subscription, error) {
	group := q.Group
	if group == "" {
		group = stream
	}
	// A negative AckWait would wrap to a huge value in the u32 wire field;
	// treat it as unset rather than as ~49 days.
	ackWait := q.AckWait
	if ackWait < 0 {
		ackWait = 0
	}
	// The subject filter belongs to the subscription, not the consumer:
	// workers on one queue may narrow differently, and a consumer-level
	// filter would durably record the first joiner's view for everyone. So
	// the consumer is created with an empty subject and the filter is
	// applied to the Subscribe frame only, matching the Rust and C clients.
	return c.Subscribe(ctx, stream, ConsumerConfig{
		Name:                group,
		Group:               group,
		Filter:              "",
		Fanout:              false,
		AckPolicy:           AckExplicit,
		AckWait:             ackWait,
		MaxInflight:         q.MaxInflight,
		DeliverPolicy:       uint32(q.DeliverPolicy),
		StartSeq:            q.StartSeq,
		MaxSubjectInflights: q.MaxSubjectInflights,
	}, append(opts, withSubFilter(q.Filter))...)
}

// Subscribe creates a consumer (if needed) and starts receiving messages.
func (c *Client) Subscribe(ctx context.Context, stream string, cfg ConsumerConfig, opts ...SubscribeOption) (*Subscription, error) {
	so := subscribeOptions{}
	for _, fn := range opts {
		fn(&so)
	}

	// Resolve or create consumer
	consumerID, err := c.ensureConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, err
	}

	sub := &Subscription{
		client:     c,
		id:         c.allocSubID(),
		consumerID: consumerID,
		ch:         make(chan *Msg, subChanCap),
		handler:    so.handler,
		closed:     make(chan struct{}),
	}

	// Resolve the redelivery-dedup slot for this (stream, consumer) pair.
	// Keyed by the DURABLE names — survives consumer delete+recreate. Nil if
	// dedup is disabled. Errors here are non-fatal: dedup degrades to
	// at-least-once rather than failing the subscribe.
	if c.ackStore != nil {
		consumerName := cfg.Name
		if consumerName == "" {
			consumerName = stream
		}
		if slot, serr := c.ackStore.Slot(c.prefixed(stream), consumerName); serr == nil {
			sub.slot = slot
		}
	}

	// Send Subscribe frame. QueueSubscribe supplies the filter here rather
	// than in cfg so the consumer itself is created with an empty subject.
	subFilter := cfg.Filter
	if so.subFilter != nil {
		subFilter = *so.subFilter
	}
	var filters [][]byte
	if subFilter != "" {
		filters = [][]byte{[]byte(c.prefixed(subFilter))}
	}

	// The matcher decides, on the delivery path, whether a fanned-out copy
	// belongs to this subscription. Classified once, here, so the hot path
	// never re-scans the filter for wildcards.
	if len(filters) > 0 {
		sub.match = newMatcher(filters[0])
	}

	// Register subscription in dispatch table, and stash the filters so the
	// reconnect supervisor (G02) can replay this Subscribe against a freshly
	// redialed connection.
	c.registerSubscription(sub, filters)
	c.activeSubs.Add(1)

	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeSubscribe(seq, consumerID, sub.id, filters)
	if err != nil {
		return nil, err
	}
	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return nil, err
	}

	// The broker reports the delivery mode in the RepOk ref_seq: bit 63 set
	// means fanout. Queue mode has exactly one target per copy; fanout means
	// this client must hand each copy to every sibling whose filter matches.
	if len(reply) >= proto.HeaderSize+8 {
		refSeq := proto.RepOkRefSeq(reply[proto.HeaderSize:])
		c.subs.setFanout(consumerID, refSeq>>63 == 1)
	}

	// On-connect ackstore purge: the store may hold entries recorded by a
	// previous, dead session (its AckBatchResp never arrived, so nothing
	// confirmed them). Ask the broker for its authoritative ack cursor ONCE,
	// now that the subscribe is confirmed (the consumer provably exists
	// server-side); handleAckStateRep drops every entry at or below that
	// cursor. Cold path — one 24 B fire-and-forget frame per subscribe,
	// nothing on the delivery/ack hot path. Reconnects are covered
	// separately by replayAckState.
	if sub.slot != nil {
		cn := c.getConn()
		var gen uint32
		if c.ackRelay != nil {
			gen = c.ackRelay.Generation(consumerID)
		}
		reqSeq := cn.NextSeq()
		_ = cn.TrySend(proto.EncodeAckStateReq(reqSeq, consumerID, gen))
	}

	return sub, nil
}

// resolveConsumerNaming applies the consumer naming defaults shared by every
// arbitro client (arbitro-ts client.ts:521 is the reference):
//
//	name  = cfg.Name  else stream
//	group = cfg.Group else name else stream
//
// The group default is UNCONDITIONAL — it does NOT depend on the delivery
// mode. Two reasons:
//
//   - An empty group is never a useful request. On the broker an empty group
//     in queue mode allocates a real shared queue keyed (stream_id, ""), so
//     every no-group queue consumer on that stream silently lands in one
//     anonymous queue together. The broker now rejects an empty group
//     outright; filling it in is the client's job.
//   - Making the default conditional on the mode (the previous
//     `if !cfg.Fanout && group == ""` form) left fanout consumers sending an
//     empty group on the wire, which is exactly the request the broker
//     rejects.
//
// Defaulting the group to the consumer name is safe in fanout mode: the group
// only partitions delivery among consumers that SHARE it, and the consumer
// name is unique per stream, so a name-derived group is a group of one and
// fanout delivery is unchanged.
func resolveConsumerNaming(cfgName, cfgGroup, stream string) (name, group string) {
	name = cfgName
	if name == "" {
		name = stream
	}
	group = cfgGroup
	if group == "" {
		group = name
	}
	return name, group
}

func (c *Client) ensureConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (uint32, error) {
	if err := cfg.Validate(); err != nil {
		return 0, err
	}

	// Try to get existing consumer first
	streamID, err := c.resolveStreamID(ctx, stream)
	if err != nil {
		return 0, err
	}

	name, group := resolveConsumerNaming(cfg.Name, cfg.Group, stream)

	subjectLimits := make([]proto.SubjectLimitJSON, len(cfg.MaxSubjectInflights))
	for i, sl := range cfg.MaxSubjectInflights {
		subjectLimits[i] = proto.SubjectLimitJSON{
			Pattern: bytesArr([]byte(sl.Pattern)),
			Limit:   sl.Limit,
		}
	}

	var deliverMode uint32 = 1 // Queue
	if cfg.Fanout {
		deliverMode = 0 // Fanout
	}

	seq := c.getConn().NextSeq()
	frame, err := proto.EncodeCreateConsumer(
		seq, streamID,
		[]byte(name), []byte(group), []byte(c.prefixed(cfg.Filter)),
		cfg.MaxInflight, cfg.AckPolicy, cfg.DeliverPolicy, deliverMode,
		uint32(cfg.AckWait.Milliseconds()), cfg.StartSeq,
		subjectLimits,
	)
	if err != nil {
		return 0, err
	}

	reply, err := c.getConn().SendExpectReply(ctx, frame, seq)
	if err != nil {
		return 0, err
	}
	if err := c.checkReply(reply); err != nil {
		if !IsAlreadyExists(err) {
			return 0, err
		}
		// Consumer exists — resolve its real ID via GetConsumer
		return c.resolveConsumerID(ctx, streamID, name)
	}
	body := reply[proto.HeaderSize:]
	if len(body) < 8 {
		return 0, &ArbitroError{Code: ErrCodeInternalError, Message: "create consumer: reply body too short"}
	}
	consumerID := uint32(proto.RepOkRefSeq(body))
	return consumerID, nil
}

// subReplay is what the reconnect supervisor needs to re-issue one
// Subscribe frame: the consumer it binds to and the filters it asked for.
// Keyed by sub_id, because that id must survive the redial — the broker
// stamps it on deliveries and the client's routes are keyed by it.
type subReplay struct {
	consumerID uint32
	filters    [][]byte
}

// BatchSubscribeEntry is one filtered subscription inside a SubscribeBatch.
// An empty Filter inherits the consumer's, exactly as a plain Subscribe does.
type BatchSubscribeEntry struct {
	Filter  string
	Handler func(*Msg)
}

// SubscribeBatchFailure names one entry the broker refused.
type SubscribeBatchFailure struct {
	// Index in the slice the caller passed.
	Index  int
	Filter string
	// Wire error code — usually ErrCodeInvalidSubscriptionFilter.
	Code uint16
}

// SubscribeBatch opens N filtered subscriptions on one consumer in a SINGLE
// round-trip.
//
// A hundred Subscribe calls cost a hundred round-trips; the broker's work per
// subscription is a filter check and a binding, so the trip is nearly the
// whole cost. Every entry runs the same admission rules as Subscribe — the
// batch buys a trip, not a different contract.
//
// Returns the accepted subscriptions in the order requested. If the broker
// refused any entry, the error is *SubscribeBatchError, which names the
// offending indices and carries the accepted subscriptions so they can still
// be closed. A refused entry means its filter escapes the consumer's slice —
// a wiring mistake, not a runtime condition.
//
// The wire carries a consumer_id per entry, so a future variant could span
// several consumers. This one does not: that would need an ensureConsumer
// round-trip apiece, which is the cost being removed.
func (c *Client) SubscribeBatch(
	ctx context.Context, stream string, cfg ConsumerConfig, entries []BatchSubscribeEntry,
) ([]*Subscription, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	if len(entries) > proto.MaxSubscribeBatch {
		return nil, &ArbitroError{
			Code:    ErrCodeInvalidEntryCount,
			Message: "subscribe batch: too many entries",
		}
	}

	consumerID, err := c.ensureConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, err
	}

	var slot ackstore.SlotRef
	if c.ackStore != nil {
		consumerName := cfg.Name
		if consumerName == "" {
			consumerName = stream
		}
		if s, serr := c.ackStore.Slot(c.prefixed(stream), consumerName); serr == nil {
			slot = s
		}
	}

	// Subscriptions registered BEFORE the frame goes out, for the reason
	// Subscribe documents: the broker serves each backlog as soon as it
	// processes the entry, and those deliveries can outrun the reply.
	subs := make([]*Subscription, len(entries))
	wire := make([]proto.SubscribeBatchEntry, len(entries))
	for i, e := range entries {
		filterText := e.Filter
		if filterText == "" {
			filterText = cfg.Filter
		}
		var filters [][]byte
		if filterText != "" {
			filters = [][]byte{[]byte(c.prefixed(filterText))}
		}
		sub := &Subscription{
			client:     c,
			id:         c.allocSubID(),
			consumerID: consumerID,
			ch:         make(chan *Msg, subChanCap),
			handler:    e.Handler,
			closed:     make(chan struct{}),
			slot:       slot,
		}
		if len(filters) > 0 {
			sub.match = newMatcher(filters[0])
		}
		c.registerSubscription(sub, filters)
		c.activeSubs.Add(1)
		subs[i] = sub
		wire[i] = proto.SubscribeBatchEntry{
			ConsumerID: consumerID, SubID: sub.id, Filters: filters,
		}
	}

	dropAll := func() {
		for _, sub := range subs {
			c.unregisterSubscription(sub.id)
			c.activeSubs.Add(^uint64(0)) // decrement
		}
	}

	cn := c.getConn()
	seq := cn.NextSeq()
	frame, err := proto.EncodeSubscribeBatch(seq, wire)
	if err != nil {
		dropAll()
		return nil, err
	}
	reply, err := cn.SendExpectReply(ctx, frame, seq)
	if err != nil {
		// A whole-frame refusal: no entry has a verdict of its own, so none
		// of these routes may survive.
		dropAll()
		return nil, err
	}
	if len(reply) < proto.HeaderSize {
		dropAll()
		return nil, &ArbitroError{
			Code: ErrCodeInternalError, Message: "subscribe batch: reply too short",
		}
	}
	body, derr := proto.DecodeSubscribeBatchReply(reply[proto.HeaderSize:])
	if derr != nil {
		dropAll()
		return nil, &ArbitroError{
			Code: ErrCodeInternalError, Message: "subscribe batch: malformed reply",
		}
	}

	// The reply names only failures — this client allocated every id, so an
	// id absent from Errors was accepted.
	rejected := make(map[uint32]uint16, len(body.Errors))
	for _, e := range body.Errors {
		rejected[e.SubscriptionID] = e.Code
	}
	fanout := make(map[uint32]bool, len(body.FanoutConsumers))
	for _, id := range body.FanoutConsumers {
		fanout[id] = true
	}
	c.subs.setFanout(consumerID, fanout[consumerID])

	accepted := make([]*Subscription, 0, len(subs))
	var failures []SubscribeBatchFailure
	for i, sub := range subs {
		code, bad := rejected[sub.id]
		if !bad {
			accepted = append(accepted, sub)
			continue
		}
		// A rejected entry has no binding on the broker; leaving its route
		// behind would strand deliveries meant for a sibling.
		c.unregisterSubscription(sub.id)
		c.activeSubs.Add(^uint64(0)) // decrement
		filterText := entries[i].Filter
		if filterText == "" {
			filterText = cfg.Filter
		}
		failures = append(failures, SubscribeBatchFailure{
			Index: i, Filter: filterText, Code: code,
		})
	}

	// One cursor request for the consumer, not one per subscription — the
	// ackstore slot is keyed by consumer.
	if slot != nil && len(accepted) > 0 {
		var gen uint32
		if c.ackRelay != nil {
			gen = c.ackRelay.Generation(consumerID)
		}
		_ = cn.TrySend(proto.EncodeAckStateReq(cn.NextSeq(), consumerID, gen))
	}

	if len(failures) > 0 {
		return nil, &SubscribeBatchError{Failures: failures, Accepted: accepted}
	}
	return accepted, nil
}

// allocSubID hands out the next subscription id for this connection.
// Starts at 1 — zero is the wire's "unnamed" sentinel.
func (c *Client) allocSubID() uint32 {
	return c.nextSubID.Add(1)
}

// registerSubscription stores the subscription for delivery dispatch and
// remembers its filters (guarded by subFilterMu) so the reconnect supervisor
// can replay the Subscribe frame after a redial. filters may be nil for
// subscriptions (e.g. service consumers) that pass their own filter list
// directly at the call site.
func (c *Client) registerSubscription(sub *Subscription, filters [][]byte) {
	c.subs.register(sub)
	c.subFilterMu.Lock()
	if c.subFilters == nil {
		c.subFilters = make(map[uint32]subReplay)
	}
	c.subFilters[sub.id] = subReplay{consumerID: sub.consumerID, filters: filters}
	c.subFilterMu.Unlock()
}

func (c *Client) unregisterSubscription(subID uint32) {
	c.subs.remove(subID)
	c.subFilterMu.Lock()
	delete(c.subFilters, subID)
	c.subFilterMu.Unlock()
}

func bytesArr(b []byte) []int {
	arr := make([]int, len(b))
	for i, v := range b {
		arr[i] = int(v)
	}
	return arr
}
