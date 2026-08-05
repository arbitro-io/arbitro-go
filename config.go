package arbitro

import "time"

// Journal types for stream storage.
const (
	JournalMemory   uint32 = 0
	JournalTolerant uint32 = 1
	JournalStrict   uint32 = 2
)

// Wire values pinned to arbitro-proto/src/config/consumer.rs — DO NOT reorder.

// Ack policies for consumer delivery.
const (
	AckNone     uint32 = 0
	AckExplicit uint32 = 1
)

// Deliver policies for consumer start position.
const (
	DeliverAll        uint32 = 0
	DeliverNew        uint32 = 1
	DeliverByStartSeq uint32 = 2
)

// StreamConfig defines stream creation parameters.
type StreamConfig struct {
	SubjectFilter     string
	MaxMsgs           uint64
	MaxBytes          uint64
	MaxAge            time.Duration
	Replicas          uint32
	Journal           uint32
	IdempotencyWindow time.Duration
}

// ConsumerConfig defines consumer creation parameters.
type ConsumerConfig struct {
	// Name is the durable consumer name. Defaults to the stream name.
	Name string
	// Group is the queue group. It defaults to Name, and therefore to the
	// stream name when Name is also empty — unconditionally, whatever Fanout
	// is set to. The client never puts an empty group on the wire; the broker
	// rejects one.
	//
	// In queue mode (Fanout false) the group is the unit of load balancing:
	// members of one group share the messages round-robin, different groups
	// each get their own copy. In fanout mode the group is inert, and the
	// name-derived default is a group of one, so fanout delivery is unchanged.
	Group               string
	Filter              string
	Fanout              bool
	AckPolicy           uint32
	DeliverPolicy       uint32
	MaxInflight         uint16
	AckWait             time.Duration
	MaxDeliver          uint32
	StartSeq            uint64
	MaxSubjectInflights []SubjectLimit
}

// SubjectLimit caps in-flight messages for a subject pattern.
type SubjectLimit struct {
	Pattern string
	Limit   uint32
}

// Validate checks the same invariants arbitro-client-tokio's
// ConsumerBuilder::validate enforces before wire-encoding a CreateConsumer
// request (see consumer_builder.rs:180-223), so an invalid combination is
// rejected client-side with a clear message instead of a generic broker
// error (or, worse, silently accepted misconfiguration).
//
// One Rust invariant does not port 1:1: the Rust builder stores AckPolicy
// as Option<AckPolicy> and rejects it being unset outright ("no safe
// default"). Go's AckPolicy field is a plain uint32 whose zero value
// (AckNone) is already the long-standing implicit default used throughout
// this codebase (e.g. ensureConsumer, and the reconnect integration test
// that subscribes with only Name/Filter set) — introducing a distinct
// "unset" sentinel would be a breaking API change out of proportion to a
// validation pass. The remaining invariants, which ARE meaningfully
// checkable against Go's zero-value semantics, are enforced as-is:
//
//   - MaxInflight, MaxSubjectInflights, and AckWait all require
//     AckPolicy == AckExplicit (fire-and-forget consumers don't track
//     inflight or ack deadlines).
//   - DeliverPolicy == DeliverByStartSeq requires StartSeq > 0.
func (c ConsumerConfig) Validate() error {
	if c.AckPolicy != AckExplicit {
		if c.MaxInflight != 0 {
			return &ArbitroError{Code: ErrCodeInvalidConfig, Message: "MaxInflight requires AckPolicy: AckExplicit (fire-and-forget consumers don't track inflight)"}
		}
		if len(c.MaxSubjectInflights) > 0 {
			return &ArbitroError{Code: ErrCodeInvalidConfig, Message: "MaxSubjectInflights requires AckPolicy: AckExplicit (fire-and-forget consumers don't track inflight)"}
		}
		if c.AckWait != 0 {
			return &ArbitroError{Code: ErrCodeInvalidConfig, Message: "AckWait requires AckPolicy: AckExplicit"}
		}
	}
	if c.DeliverPolicy == DeliverByStartSeq && c.StartSeq == 0 {
		return &ArbitroError{Code: ErrCodeInvalidConfig, Message: "DeliverPolicy: DeliverByStartSeq requires StartSeq > 0"}
	}
	return nil
}

// BatchEntry is one message in a batch publish.
type BatchEntry struct {
	Subject string
	Payload []byte
	MsgID   string
}

// StreamInfo holds metadata about a stream returned by the broker.
type StreamInfo struct {
	Name     string
	StreamID uint32
	Filter   string
	Messages uint64
	Bytes    uint64
	Replicas uint32
}

// ConsumerInfo holds metadata about a consumer returned by the broker.
type ConsumerInfo struct {
	Name       string
	ConsumerID uint32
	StreamID   uint32
	Filter     string
	Pending    uint64
	AckPolicy  uint32
}

// MetricsSnapshot is a point-in-time snapshot of client counters.
type MetricsSnapshot struct {
	PublishesSent   uint64
	DeliveriesRecv  uint64
	AcksSent        uint64
	NacksSent       uint64
	Reconnects      uint64
	PendingRequests uint64
	ActiveSubs      uint64
	BatchFramesRecv uint64

	// PublishErrors counts failed Publish/PublishAsync/PublishBatch/
	// PublishDelayed/PublishWithHeaders/PublishFireAndForget calls (G14).
	PublishErrors uint64
	// AcksDeferred is a live gauge: the number of ack-reliability hot-tier
	// entries (internal/ackrel.Relay) not yet broker-confirmed.
	AcksDeferred uint64
	// AcksConfirmed is a cumulative counter of hot-tier entries purged by
	// AckBatchResp/AckStateRep (broker confirmation).
	AcksConfirmed uint64
	// AcksExpired is a cumulative counter of hot-tier entries dropped by
	// the TTL sweep without ever being broker-confirmed.
	AcksExpired uint64
}
