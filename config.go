package arbitro

import "time"

// Journal types for stream storage.
const (
	JournalMemory   uint32 = 0
	JournalTolerant uint32 = 1
	JournalStrict   uint32 = 2
)

// Ack policies for consumer delivery.
const (
	AckExplicit uint32 = 0
	AckNone     uint32 = 1
)

// Deliver policies for consumer start position.
const (
	DeliverAll    uint32 = 0
	DeliverNew    uint32 = 1
	DeliverLast   uint32 = 2
	DeliverBySeq  uint32 = 3
	DeliverByTime uint32 = 4
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
	Name                string
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

// BatchEntry is one message in a batch publish.
type BatchEntry struct {
	Subject string
	Payload []byte
	MsgID   string
}

// StreamInfo holds metadata about a stream returned by the broker.
type StreamInfo struct {
	Name      string
	StreamID  uint32
	Filter    string
	Messages  uint64
	Bytes     uint64
	Replicas  uint32
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
}
