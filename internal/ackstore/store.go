// Package ackstore is the pluggable delivered-set store for the client's
// redelivery-dedup guarantee. On each incoming Deliver frame the client asks
// the store "have I already run the user handler for this message?" — a "yes"
// means silently re-ack, a "no" means run the handler.
//
// # Identity: (stream_name, consumer_name, seq)
//
// The dedup key is NOT the broker's numeric consumer_id — that ID is
// ephemeral and changes when a consumer is deleted and recreated. Because
// arbitro's `seq` is stream-scoped (the same message always has the same seq
// for every consumer of that stream — verified against the server), a durable
// key must be:
//
//	(stream_name, consumer_name, seq)
//
//   - stream_name / consumer_name: user-defined, stable across restarts and
//     consumer delete+recreate cycles.
//   - seq: the stream-level message sequence, stable per stream.
//
// This means a consumer deleted and recreated under the SAME name still
// recognizes already-processed messages (no double-processing of jobs). A
// consumer recreated under a DIFFERENT name is treated as a fresh workload
// (correct — it is a different logical consumer).
//
// # Hot-path design: resolve once, bounds-gate per message
//
// String keys are hashed once per subscription via Store.Slot(), which returns
// a SlotRef handle. The delivery hot path calls SlotRef.Fresh(seq) — a
// lock-free (min_seq, max_seq) bounds probe — and only falls back to the exact
// set check + persist when the bounds gate is inconclusive.
package ackstore

import "time"

// Store is the pluggable delivered-set store. Implementations MUST be safe for
// concurrent use.
type Store interface {
	// Slot resolves (stream, consumer) to a durable handle, allocating and
	// persisting a symbol-table entry on first use. Cold path — the client
	// calls this once per subscription and caches the returned SlotRef.
	Slot(stream, consumer string) (SlotRef, error)

	// Sync flushes buffered writes and (if configured) fsyncs. The client's
	// ack batcher calls this after a batch of confirms.
	Sync() error

	// Restore rebuilds in-memory state from persistent storage. Called once
	// at construction. No-op for the memory backend.
	Restore() error

	// Close flushes and releases resources. After Close, all methods error.
	Close() error

	// --- admin / introspection ---

	// ListSlots returns a snapshot of every known slot's metadata.
	ListSlots() []SlotInfo

	// SlotInfoByName returns the metadata for one slot, ok=false if unknown.
	SlotInfoByName(stream, consumer string) (SlotInfo, bool)

	// DeleteSlot forgets a slot entirely (all its seqs), persisting a
	// tombstone. Used when a consumer is permanently retired.
	DeleteSlot(stream, consumer string) error

	// Metrics returns cumulative counters and live gauges.
	Metrics() Metrics
}

// SlotRef is a resolved handle to one (stream, consumer) pair. The client
// caches it for the subscription's lifetime and uses it on the hot path.
//
// Two usage patterns:
//
//	A) record-at-ack (recommended, lock-free hot path):
//	     on delivery:  if !slot.Seen(seq) { runHandler(); }
//	     on ack:        slot.Record(seq)          // batched write
//	     on broker-ack: slot.Confirm(seq)         // optional cleanup
//
//	B) test-and-set at delivery (records eagerly):
//	     isNew, _ := slot.CheckRecord(seq)
//	     if isNew { runHandler() } else { reAck() }
//
// Pattern A keeps the delivery path lock-free for the common case (Seen uses
// the bounds gate and only locks for in-bounds seqs), deferring the write to
// the already-batched ack flush.
type SlotRef interface {
	// Seen reports whether seq has already been recorded (the message was
	// processed + acked before). Lock-free bounds probe first (returns false
	// fast when seq is outside the recorded range); locks only for in-bounds
	// seqs to consult the exact set. This is the DELIVERY-path dedup check.
	Seen(seq uint64) bool

	// Record marks seq as processed/acked and persists it. Idempotent. Called
	// on ACK (batched); a following Store.Sync() makes it durable.
	Record(seq uint64) error

	// Fresh is the raw lock-free bounds gate: true = seq is outside the
	// recorded [min,max] range (definitely not seen). Exposed for advanced
	// callers; most use Seen instead.
	Fresh(seq uint64) bool

	// CheckRecord is an atomic test-and-set: exact lookup, and on a miss it
	// records + persists, returning true (new). Returns false for a duplicate.
	// Use this for pattern B (eager record at delivery).
	CheckRecord(seq uint64) (isNew bool, err error)

	// Confirm marks seq as broker-acked; removes it from the live set and
	// appends a confirm record so it need not persist across restart.
	Confirm(seq uint64) error

	// ConfirmUpTo drops every live seq ≤ cursor in one pass. Driven by the
	// server's ack cursor (AckBatchResp.new_cursor / AckStateRep.cursor):
	// the broker never redelivers at or below it, so those seqs need not be
	// remembered. Keeps the live set tiny on the normal ack path — no
	// periodic job required. Returns the count dropped.
	ConfirmUpTo(cursor uint64) (int, error)

	// Info returns this slot's current metadata.
	Info() SlotInfo
}

// SlotInfo is the introspection view of one slot.
type SlotInfo struct {
	SlotID       uint32
	Stream       string
	Consumer     string
	Live         int    // live seqs (recorded, not yet confirmed/expired)
	MinSeq       uint64 // smallest live seq (0 if empty)
	MaxSeq       uint64 // largest live seq (0 if empty)
	OldestTsMs   uint64 // ts of the oldest live seq (0 if empty)
	RegisteredAt time.Time
}

// Metrics is a point-in-time snapshot of store counters.
type Metrics struct {
	Slots          int    // active slots
	LiveEntries    uint64 // total live seqs across all slots
	Recorded       uint64 // cumulative CheckRecord inserts
	Confirmed      uint64 // cumulative Confirm removals
	Expired        uint64 // cumulative TTL-swept removals
	Registered     uint64 // cumulative slot registrations
	Tombstoned     uint64 // cumulative slot deletions
	SyncErrors     uint64 // cumulative Sync() failures
	FileSize       int64  // current WAL file size in bytes (0 for memory)
	LastSyncMs     int64  // unix ms of last successful Sync (0 if never)
	LastSnapshotMs int64  // unix ms of last snapshot (0 if never)
}
