package conn

import (
	"encoding/binary"
	"time"

	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// ackBatchMax is the maximum number of acks to batch before flushing.
const ackBatchMax = 64

// ackFlushInterval is the maximum time to hold acks before flushing.
const ackFlushInterval = time.Millisecond

// AckItem represents a single ack to be batched.
type AckItem struct {
	ConsumerID  uint32
	SubjectHash uint32
	Seq         uint64
}

// AckBatcher accumulates individual acks and flushes them as BatchAck frames.
// This reduces wire traffic from N individual 32-byte frames to one BatchAck
// frame containing N entries — same pattern as the Rust client's ack_batcher_task.
type AckBatcher struct {
	ch   chan AckItem
	conn *Connection
	done <-chan struct{}
}

// NewAckBatcher creates and starts an ack batcher goroutine.
func NewAckBatcher(c *Connection) *AckBatcher {
	ab := &AckBatcher{
		ch:   make(chan AckItem, 4096),
		conn: c,
		done: c.done,
	}
	go ab.run()
	return ab
}

// Ack enqueues an ack for batching. Non-blocking.
func (ab *AckBatcher) Ack(consumerID, subjectHash uint32, seq uint64) {
	select {
	case ab.ch <- AckItem{ConsumerID: consumerID, SubjectHash: subjectHash, Seq: seq}:
	case <-ab.done:
	}
}

func (ab *AckBatcher) run() {
	// Group acks by consumer_id for efficient BatchAck encoding.
	// Most workloads have 1-3 active consumers, so a simple map is fine.
	pending := make(map[uint32][]proto.AckEntry)
	timer := time.NewTimer(ackFlushInterval)
	timer.Stop()
	timerActive := false

	for {
		select {
		case item, ok := <-ab.ch:
			if !ok {
				ab.flush(pending)
				return
			}
			pending[item.ConsumerID] = append(pending[item.ConsumerID], proto.AckEntry{
				Seq:         item.Seq,
				SubjectHash: item.SubjectHash,
			})

			// Check if any consumer hit the batch cap.
			if len(pending[item.ConsumerID]) >= ackBatchMax {
				ab.flushConsumer(pending, item.ConsumerID)
			}

			// Start the flush timer if not already active.
			if !timerActive {
				timer.Reset(ackFlushInterval)
				timerActive = true
			}

		case <-timer.C:
			timerActive = false
			ab.flush(pending)

		case <-ab.done:
			ab.flush(pending)
			return
		}
	}
}

func (ab *AckBatcher) flush(pending map[uint32][]proto.AckEntry) {
	for cid, entries := range pending {
		if len(entries) == 0 {
			continue
		}
		ab.sendBatchAck(cid, entries)
		pending[cid] = entries[:0] // reuse slice
	}
}

func (ab *AckBatcher) flushConsumer(pending map[uint32][]proto.AckEntry, cid uint32) {
	entries := pending[cid]
	if len(entries) == 0 {
		return
	}
	ab.sendBatchAck(cid, entries)
	pending[cid] = entries[:0]
}

func (ab *AckBatcher) sendBatchAck(consumerID uint32, entries []proto.AckEntry) {
	if len(entries) == 1 {
		// Single ack — encode directly (no batch overhead).
		frame := encodeSingleAckInline(ab.conn.NextSeq(), consumerID, entries[0])
		_ = ab.conn.Send(frame)
		return
	}
	seq := ab.conn.NextSeq()
	frame := proto.EncodeBatchAck(seq, consumerID, entries)
	_ = ab.conn.Send(frame)
}

// encodeSingleAckInline encodes a single ack without allocation from a pool.
// For the common case of 1 pending ack, avoid BatchAck overhead.
func encodeSingleAckInline(seq uint64, consumerID uint32, entry proto.AckEntry) []byte {
	frame := make([]byte, 32) // Ack frame is always 32 bytes
	proto.EncodeHeader(frame, proto.Header{
		Action: proto.ActionAck,
		Flags:  proto.FlagNone,
		MsgLen: 16,
		Seq:    seq,
	})
	body := frame[proto.HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], consumerID)
	binary.LittleEndian.PutUint32(body[4:8], entry.SubjectHash)
	binary.LittleEndian.PutUint64(body[8:16], entry.Seq)
	return frame
}
