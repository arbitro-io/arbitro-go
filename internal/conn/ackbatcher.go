package conn

import (
	"encoding/binary"

	"github.com/arbitro-io/arbitro-go/internal/proto"
)

// ackBatchMax is the maximum number of acks to batch before flushing.
const ackBatchMax = 64

// AckItem represents a single ack to be batched.
type AckItem struct {
	ConsumerID  uint32
	SubjectHash uint32
	Seq         uint64
}

// AckBatcher accumulates individual acks and flushes them as BatchAck frames.
// Same shape as Rust ack_batcher_task (consume/mod.rs): recv() → drain-until-
// empty → flush. No mandatory-wait timer — that timer capped single-msg ack
// throughput at ~1kHz and was the direct cause of BenchmarkPubSubE2E stalls
// (Go-HP1 audit finding).
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
	pending := make(map[uint32][]proto.AckEntry)

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

		drain:
			for {
				select {
				case it, ok := <-ab.ch:
					if !ok {
						ab.flush(pending)
						return
					}
					pending[it.ConsumerID] = append(pending[it.ConsumerID], proto.AckEntry{
						Seq:         it.Seq,
						SubjectHash: it.SubjectHash,
					})
					if len(pending[it.ConsumerID]) >= ackBatchMax {
						ab.flushConsumer(pending, it.ConsumerID)
					}
				default:
					break drain
				}
			}
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
		// TrySend, not Send: a full queue is exactly what recordFailed's hot
		// tier exists for, and blocking the batcher goroutine would stop
		// every other consumer's acks too.
		if err := ab.conn.TrySend(frame); err != nil {
			ab.recordFailed(consumerID, entries)
		}
		return
	}
	seq := ab.conn.NextSeq()
	frame := proto.EncodeBatchAck(seq, consumerID, entries)
	if err := ab.conn.TrySend(frame); err != nil {
		ab.recordFailed(consumerID, entries)
	}
}

// recordFailed defers acks that failed to reach the wire (write channel
// backpressure, mid-flight disconnect) into the ack-reliability hot tier
// (G01) instead of silently dropping them. The sweep goroutine — or the
// reconnect supervisor's AckStateReq replay — retries them later.
func (ab *AckBatcher) recordFailed(consumerID uint32, entries []proto.AckEntry) {
	relay := ab.conn.AckRelay
	if relay == nil {
		return
	}
	for _, e := range entries {
		relay.Record(consumerID, e.Seq)
	}
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
