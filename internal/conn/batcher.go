package conn

import (
	"sync"
	"time"

	"github.com/arbitro-io/arbitro-go/internal/proto"
)

const (
	batchFlushInterval = time.Millisecond
	batchMaxEntries    = 64
)

// AckBatcher accumulates individual acks and flushes them as batched frames.
// This reduces syscalls on the hot path: instead of N ack writes, one batch write.
type AckBatcher struct {
	conn *Connection

	mu      sync.Mutex
	acks    []proto.AckEntry
	nacks   []proto.NackEntry
	consID  uint32

	stopCh chan struct{}
	done   chan struct{}
}

// NewAckBatcher creates and starts the background flush goroutine.
func NewAckBatcher(c *Connection) *AckBatcher {
	b := &AckBatcher{
		conn:   c,
		acks:   make([]proto.AckEntry, 0, batchMaxEntries),
		nacks:  make([]proto.NackEntry, 0, batchMaxEntries),
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go b.flushLoop()
	return b
}

// Ack queues an ack entry for batching. Flushes immediately if batch is full.
func (b *AckBatcher) Ack(consumerID uint32, subjectHash uint32, seq uint64) {
	b.mu.Lock()
	b.consID = consumerID
	b.acks = append(b.acks, proto.AckEntry{Seq: seq, SubjectHash: subjectHash})
	full := len(b.acks) >= batchMaxEntries
	b.mu.Unlock()

	if full {
		b.flush()
	}
}

// Nack queues a nack entry with delay for batching.
func (b *AckBatcher) Nack(consumerID uint32, subjectHash uint32, seq uint64, delayMs uint32) {
	b.mu.Lock()
	b.consID = consumerID
	b.nacks = append(b.nacks, proto.NackEntry{Seq: seq, SubjectHash: subjectHash, DelayMs: delayMs})
	full := len(b.nacks) >= batchMaxEntries
	b.mu.Unlock()

	if full {
		b.flush()
	}
}

// Stop halts the background flush goroutine and does a final flush.
func (b *AckBatcher) Stop() {
	close(b.stopCh)
	<-b.done
	b.flush()
}

func (b *AckBatcher) flushLoop() {
	defer close(b.done)
	ticker := time.NewTicker(batchFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.flush()
		case <-b.stopCh:
			return
		}
	}
}

func (b *AckBatcher) flush() {
	b.mu.Lock()
	acks := b.acks
	nacks := b.nacks
	consID := b.consID
	if len(acks) > 0 {
		b.acks = make([]proto.AckEntry, 0, batchMaxEntries)
	}
	if len(nacks) > 0 {
		b.nacks = make([]proto.NackEntry, 0, batchMaxEntries)
	}
	b.mu.Unlock()

	if len(acks) == 0 && len(nacks) == 0 {
		return
	}

	if len(acks) == 1 {
		// Single ack: send directly (no batch overhead)
		frame := proto.EncodeAck(b.conn.NextSeq(), consID, acks[0].SubjectHash, acks[0].Seq)
		_ = b.conn.Send(frame)
	} else if len(acks) > 1 {
		frame := proto.EncodeBatchAck(b.conn.NextSeq(), consID, acks)
		_ = b.conn.Send(frame)
	}

	if len(nacks) == 1 {
		frame := proto.EncodeNack(b.conn.NextSeq(), consID, nacks[0].SubjectHash, nacks[0].Seq)
		_ = b.conn.Send(frame)
	} else if len(nacks) > 1 {
		frame := proto.EncodeBatchNack(b.conn.NextSeq(), consID, nacks)
		_ = b.conn.Send(frame)
	}
}
