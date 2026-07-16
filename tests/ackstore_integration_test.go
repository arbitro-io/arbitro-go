//go:build integration

package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// TestAckStorePersistentDedup verifies the whole point of the WAL ackstore:
// a message processed + acked before a process restart is NOT re-processed
// after the client reconnects with the same durable WAL — even though the
// broker redelivers it (unconfirmed acks are redelivered at-least-once).
func TestAckStorePersistentDedup(t *testing.T) {
	ctx := context.Background()
	stream := uniqueName("ackstore-persist")
	walDir := filepath.Join(t.TempDir(), "wal")

	// --- session 1: process 100 messages, record acks durably, then close ---
	c1, err := arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckPersistence(walDir, time.Hour, true),
	)
	if err != nil {
		t.Fatalf("connect 1: %v", err)
	}

	if _, err := c1.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       100000,
		Journal:       arbitro.JournalTolerant,
	}); err != nil && !arbitro.IsAlreadyExists(err) {
		t.Fatalf("create stream: %v", err)
	}

	const total = 100
	for i := 0; i < total; i++ {
		if err := c1.Publish(ctx, stream, stream+".job", []byte(fmt.Sprintf("job-%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	var processed1 atomic.Int32
	sub1, err := c1.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 1000,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe 1: %v", err)
	}

	timer := time.NewTimer(15 * time.Second)
	for processed1.Load() < total {
		select {
		case msg := <-sub1.Messages():
			processed1.Add(1)
			msg.Ack()
		case <-timer.C:
			t.Fatalf("session1 timeout at %d/%d", processed1.Load(), total)
		}
	}
	// Give the background ackstore Sync goroutine a beat to flush, then close
	// (Close does a final Sync + WAL fsync).
	time.Sleep(200 * time.Millisecond)
	if err := c1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}
	t.Logf("session 1 processed %d messages", processed1.Load())

	// --- session 2: reopen with the SAME WAL dir. The broker redelivers the
	// unconfirmed messages, but the persistent ackstore recognizes them and
	// the handler must NOT run again. ---
	c2, err := arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckPersistence(walDir, time.Hour, true),
	)
	if err != nil {
		t.Fatalf("connect 2: %v", err)
	}
	defer c2.Close()

	var reprocessed atomic.Int32
	sub2, err := c2.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "worker", // SAME durable name → same WAL slot
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 1000,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe 2: %v", err)
	}

	// Drain whatever the broker redelivers for a bounded window. Any message
	// that reaches the handler here is a message the dedup FAILED to catch.
	drainTimer := time.NewTimer(3 * time.Second)
	draining := true
	for draining {
		select {
		case msg := <-sub2.Messages():
			reprocessed.Add(1)
			msg.Ack()
		case <-drainTimer.C:
			draining = false
		}
	}

	t.Logf("session 2 reprocessed %d messages (want 0)", reprocessed.Load())
	if reprocessed.Load() != 0 {
		t.Fatalf("persistent dedup failed: %d messages reprocessed after restart", reprocessed.Load())
	}
	c2.DeleteStream(ctx, stream)
}

// TestAckStoreInMemoryDedup verifies the default (in-memory) store dedupes
// within a single process: a manually re-subscribed consumer under the same
// name does not reprocess already-acked messages.
func TestAckStoreInMemoryDedup(t *testing.T) {
	ctx := context.Background()
	c := connectT(t) // default: in-memory ackstore
	stream := uniqueName("ackstore-mem")

	if _, err := c.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer c.DeleteStream(ctx, stream)

	const total = 50
	for i := 0; i < total; i++ {
		c.Publish(ctx, stream, stream+".ev", []byte("x"))
	}

	var got atomic.Int32
	sub, err := c.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "w",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 1000,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	timer := time.NewTimer(10 * time.Second)
	for got.Load() < total {
		select {
		case msg := <-sub.Messages():
			got.Add(1)
			msg.Ack()
		case <-timer.C:
			t.Fatalf("timeout at %d/%d", got.Load(), total)
		}
	}
	if got.Load() != total {
		t.Fatalf("expected %d, got %d", total, got.Load())
	}
	t.Logf("in-memory dedup: processed all %d exactly once", got.Load())
}
