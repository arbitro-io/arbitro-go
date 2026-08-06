//go:build integration

package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
	"github.com/arbitro-io/arbitro-go/internal/ackstore"
)

// TestAckStoreConnectPurge verifies the on-connect ackstore purge: WAL
// entries recorded by a session that died before broker confirmation (its
// AckBatchResp never arrived — normal acks are fire-and-forget, so an idle
// consumer never confirms anything) are purged against the server's consumer
// cursor when a new session subscribes. Entries at or below the cursor are
// dropped; entries above it (acks the server never saw) are untouched.
func TestAckStoreConnectPurge(t *testing.T) {
	ctx := context.Background()
	stream := uniqueName("ackstore-purge")
	walDir := filepath.Join(t.TempDir(), "wal")

	const total = 10
	// Planted entry ABOVE the server cursor — simulates an ack recorded
	// locally that never reached the broker. Must survive the purge.
	const orphanSeq = uint64(10_000)

	walCfg := ackstore.Config{Dir: walDir, Fsync: true}

	// --- session 1: process + ack, then die without broker confirmation ---
	store1, err := ackstore.OpenWAL(walCfg)
	if err != nil {
		t.Fatalf("open wal 1: %v", err)
	}
	c1, err := arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckStore(store1),
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

	for i := 0; i < total; i++ {
		if err := c1.Publish(ctx, stream, stream+".job", []byte(fmt.Sprintf("job-%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

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

	processed := 0
	timer := time.NewTimer(15 * time.Second)
	for processed < total {
		select {
		case msg := <-sub1.Messages():
			processed++
			msg.Ack() // records seq into the WAL + acks the broker
		case <-timer.C:
			t.Fatalf("session1 timeout at %d/%d", processed, total)
		}
	}

	// Let the acks reach the broker (cursor advances to `total`) and the
	// background store sync flush the recorded seqs. NOTE: nothing confirms
	// the WAL entries — all of them are still live when the session ends.
	time.Sleep(500 * time.Millisecond)
	if err := c1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// --- plant the orphan above the cursor, verify the precondition ---
	{
		w, err := ackstore.OpenWAL(walCfg)
		if err != nil {
			t.Fatalf("open wal (plant): %v", err)
		}
		slot, err := w.Slot(stream, "worker")
		if err != nil {
			t.Fatalf("slot: %v", err)
		}
		if err := slot.Record(orphanSeq); err != nil {
			t.Fatalf("record orphan: %v", err)
		}
		if err := w.Sync(); err != nil {
			t.Fatalf("sync: %v", err)
		}
		info, ok := w.SlotInfoByName(stream, "worker")
		if !ok {
			t.Fatal("slot info missing after plant")
		}
		if info.Live != total+1 {
			t.Fatalf("precondition: want %d live (unconfirmed) entries, got %d", total+1, info.Live)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close wal (plant): %v", err)
		}
	}

	// --- session 2: reconnect against the same WAL — the purge must fire ---
	store2, err := ackstore.OpenWAL(walCfg)
	if err != nil {
		t.Fatalf("open wal 2: %v", err)
	}
	c2, err := arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckStore(store2),
	)
	if err != nil {
		t.Fatalf("connect 2: %v", err)
	}
	defer c2.Close()

	// Same durable consumer name → same server-side cursor (= total).
	if _, err := c2.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 1000,
		AckWait:     30 * time.Second,
	}); err != nil {
		t.Fatalf("subscribe 2: %v", err)
	}

	// The subscribe-time AckStateReq → AckStateRep round-trip is async;
	// poll the store until the purge lands (or time out).
	deadline := time.Now().Add(10 * time.Second)
	var final ackstore.SlotInfo
	for {
		info, ok := store2.SlotInfoByName(stream, "worker")
		if ok {
			final = info
			if info.Live == 1 {
				break
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if final.Live != 1 {
		t.Fatalf("entries at or below the server cursor (%d) must be purged on connect; got %d live entries (min=%d max=%d)",
			total, final.Live, final.MinSeq, final.MaxSeq)
	}
	if final.MinSeq != orphanSeq {
		t.Fatalf("the entry above the server cursor must be untouched: min=%d want %d", final.MinSeq, orphanSeq)
	}
	t.Logf("purge OK: %d stale entries dropped, orphan %d preserved", total, orphanSeq)

	c2.DeleteStream(ctx, stream)
}
