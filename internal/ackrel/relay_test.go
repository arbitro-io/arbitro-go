package ackrel

import (
	"testing"
	"time"
)

func TestRecordAndConfirmUpTo(t *testing.T) {
	r := NewRelay(0, nil) // sweepInterval=0 disables the background goroutine
	defer r.Close()

	r.Record(1, 10)
	r.Record(1, 20)
	r.Record(1, 30)

	if got := r.PendingSeqs(1); len(got) != 3 {
		t.Fatalf("pending = %v, want 3 entries", got)
	}

	removed := r.ConfirmUpTo(1, 20)
	if removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	got := r.PendingSeqs(1)
	if len(got) != 1 || got[0] != 30 {
		t.Fatalf("pending after confirm = %v, want [30]", got)
	}
}

func TestConfirmRemovesExactSeqs(t *testing.T) {
	r := NewRelay(0, nil)
	defer r.Close()

	r.Record(2, 5)
	r.Record(2, 6)
	r.Record(2, 7)

	r.Confirm(2, []uint64{6})
	got := r.PendingSeqs(2)
	if len(got) != 2 || got[0] != 5 || got[1] != 7 {
		t.Fatalf("pending = %v, want [5 7]", got)
	}
}

func TestEnsureGenerationWipesOnMismatch(t *testing.T) {
	r := NewRelay(0, nil)
	defer r.Close()

	r.Record(3, 100)
	if len(r.PendingSeqs(3)) != 1 {
		t.Fatal("expected 1 pending entry before generation change")
	}

	r.EnsureGeneration(3, 5)
	if got := r.Generation(3); got != 5 {
		t.Fatalf("generation = %d, want 5", got)
	}
	if got := r.PendingSeqs(3); len(got) != 0 {
		t.Fatalf("pending after generation mismatch = %v, want empty", got)
	}

	// Same generation again — no wipe.
	r.Record(3, 200)
	r.EnsureGeneration(3, 5)
	if got := r.PendingSeqs(3); len(got) != 1 {
		t.Fatalf("pending after matching generation = %v, want 1 entry", got)
	}
}

func TestActiveConsumersOnlyReturnsNonEmpty(t *testing.T) {
	r := NewRelay(0, nil)
	defer r.Close()

	r.Record(1, 10)
	r.Record(2, 20)
	r.ConfirmUpTo(2, 20) // consumer 2 now empty

	active := r.ActiveConsumers()
	if len(active) != 1 || active[0] != 1 {
		t.Fatalf("active consumers = %v, want [1]", active)
	}
}

func TestSweepInvokesSendCallback(t *testing.T) {
	type call struct {
		cid  uint32
		gen  uint32
		seqs []uint64
	}
	calls := make(chan call, 8)

	r := NewRelay(10*time.Millisecond, func(consumerID, generation uint32, seqs []uint64) {
		cp := append([]uint64(nil), seqs...)
		calls <- call{consumerID, generation, cp}
	})
	defer r.Close()

	r.Record(9, 42)

	select {
	case c := <-calls:
		if c.cid != 9 || len(c.seqs) != 1 || c.seqs[0] != 42 {
			t.Fatalf("unexpected sweep call: %+v", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sweep goroutine never invoked send callback")
	}
}

func TestHotCount(t *testing.T) {
	r := NewRelay(0, nil)
	defer r.Close()

	r.Record(1, 1)
	r.Record(1, 2)
	r.Record(2, 3)

	if got := r.HotCount(); got != 3 {
		t.Fatalf("HotCount = %d, want 3", got)
	}
}
