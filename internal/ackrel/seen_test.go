package ackrel

import "testing"

func TestSeenCacheHitThenMiss(t *testing.T) {
	s := NewSeenCache()
	if !s.InsertIfNew(1, 100) {
		t.Fatal("first insert should be a miss (true)")
	}
	if s.InsertIfNew(1, 100) {
		t.Fatal("second insert of same key should be a hit (false)")
	}
	if !s.InsertIfNew(1, 101) {
		t.Fatal("different seq should be a miss (true)")
	}
	if !s.InsertIfNew(2, 100) {
		t.Fatal("different consumer, same seq should be a miss (true)")
	}
}

func TestSeenCacheFIFOEviction(t *testing.T) {
	s := NewSeenCacheWithCap(2)
	if !s.InsertIfNew(1, 1) {
		t.Fatal("expected miss")
	}
	if !s.InsertIfNew(1, 2) {
		t.Fatal("expected miss")
	}
	if !s.InsertIfNew(1, 3) {
		t.Fatal("expected miss")
	}
	// cap=2 should have evicted (1,1); it's a miss (newly inserted) again.
	if !s.InsertIfNew(1, 1) {
		t.Fatal("expected (1,1) to have been evicted and re-insertable")
	}
}

func TestSeenCacheLen(t *testing.T) {
	s := NewSeenCacheWithCap(10)
	for i := uint64(0); i < 5; i++ {
		s.InsertIfNew(1, i)
	}
	if got := s.Len(); got != 5 {
		t.Fatalf("Len = %d, want 5", got)
	}
}
