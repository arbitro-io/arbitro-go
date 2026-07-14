package ackrel

import "sync"

// DefaultSeenCap mirrors the Rust client's SeenCache DEFAULT_CAP: ~1M
// entries, comfortably bounded (~12 MiB at 12B/entry).
const DefaultSeenCap = 1_000_000

type seenKey struct {
	ConsumerID uint32
	Seq        uint64
}

// SeenCache is a bounded FIFO dedup set of (consumerID, seq) pairs — a
// redelivery guard for at-least-once ack semantics (G18). A hit means the
// message already ran the user callback once; the caller re-acks without
// invoking it again.
type SeenCache struct {
	mu   sync.Mutex
	cap  int
	set  map[seenKey]struct{}
	fifo []seenKey
	head int // fifo[:head] are already-evicted slots pending compaction
}

// NewSeenCache creates a SeenCache with the default capacity.
func NewSeenCache() *SeenCache {
	return NewSeenCacheWithCap(DefaultSeenCap)
}

// NewSeenCacheWithCap creates a SeenCache with an explicit capacity (tests
// use a small cap to exercise eviction cheaply).
func NewSeenCacheWithCap(cap int) *SeenCache {
	if cap <= 0 {
		cap = DefaultSeenCap
	}
	return &SeenCache{
		cap: cap,
		set: make(map[seenKey]struct{}),
	}
}

// InsertIfNew returns true if (cid, seq) was newly inserted (a miss — first
// delivery). Returns false if already present (a hit — redelivery).
func (s *SeenCache) InsertIfNew(cid uint32, seq uint64) bool {
	key := seenKey{ConsumerID: cid, Seq: seq}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.set[key]; ok {
		return false
	}
	s.set[key] = struct{}{}
	s.fifo = append(s.fifo, key)

	for len(s.fifo)-s.head > s.cap {
		old := s.fifo[s.head]
		delete(s.set, old)
		s.head++
	}
	// Compact periodically so the backing slice doesn't grow unbounded —
	// equivalent to VecDeque's ring-buffer behavior in the Rust client.
	if s.head > 1<<16 && s.head*2 > len(s.fifo) {
		remaining := len(s.fifo) - s.head
		compacted := make([]seenKey, remaining)
		copy(compacted, s.fifo[s.head:])
		s.fifo = compacted
		s.head = 0
	}
	return true
}

// Len returns the number of entries currently tracked.
func (s *SeenCache) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.set)
}
