package conn

import "sync"

// PendingMap tracks in-flight request/reply correlations by sequence number.
type PendingMap struct {
	mu      sync.Mutex
	entries map[uint64]chan []byte
}

// NewPendingMap creates a new pending correlation map.
func NewPendingMap() *PendingMap {
	return &PendingMap{
		entries: make(map[uint64]chan []byte),
	}
}

// Register adds a pending entry for the given seq, returns the reply channel.
func (p *PendingMap) Register(seq uint64) <-chan []byte {
	ch := make(chan []byte, 1)
	p.mu.Lock()
	p.entries[seq] = ch
	p.mu.Unlock()
	return ch
}

// Resolve delivers a reply to the pending entry for seq.
func (p *PendingMap) Resolve(seq uint64, frame []byte) {
	p.mu.Lock()
	ch, ok := p.entries[seq]
	if ok {
		delete(p.entries, seq)
	}
	p.mu.Unlock()
	if ok {
		ch <- frame
	}
}

// Remove cancels a pending entry (e.g., on context timeout).
func (p *PendingMap) Remove(seq uint64) {
	p.mu.Lock()
	delete(p.entries, seq)
	p.mu.Unlock()
}

// CloseAll resolves all pending entries with nil (connection closed).
func (p *PendingMap) CloseAll() {
	p.mu.Lock()
	for seq, ch := range p.entries {
		close(ch)
		delete(p.entries, seq)
	}
	p.mu.Unlock()
}

// Len returns the number of in-flight requests.
func (p *PendingMap) Len() int {
	p.mu.Lock()
	n := len(p.entries)
	p.mu.Unlock()
	return n
}
