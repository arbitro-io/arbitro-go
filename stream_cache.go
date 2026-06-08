package arbitro

import "sync"

// streamCache maps stream names to their broker-assigned IDs.
type streamCache struct {
	mu    sync.RWMutex
	cache map[string]uint32
}

func (sc *streamCache) get(name string) (uint32, bool) {
	sc.mu.RLock()
	id, ok := sc.cache[name]
	sc.mu.RUnlock()
	return id, ok
}

func (sc *streamCache) set(name string, id uint32) {
	sc.mu.Lock()
	sc.cache[name] = id
	sc.mu.Unlock()
}
