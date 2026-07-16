package ackstore

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Memory is the in-memory-only Store — no persistence, no recovery. The
// correct choice when restart-durability isn't required (the broker
// redelivers unacked messages on reconnect anyway) and as the fast baseline
// for benchmarking the WAL. Implements the exact same (stream, consumer, seq)
// identity + bounds-gate hot path.
type Memory struct {
	cap int

	symMu  sync.RWMutex
	byName map[string]*memSlot
	nextID uint32
	nowFn  func() int64

	mRecorded  atomic.Uint64
	mConfirmed atomic.Uint64
	mExpired   atomic.Uint64
	mRegistered atomic.Uint64
	mTombstoned atomic.Uint64
}

type memSlot struct {
	slotID       uint32
	stream       string
	consumer     string
	registeredAt time.Time
	mem          *Memory

	minSeq atomic.Uint64
	maxSeq atomic.Uint64

	mu       sync.Mutex
	set      map[uint64]int64
	fifo     []uint64
	oldestTs int64
	tombed   bool
}

// NewMemory returns an in-memory Store. cap bounds each slot's live set
// (FIFO eviction); cap <= 0 uses 1_000_000.
func NewMemory(cap int) *Memory {
	if cap <= 0 {
		cap = 1_000_000
	}
	return &Memory{
		cap:    cap,
		byName: make(map[string]*memSlot),
		nowFn:  func() int64 { return time.Now().UnixMilli() },
	}
}

func (m *Memory) Slot(stream, consumer string) (SlotRef, error) {
	if len(stream) > maxNameLen || len(consumer) > maxNameLen {
		return nil, ErrNameTooLong
	}
	key := slotKey(stream, consumer)
	m.symMu.RLock()
	st := m.byName[key]
	m.symMu.RUnlock()
	if st != nil {
		return st, nil
	}
	m.symMu.Lock()
	defer m.symMu.Unlock()
	if st = m.byName[key]; st != nil {
		return st, nil
	}
	if m.nextID == ^uint32(0) {
		return nil, ErrSlotOverflow
	}
	st = &memSlot{
		slotID:       m.nextID,
		stream:       stream,
		consumer:     consumer,
		registeredAt: time.UnixMilli(m.nowFn()),
		mem:          m,
		set:          make(map[uint64]int64),
	}
	m.nextID++
	m.byName[key] = st
	m.mRegistered.Add(1)
	return st, nil
}

func (s *memSlot) Fresh(seq uint64) bool {
	max := s.maxSeq.Load()
	if max == 0 || seq > max {
		return true
	}
	if seq < s.minSeq.Load() {
		return true
	}
	return false
}

func (s *memSlot) Seen(seq uint64) bool {
	if s.Fresh(seq) {
		return false
	}
	s.mu.Lock()
	_, ok := s.set[seq]
	s.mu.Unlock()
	return ok
}

func (s *memSlot) Record(seq uint64) error {
	_, err := s.CheckRecord(seq)
	return err
}

func (s *memSlot) CheckRecord(seq uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tombed {
		return true, nil
	}
	if _, exists := s.set[seq]; exists {
		return false, nil
	}
	now := s.mem.nowFn()
	s.set[seq] = now
	s.fifo = append(s.fifo, seq)
	if s.oldestTs == 0 {
		s.oldestTs = now
	}
	for len(s.fifo) > s.mem.cap {
		old := s.fifo[0]
		s.fifo = s.fifo[1:]
		delete(s.set, old)
	}
	if cur := s.maxSeq.Load(); seq > cur {
		s.maxSeq.Store(seq)
	}
	if len(s.fifo) > 0 {
		s.minSeq.Store(s.fifo[0])
	}
	s.mem.mRecorded.Add(1)
	return true, nil
}

func (s *memSlot) Confirm(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.set[seq]; ok {
		delete(s.set, seq)
		if len(s.set) == 0 {
			s.oldestTs = 0
		}
		s.mem.mConfirmed.Add(1)
	}
	return nil
}

func (s *memSlot) ConfirmUpTo(cursor uint64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	var minSeq uint64
	for seq := range s.set {
		if seq <= cursor {
			delete(s.set, seq)
			removed++
		} else if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}
	}
	if removed > 0 {
		s.minSeq.Store(minSeq)
		if len(s.set) == 0 {
			s.oldestTs = 0
		}
		s.mem.mConfirmed.Add(uint64(removed))
	}
	return removed, nil
}

func (s *memSlot) Info() SlotInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	var minSeq, maxSeq uint64
	for seq := range s.set {
		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return SlotInfo{
		SlotID:       s.slotID,
		Stream:       s.stream,
		Consumer:     s.consumer,
		Live:         len(s.set),
		MinSeq:       minSeq,
		MaxSeq:       maxSeq,
		OldestTsMs:   uint64(s.oldestTs),
		RegisteredAt: s.registeredAt,
	}
}

func (m *Memory) Sync() error    { return nil }
func (m *Memory) Restore() error { return nil }
func (m *Memory) Close() error   { return nil }

func (m *Memory) ListSlots() []SlotInfo {
	m.symMu.RLock()
	sts := make([]*memSlot, 0, len(m.byName))
	for _, st := range m.byName {
		sts = append(sts, st)
	}
	m.symMu.RUnlock()
	out := make([]SlotInfo, 0, len(sts))
	for _, st := range sts {
		out = append(out, st.Info())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SlotID < out[j].SlotID })
	return out
}

func (m *Memory) SlotInfoByName(stream, consumer string) (SlotInfo, bool) {
	m.symMu.RLock()
	st := m.byName[slotKey(stream, consumer)]
	m.symMu.RUnlock()
	if st == nil {
		return SlotInfo{}, false
	}
	return st.Info(), true
}

func (m *Memory) DeleteSlot(stream, consumer string) error {
	key := slotKey(stream, consumer)
	m.symMu.Lock()
	st := m.byName[key]
	if st == nil {
		m.symMu.Unlock()
		return ErrUnknownSlot
	}
	delete(m.byName, key)
	m.symMu.Unlock()
	st.mu.Lock()
	st.tombed = true
	st.set = make(map[uint64]int64)
	st.fifo = nil
	st.minSeq.Store(0)
	st.maxSeq.Store(0)
	st.mu.Unlock()
	m.mTombstoned.Add(1)
	return nil
}

func (m *Memory) Metrics() Metrics {
	m.symMu.RLock()
	slots := len(m.byName)
	var live uint64
	for _, st := range m.byName {
		st.mu.Lock()
		live += uint64(len(st.set))
		st.mu.Unlock()
	}
	m.symMu.RUnlock()
	return Metrics{
		Slots:       slots,
		LiveEntries: live,
		Recorded:    m.mRecorded.Load(),
		Confirmed:   m.mConfirmed.Load(),
		Expired:     m.mExpired.Load(),
		Registered:  m.mRegistered.Load(),
		Tombstoned:  m.mTombstoned.Load(),
	}
}
