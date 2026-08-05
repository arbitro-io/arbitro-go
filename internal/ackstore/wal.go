package ackstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// WAL is a persistent, append-only Store. The in-memory state (per-slot
// bounds + live set) is the runtime source of truth; the log is read only at
// Restore(). Records are length-framed and CRC-guarded (see format.go).
//
// Durability contract: a record is durable once Sync() (with Fsync enabled)
// returns. Between CheckRecord and the next Sync a crash loses that record,
// and the message is reprocessed after restart — the at-least-once guarantee
// the broker already provides. Dedup is a best-effort optimization layered on
// top; it is never wrong (CheckRecord always consults the exact set), only
// occasionally missed after an un-synced crash.
type WAL struct {
	cfg Config

	// Writer state (shared file). Held briefly for each framed append and
	// for Sync/Close. Lock order: slotState.mu → writer.mu, and
	// symbol-table.mu → writer.mu. Never the reverse.
	writer struct {
		mu       sync.Mutex
		f        *os.File
		bw       *bufio.Writer
		size     int64
		scratch  []byte // reusable encode buffer (guarded by writer.mu)
		closed   bool
	}

	// Symbol table + slot registry.
	symMu   sync.RWMutex
	byName  map[string]*slotState // key = stream \x00 consumer
	byID    []*slotState          // index by slotID (dense, monotonic)
	nextID  uint32

	// TTL sweeper.
	stopCh   chan struct{}
	stopOnce sync.Once
	sweeping sync.WaitGroup

	// Metrics (cumulative, atomic).
	mRecorded    atomic.Uint64
	mConfirmed   atomic.Uint64
	mExpired     atomic.Uint64
	mRegistered  atomic.Uint64
	mTombstoned  atomic.Uint64
	mSyncErrors  atomic.Uint64
	mLastSyncMs  atomic.Int64
	mLastSnapMs  atomic.Int64
	recsSinceSnap atomic.Uint64
}

type slotState struct {
	slotID       uint32
	stream       string
	consumer     string
	registeredAt time.Time
	wal          *WAL // back-pointer for appends

	minSeq atomic.Uint64 // conservative low bound (eviction floor)
	maxSeq atomic.Uint64 // conservative high bound (0 = never recorded)

	mu       sync.Mutex       // guards set/fifo/oldestTs AND serializes this slot's disk appends
	set      map[uint64]int64 // seq → ts_ms
	fifo     []uint64         // insertion order (may hold confirmed/dead seqs, compacted lazily)
	oldestTs int64
	tombed   bool
}

// Config controls WAL behavior. Zero values fall back to defaults.
type Config struct {
	Dir            string        // directory holding ackstore.log
	TTL            time.Duration // per-entry expiry; 0 = never expire
	SweepInterval  time.Duration // TTL sweep cadence; default 5s
	Fsync          bool          // fsync on Sync()/Close()
	BufferSize     int           // bufio writer size; default 64 KiB
	MaxCap         int           // live seqs per slot before FIFO eviction; default 1_000_000
	SnapshotEveryN int           // records between auto-snapshots; 0 disables
	CompactAtBytes int64         // file size that triggers auto-compaction on Sync; 0 disables
	nowFn          func() int64  // injectable clock (unix ms) for tests
}

func (c *Config) withDefaults() {
	if c.SweepInterval <= 0 {
		c.SweepInterval = 5 * time.Second
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 64 * 1024
	}
	if c.MaxCap <= 0 {
		c.MaxCap = 1_000_000
	}
	if c.nowFn == nil {
		c.nowFn = func() int64 { return time.Now().UnixMilli() }
	}
}

var (
	ErrClosed         = errors.New("ackstore: closed")
	ErrSlotOverflow   = errors.New("ackstore: slot id overflow")
	ErrNameTooLong    = errors.New("ackstore: stream/consumer name too long")
	ErrUnknownSlot    = errors.New("ackstore: unknown slot")
)

const maxNameLen = 65535

// OpenWAL opens (creating if needed) a WAL at cfg.Dir and replays it.
func OpenWAL(cfg Config) (*WAL, error) {
	cfg.withDefaults()
	if cfg.Dir == "" {
		return nil, errors.New("ackstore: Config.Dir required")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("ackstore: mkdir %s: %w", cfg.Dir, err)
	}
	fp := filepath.Join(cfg.Dir, "ackstore.log")
	f, err := os.OpenFile(fp, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("ackstore: open %s: %w", fp, err)
	}

	w := &WAL{
		cfg:    cfg,
		byName: make(map[string]*slotState),
		stopCh: make(chan struct{}),
	}
	w.writer.f = f
	w.writer.scratch = make([]byte, 0, 4096)

	if err := w.replay(); err != nil {
		f.Close()
		return nil, err
	}

	// Position at end for appends; ensure magic header exists.
	if err := w.ensureHeader(); err != nil {
		f.Close()
		return nil, err
	}
	w.writer.bw = bufio.NewWriterSize(f, cfg.BufferSize)

	if cfg.TTL > 0 {
		w.sweeping.Add(1)
		go w.sweepLoop()
	}
	return w, nil
}

// ensureHeader writes the file magic if the file is empty and leaves the
// offset at EOF. Records writer.size.
func (w *WAL) ensureHeader() error {
	f := w.writer.f
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		if _, err := f.WriteAt(fileMagic[:], 0); err != nil {
			return err
		}
		w.writer.size = int64(len(fileMagic))
	} else {
		w.writer.size = info.Size()
	}
	if _, err := f.Seek(w.writer.size, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func slotKey(stream, consumer string) string {
	return stream + "\x00" + consumer
}

// Slot resolves (stream, consumer) to a durable handle, registering it on
// first use.
func (w *WAL) Slot(stream, consumer string) (SlotRef, error) {
	if len(stream) > maxNameLen || len(consumer) > maxNameLen {
		return nil, ErrNameTooLong
	}
	key := slotKey(stream, consumer)

	w.symMu.RLock()
	st := w.byName[key]
	w.symMu.RUnlock()
	if st != nil {
		return st, nil
	}

	w.symMu.Lock()
	if st = w.byName[key]; st != nil {
		w.symMu.Unlock()
		return st, nil
	}
	if w.nextID == ^uint32(0) {
		w.symMu.Unlock()
		return nil, ErrSlotOverflow
	}
	id := w.nextID
	w.nextID++
	now := w.cfg.nowFn()
	st = &slotState{
		slotID:       id,
		stream:       stream,
		consumer:     consumer,
		registeredAt: time.UnixMilli(now),
		wal:          w,
		set:          make(map[uint64]int64),
	}
	w.byName[key] = st
	for uint32(len(w.byID)) <= id {
		w.byID = append(w.byID, nil)
	}
	w.byID[id] = st
	w.symMu.Unlock()

	// Persist the Register record.
	sz := registerFrameSize(len(stream), len(consumer))
	buf := make([]byte, sz)
	encodeRegister(buf, id, uint64(now), []byte(stream), []byte(consumer))
	if err := w.append(buf); err != nil {
		return st, err // slot usable in memory; register will re-persist on next op if needed
	}
	w.mRegistered.Add(1)
	return st, nil
}

// append writes a pre-framed record to the buffered writer. Used by the
// variable-length paths (Register, Snapshot) that build their frame in a
// caller-owned buffer. Caller may hold a slotState.mu; append only takes
// writer.mu (lock order respected).
func (w *WAL) append(frame []byte) error {
	w.writer.mu.Lock()
	defer w.writer.mu.Unlock()
	if w.writer.closed {
		return ErrClosed
	}
	n, err := w.writer.bw.Write(frame)
	w.writer.size += int64(n)
	return err
}

// appendSeqOp encodes and writes a Record/Confirm/Expire frame using the
// writer's reusable scratch buffer — zero per-call heap allocation. The
// encode happens under writer.mu; the caller holding slotState.mu guarantees
// disk order matches memory order for that slot (append is called while the
// slot lock is held).
func (w *WAL) appendSeqOp(op byte, slotID uint32, tsMs, seq uint64) error {
	w.writer.mu.Lock()
	defer w.writer.mu.Unlock()
	if w.writer.closed {
		return ErrClosed
	}
	if cap(w.writer.scratch) < recordFrameSize() {
		w.writer.scratch = make([]byte, recordFrameSize())
	}
	buf := w.writer.scratch[:recordFrameSize()]
	sz := encodeSeqOp(buf, op, slotID, tsMs, seq)
	n, err := w.writer.bw.Write(buf[:sz])
	w.writer.size += int64(n)
	return err
}

// appendTombstone encodes and writes a Tombstone via the writer scratch.
func (w *WAL) appendTombstone(slotID uint32, tsMs uint64) error {
	w.writer.mu.Lock()
	defer w.writer.mu.Unlock()
	if w.writer.closed {
		return ErrClosed
	}
	if cap(w.writer.scratch) < tombstoneFrameSize() {
		w.writer.scratch = make([]byte, tombstoneFrameSize())
	}
	buf := w.writer.scratch[:tombstoneFrameSize()]
	sz := encodeTombstone(buf, slotID, tsMs)
	n, err := w.writer.bw.Write(buf[:sz])
	w.writer.size += int64(n)
	return err
}

// --- SlotRef hot path ---

func (st *slotState) Fresh(seq uint64) bool {
	max := st.maxSeq.Load()
	if max == 0 || seq > max {
		return true
	}
	if seq < st.minSeq.Load() {
		return true
	}
	return false
}

// Seen is the delivery-path dedup check: true = already recorded. Lock-free
// bounds gate first; locks only for in-bounds seqs.
func (st *slotState) Seen(seq uint64) bool {
	if st.Fresh(seq) {
		return false
	}
	st.mu.Lock()
	_, ok := st.set[seq]
	st.mu.Unlock()
	return ok
}

// Record marks seq as processed/acked (idempotent). Delegates to the
// test-and-set CheckRecord and discards the "new" result.
func (st *slotState) Record(seq uint64) error {
	_, err := st.CheckRecord(seq)
	return err
}

func (st *slotState) CheckRecord(seq uint64) (bool, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.tombed {
		return true, nil
	}
	if _, exists := st.set[seq]; exists {
		return false, nil
	}
	now := st.wal.cfg.nowFn()
	st.set[seq] = now
	st.fifo = append(st.fifo, seq)
	if st.oldestTs == 0 {
		st.oldestTs = now
	}
	st.evictLocked()
	st.refreshBoundsLocked(seq)

	// Persist Record (append while holding slot.mu → disk order == memory order).
	if err := st.wal.appendSeqOp(opRecord, st.slotID, uint64(now), seq); err != nil {
		return true, err
	}
	st.wal.mRecorded.Add(1)
	st.wal.recsSinceSnap.Add(1)
	return true, nil
}

func (st *slotState) Confirm(seq uint64) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, ok := st.set[seq]; ok {
		delete(st.set, seq)
		if len(st.set) == 0 {
			st.oldestTs = 0
		}
	}
	now := st.wal.cfg.nowFn()
	if err := st.wal.appendSeqOp(opConfirm, st.slotID, uint64(now), seq); err != nil {
		return err
	}
	st.wal.mConfirmed.Add(1)
	return nil
}

// ConfirmUpTo drops every live seq ≤ cursor for this slot in a single pass —
// the server's ack cursor (from AckBatchResp/AckStateRep) means the broker
// will never redeliver anything at or below it, so the ackstore need not
// remember them. Persists ONE opConfirmUpTo record covering the whole range
// (compact vs one Confirm per seq). Returns the number dropped.
func (st *slotState) ConfirmUpTo(cursor uint64) (int, error) {
	st.mu.Lock()
	removed := 0
	for seq := range st.set {
		if seq <= cursor {
			delete(st.set, seq)
			removed++
		}
	}
	if removed > 0 {
		st.recomputeOldestLocked()
		st.compactFifoLocked()
	}
	now := st.wal.cfg.nowFn()
	err := st.wal.appendSeqOp(opConfirmUpTo, st.slotID, uint64(now), cursor)
	st.mu.Unlock()
	if err != nil {
		return removed, err
	}
	if removed > 0 {
		st.wal.mConfirmed.Add(uint64(removed))
	}
	return removed, nil
}

// evictLocked enforces MaxCap by dropping oldest fifo entries. Confirmed/dead
// seqs (not in set) are skipped without an Expire record (they were already
// confirmed). Assumes st.mu held.
func (st *slotState) evictLocked() {
	cap := st.wal.cfg.MaxCap
	for len(st.fifo) > cap {
		old := st.fifo[0]
		st.fifo = st.fifo[1:]
		delete(st.set, old)
	}
}

// refreshBoundsLocked updates min/max after inserting seq. Bounds are
// conservative optimizations (see store.go); correctness never depends on
// them. Assumes st.mu held.
func (st *slotState) refreshBoundsLocked(inserted uint64) {
	if cur := st.maxSeq.Load(); inserted > cur {
		st.maxSeq.Store(inserted)
	}
	if len(st.fifo) > 0 {
		st.minSeq.Store(st.fifo[0])
	}
}

func (st *slotState) Info() SlotInfo {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.infoLocked()
}

func (st *slotState) infoLocked() SlotInfo {
	var minSeq, maxSeq uint64
	for seq := range st.set {
		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return SlotInfo{
		SlotID:       st.slotID,
		Stream:       st.stream,
		Consumer:     st.consumer,
		Live:         len(st.set),
		MinSeq:       minSeq,
		MaxSeq:       maxSeq,
		OldestTsMs:   uint64(st.oldestTs),
		RegisteredAt: st.registeredAt,
	}
}

// --- admin ---

func (w *WAL) ListSlots() []SlotInfo {
	w.symMu.RLock()
	sts := make([]*slotState, 0, len(w.byName))
	for _, st := range w.byName {
		sts = append(sts, st)
	}
	w.symMu.RUnlock()

	out := make([]SlotInfo, 0, len(sts))
	for _, st := range sts {
		out = append(out, st.Info())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SlotID < out[j].SlotID })
	return out
}

func (w *WAL) SlotInfoByName(stream, consumer string) (SlotInfo, bool) {
	w.symMu.RLock()
	st := w.byName[slotKey(stream, consumer)]
	w.symMu.RUnlock()
	if st == nil {
		return SlotInfo{}, false
	}
	return st.Info(), true
}

func (w *WAL) DeleteSlot(stream, consumer string) error {
	key := slotKey(stream, consumer)
	w.symMu.Lock()
	st := w.byName[key]
	if st == nil {
		w.symMu.Unlock()
		return ErrUnknownSlot
	}
	delete(w.byName, key)
	if int(st.slotID) < len(w.byID) {
		w.byID[st.slotID] = nil
	}
	w.symMu.Unlock()

	st.mu.Lock()
	st.tombed = true
	st.set = make(map[uint64]int64)
	st.fifo = nil
	st.oldestTs = 0
	st.minSeq.Store(0)
	st.maxSeq.Store(0)
	now := w.cfg.nowFn()
	err := w.appendTombstone(st.slotID, uint64(now))
	st.mu.Unlock()
	if err != nil {
		return err
	}
	w.mTombstoned.Add(1)
	return nil
}

func (w *WAL) Metrics() Metrics {
	w.symMu.RLock()
	slots := len(w.byName)
	var live uint64
	for _, st := range w.byName {
		st.mu.Lock()
		live += uint64(len(st.set))
		st.mu.Unlock()
	}
	w.symMu.RUnlock()

	w.writer.mu.Lock()
	size := w.writer.size
	w.writer.mu.Unlock()

	return Metrics{
		Slots:          slots,
		LiveEntries:    live,
		Recorded:       w.mRecorded.Load(),
		Confirmed:      w.mConfirmed.Load(),
		Expired:        w.mExpired.Load(),
		Registered:     w.mRegistered.Load(),
		Tombstoned:     w.mTombstoned.Load(),
		SyncErrors:     w.mSyncErrors.Load(),
		FileSize:       size,
		LastSyncMs:     w.mLastSyncMs.Load(),
		LastSnapshotMs: w.mLastSnapMs.Load(),
	}
}

// --- durability ---

func (w *WAL) Sync() error {
	w.writer.mu.Lock()
	if w.writer.closed {
		w.writer.mu.Unlock()
		return ErrClosed
	}
	err := w.writer.bw.Flush()
	if err == nil && w.cfg.Fsync {
		err = w.writer.f.Sync()
	}
	w.writer.mu.Unlock()

	if err != nil {
		w.mSyncErrors.Add(1)
		return err
	}
	w.mLastSyncMs.Store(w.cfg.nowFn())

	// Auto-compaction when the file grows past the threshold — reclaims the
	// space held by tombstones (a WAL never deletes records in place). Checked
	// before the snapshot so compaction supersedes it when both would fire.
	if cb := w.cfg.CompactAtBytes; cb > 0 {
		w.writer.mu.Lock()
		size := w.writer.size
		w.writer.mu.Unlock()
		if size >= cb {
			return w.Compact()
		}
	}

	// Auto-snapshot when threshold crossed.
	if n := w.cfg.SnapshotEveryN; n > 0 && int(w.recsSinceSnap.Load()) >= n {
		if serr := w.Snapshot(); serr != nil {
			return serr
		}
	}
	return nil
}

func (w *WAL) Restore() error { return nil } // replay happened in OpenWAL

func (w *WAL) Close() error {
	w.stopOnce.Do(func() { close(w.stopCh) })
	w.sweeping.Wait()

	w.writer.mu.Lock()
	defer w.writer.mu.Unlock()
	if w.writer.closed {
		return nil
	}
	var err error
	if w.writer.bw != nil {
		err = w.writer.bw.Flush()
	}
	if w.cfg.Fsync {
		if serr := w.writer.f.Sync(); serr != nil && err == nil {
			err = serr
		}
	}
	if cerr := w.writer.f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	w.writer.closed = true
	return err
}

// --- TTL sweep ---

func (w *WAL) sweepLoop() {
	defer w.sweeping.Done()
	ticker := time.NewTicker(w.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.sweepOnce()
		}
	}
}

func (w *WAL) sweepOnce() {
	ttl := w.cfg.TTL
	if ttl <= 0 {
		return
	}
	cutoff := w.cfg.nowFn() - ttl.Milliseconds()

	w.symMu.RLock()
	sts := make([]*slotState, 0, len(w.byName))
	for _, st := range w.byName {
		sts = append(sts, st)
	}
	w.symMu.RUnlock()

	for _, st := range sts {
		st.mu.Lock()
		if len(st.set) == 0 {
			st.mu.Unlock()
			continue
		}
		expired := make([]uint64, 0)
		for seq, ts := range st.set {
			if ts < cutoff {
				expired = append(expired, seq)
			}
		}
		for _, seq := range expired {
			delete(st.set, seq)
			now := w.cfg.nowFn()
			_ = w.appendSeqOp(opExpire, st.slotID, uint64(now), seq) // best-effort
		}
		if len(expired) > 0 {
			w.mExpired.Add(uint64(len(expired)))
			// Recompute oldestTs + bounds from remaining set.
			st.recomputeOldestLocked()
			st.compactFifoLocked()
		}
		st.mu.Unlock()
	}
}

func (st *slotState) recomputeOldestLocked() {
	if len(st.set) == 0 {
		st.oldestTs = 0
		return
	}
	oldest := int64(0)
	for _, ts := range st.set {
		if oldest == 0 || ts < oldest {
			oldest = ts
		}
	}
	st.oldestTs = oldest
}

// compactFifoLocked drops fifo entries no longer in the set and refreshes the
// min bound. Assumes st.mu held.
func (st *slotState) compactFifoLocked() {
	if len(st.fifo) == 0 {
		return
	}
	kept := st.fifo[:0]
	var minSeq, maxSeq uint64
	for _, seq := range st.fifo {
		if _, ok := st.set[seq]; ok {
			kept = append(kept, seq)
			if minSeq == 0 || seq < minSeq {
				minSeq = seq
			}
			if seq > maxSeq {
				maxSeq = seq
			}
		}
	}
	st.fifo = kept
	st.minSeq.Store(minSeq)
	// maxSeq stays monotonic — do NOT lower it (conservative).
}

// --- snapshot ---

// Snapshot writes one Snapshot record per non-empty slot, capturing its live
// set. Speeds Restore by letting replay skip Record/Confirm churn before the
// snapshot. Callable manually; also auto-triggered by Sync when
// SnapshotEveryN records have accumulated.
func (w *WAL) Snapshot() error {
	w.symMu.RLock()
	sts := make([]*slotState, 0, len(w.byName))
	for _, st := range w.byName {
		sts = append(sts, st)
	}
	w.symMu.RUnlock()

	now := uint64(w.cfg.nowFn())
	for _, st := range sts {
		st.mu.Lock()
		if len(st.set) == 0 {
			st.mu.Unlock()
			continue
		}
		seqs := make([]uint64, 0, len(st.set))
		for seq := range st.set {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		buf := make([]byte, snapshotFrameSize(len(seqs)))
		encodeSnapshot(buf, st.slotID, now, seqs[0], seqs[len(seqs)-1], seqs)
		err := w.append(buf)
		st.mu.Unlock()
		if err != nil {
			return err
		}
	}
	w.recsSinceSnap.Store(0)
	w.mLastSnapMs.Store(int64(now))
	return nil
}
