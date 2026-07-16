package ackstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Compact rewrites the log so it contains ONLY the live state — a WAL never
// deletes records in place, so the file grows with every Record/Confirm/
// Expire/ConfirmUpTo tombstone until compaction reclaims the space.
//
// Correctness: the writer lock is held for the whole operation, so no appends
// run concurrently. The OLD file (flushed to a stable point) is re-scanned to
// derive the live sets — reading the file rather than the in-memory slot maps
// avoids a lock-order inversion (append path is slot.mu → writer.mu; taking
// slot.mu while holding writer.mu would deadlock). The compacted file is
// written to a temp path, fsync'd, and atomically renamed over the original.
//
// After Compact the on-disk representation is `magic + {Register, Snapshot}`
// per non-empty slot; a subsequent Restore rebuilds identical state. In-memory
// state (byName/byID/nextID) is untouched.
func (w *WAL) Compact() error {
	w.writer.mu.Lock()
	defer w.writer.mu.Unlock()
	if w.writer.closed {
		return ErrClosed
	}

	// Flush so the current file is complete up to a stable point.
	if err := w.writer.bw.Flush(); err != nil {
		return err
	}

	// Re-scan the old file into standalone live-set structures (TTL-filtered).
	ttlCutoff := int64(0)
	if w.cfg.TTL > 0 {
		ttlCutoff = w.cfg.nowFn() - w.cfg.TTL.Milliseconds()
	}
	slots, names, err := scanLiveState(w.writer.f, ttlCutoff)
	if err != nil {
		return err
	}

	// Write the compacted content to a temp file in the same dir.
	tmpPath := filepath.Join(w.cfg.Dir, "ackstore.log.compact")
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("ackstore: compact open temp: %w", err)
	}
	newSize, werr := writeCompacted(tmp, slots, names, uint64(w.cfg.nowFn()))
	if werr != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return werr
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Swap: close old, atomic rename temp → log, reopen for appends.
	oldPath := filepath.Join(w.cfg.Dir, "ackstore.log")
	if err := w.writer.f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, oldPath); err != nil {
		// Best effort reopen of the original so the WAL stays usable.
		w.reopenLocked(oldPath)
		return fmt.Errorf("ackstore: compact rename: %w", err)
	}
	if err := w.reopenLocked(oldPath); err != nil {
		return err
	}
	w.writer.size = newSize
	w.recsSinceSnap.Store(0)
	return nil
}

// reopenLocked reopens the log file at path for appending and resets the
// buffered writer. Caller holds writer.mu.
func (w *WAL) reopenLocked(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("ackstore: compact reopen: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	w.writer.f = f
	w.writer.bw = bufio.NewWriterSize(f, w.cfg.BufferSize)
	return nil
}

// scanLiveState reads the whole file and folds records into per-slot live
// sets (Records minus Confirm/Expire/ConfirmUpTo, Tombstone wipes a slot,
// Snapshot resets). TTL-filters Records older than the cutoff. Stops cleanly
// at the first torn/corrupt frame (same robustness as replay).
func scanLiveState(f *os.File, ttlCutoff int64) (map[uint32]*rebuildSlot, map[uint32][2]string, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}
	r := bufio.NewReaderSize(f, 64*1024)

	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return map[uint32]*rebuildSlot{}, map[uint32][2]string{}, nil // empty
	}
	if magic != fileMagic {
		return nil, nil, fmt.Errorf("ackstore: compact bad magic")
	}

	slots := make(map[uint32]*rebuildSlot)
	names := make(map[uint32][2]string)
	ensure := func(id uint32) *rebuildSlot {
		rs := slots[id]
		if rs == nil {
			rs = &rebuildSlot{live: make(map[uint64]int64)}
			slots[id] = rs
		}
		return rs
	}

	var lenBuf [4]byte
	var dec decodedRecord
	for {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			break
		}
		plen := int(binary.LittleEndian.Uint32(lenBuf[:]))
		if plen <= 0 || plen > 64*1024*1024 {
			break
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(r, payload); err != nil {
			break
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
			break
		}
		if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(crcBuf[:]) {
			break
		}
		if decodePayload(payload, &dec) != nil {
			break
		}
		switch dec.op {
		case opRegister:
			ensure(dec.slotID)
			names[dec.slotID] = [2]string{string(dec.stream), string(dec.consumer)}
		case opRecord:
			if ttlCutoff > 0 && int64(dec.tsMs) < ttlCutoff {
				continue
			}
			ensure(dec.slotID).live[dec.seq] = int64(dec.tsMs)
		case opConfirm, opExpire:
			delete(ensure(dec.slotID).live, dec.seq)
		case opConfirmUpTo:
			rs := ensure(dec.slotID)
			for seq := range rs.live {
				if seq <= dec.seq {
					delete(rs.live, seq)
				}
			}
		case opTombstone:
			delete(slots, dec.slotID)
			delete(names, dec.slotID)
		case opSnapshot:
			rs := ensure(dec.slotID)
			rs.live = make(map[uint64]int64, len(dec.seqs))
			if ttlCutoff > 0 && int64(dec.tsMs) < ttlCutoff {
				continue
			}
			for _, seq := range dec.seqs {
				rs.live[seq] = int64(dec.tsMs)
			}
		}
	}
	return slots, names, nil
}

// writeCompacted writes magic + {Register, Snapshot} per non-empty slot to f.
// Returns the total bytes written. Slots are emitted in slotID order for
// deterministic output.
func writeCompacted(f *os.File, slots map[uint32]*rebuildSlot, names map[uint32][2]string, nowMs uint64) (int64, error) {
	bw := bufio.NewWriterSize(f, 64*1024)
	if _, err := bw.Write(fileMagic[:]); err != nil {
		return 0, err
	}
	size := int64(len(fileMagic))

	ids := make([]uint32, 0, len(slots))
	for id := range slots {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		rs := slots[id]
		if len(rs.live) == 0 {
			continue // drop empty slots entirely
		}
		nm := names[id]
		// Register.
		regBuf := make([]byte, registerFrameSize(len(nm[0]), len(nm[1])))
		n := encodeRegister(regBuf, id, nowMs, []byte(nm[0]), []byte(nm[1]))
		if _, err := bw.Write(regBuf[:n]); err != nil {
			return 0, err
		}
		size += int64(n)
		// Snapshot (sorted seqs).
		seqs := make([]uint64, 0, len(rs.live))
		for seq := range rs.live {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		snapBuf := make([]byte, snapshotFrameSize(len(seqs)))
		m := encodeSnapshot(snapBuf, id, nowMs, seqs[0], seqs[len(seqs)-1], seqs)
		if _, err := bw.Write(snapBuf[:m]); err != nil {
			return 0, err
		}
		size += int64(m)
	}
	if err := bw.Flush(); err != nil {
		return 0, err
	}
	return size, nil
}
