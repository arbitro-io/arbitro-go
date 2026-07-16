package ackstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"time"
)

// rebuildSlot accumulates one slot's reconstructed state during replay.
type rebuildSlot struct {
	st   *slotState
	live map[uint64]int64 // seq → ts
}

// replay rebuilds the in-memory slot table + live sets from the log file.
// Robust against torn writes and corruption: it reads frames until the tail
// cannot supply a full, CRC-valid frame, then TRUNCATES the file at the last
// good record boundary so future appends start from a consistent point.
//
// Applies the TTL filter during replay: a Record whose ts is older than the
// TTL window is not resurrected (it would be swept immediately anyway).
//
// Called once from OpenWAL, before the buffered writer is attached.
func (w *WAL) replay() error {
	f := w.writer.f
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil // fresh file
	}

	r := bufio.NewReaderSize(f, 64*1024)

	// Verify magic.
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		// File too short to even hold the magic — treat as empty, truncate.
		return w.truncateAt(0)
	}
	if magic != fileMagic {
		return fmt.Errorf("ackstore: bad file magic %q", magic[:])
	}

	// Replay accumulators. We rebuild the slot table from Register/Snapshot,
	// and live sets from Record minus Confirm/Expire, with Tombstone wiping a
	// slot. A Snapshot resets a slot's live set to exactly its listed seqs
	// (later Records/Confirms after the snapshot still apply).
	slots := make(map[uint32]*rebuildSlot)
	names := make(map[uint32][2]string) // slotID → {stream, consumer}

	ttlCutoff := int64(0)
	if w.cfg.TTL > 0 {
		ttlCutoff = w.cfg.nowFn() - w.cfg.TTL.Milliseconds()
	}

	// goodOffset = byte offset just past the last fully-valid frame (starts
	// after the magic). On any torn/corrupt tail we truncate here.
	goodOffset := int64(len(fileMagic))

	var lenBuf [4]byte
	var dec decodedRecord
	for {
		// Read length prefix.
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // clean end or torn length prefix
			}
			return err
		}
		plen := int(binary.LittleEndian.Uint32(lenBuf[:]))
		if plen <= 0 || plen > 64*1024*1024 {
			break // implausible length → torn write, stop
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // torn payload
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
			break // torn crc
		}
		want := binary.LittleEndian.Uint32(crcBuf[:])
		if crc32.Checksum(payload, crcTable) != want {
			break // corrupt frame → stop, truncate here
		}
		if err := decodePayload(payload, &dec); err != nil {
			break // malformed → stop, truncate here
		}

		w.applyReplayRecord(&dec, slots, names, ttlCutoff)

		goodOffset += int64(frameLenSize + plen + frameCRCSize)
	}

	// Materialize the rebuilt slots into the live WAL structures.
	var maxID uint32
	orderedIDs := make([]uint32, 0, len(slots))
	for id := range slots {
		orderedIDs = append(orderedIDs, id)
	}
	sort.Slice(orderedIDs, func(i, j int) bool { return orderedIDs[i] < orderedIDs[j] })

	for _, id := range orderedIDs {
		rs := slots[id]
		nm := names[id]
		st := rs.st
		if st == nil {
			// Records referencing a slot that never had a Register — skip
			// (should not happen with a consistent log, but be defensive).
			continue
		}
		st.stream = nm[0]
		st.consumer = nm[1]
		// Populate the live set + fifo (sorted for deterministic bounds).
		seqs := make([]uint64, 0, len(rs.live))
		for seq := range rs.live {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		st.set = make(map[uint64]int64, len(seqs))
		st.fifo = st.fifo[:0]
		var oldest int64
		for _, seq := range seqs {
			ts := rs.live[seq]
			st.set[seq] = ts
			st.fifo = append(st.fifo, seq)
			if oldest == 0 || ts < oldest {
				oldest = ts
			}
		}
		st.oldestTs = oldest
		if len(seqs) > 0 {
			st.minSeq.Store(seqs[0])
			st.maxSeq.Store(seqs[len(seqs)-1])
		}
		w.byName[slotKey(nm[0], nm[1])] = st
		for uint32(len(w.byID)) <= id {
			w.byID = append(w.byID, nil)
		}
		w.byID[id] = st
		if id >= maxID {
			maxID = id + 1
		}
	}
	w.nextID = maxID

	// Truncate any torn/corrupt tail so future appends are consistent.
	if goodOffset < info.Size() {
		if err := w.truncateAt(goodOffset); err != nil {
			return err
		}
	}
	return nil
}

// applyReplayRecord folds one decoded record into the rebuild accumulators.
func (w *WAL) applyReplayRecord(
	dec *decodedRecord,
	slots map[uint32]*rebuildSlot,
	names map[uint32][2]string,
	ttlCutoff int64,
) {
	ensure := func(id uint32) *rebuildSlot {
		rs := slots[id]
		if rs == nil {
			rs = &rebuildSlot{live: make(map[uint64]int64)}
			slots[id] = rs
		}
		return rs
	}

	switch dec.op {
	case opRegister:
		rs := ensure(dec.slotID)
		if rs.st == nil {
			rs.st = &slotState{
				slotID:       dec.slotID,
				registeredAt: time.UnixMilli(int64(dec.tsMs)),
				wal:          w,
				set:          make(map[uint64]int64),
			}
		}
		names[dec.slotID] = [2]string{string(dec.stream), string(dec.consumer)}
	case opRecord:
		if ttlCutoff > 0 && int64(dec.tsMs) < ttlCutoff {
			return // already expired — don't resurrect
		}
		rs := ensure(dec.slotID)
		rs.live[dec.seq] = int64(dec.tsMs)
	case opConfirm, opExpire:
		rs := ensure(dec.slotID)
		delete(rs.live, dec.seq)
	case opConfirmUpTo:
		// dec.seq holds the cursor — drop every live seq ≤ cursor.
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
		// Snapshot resets the live set to exactly its listed seqs; ts is the
		// snapshot time (we lose per-entry ts precision across a snapshot,
		// which is acceptable — TTL is coarse).
		rs.live = make(map[uint64]int64, len(dec.seqs))
		for _, seq := range dec.seqs {
			if ttlCutoff > 0 && int64(dec.tsMs) < ttlCutoff {
				continue
			}
			rs.live[seq] = int64(dec.tsMs)
		}
	}
}

// truncateAt trims the file to n bytes and repositions at EOF.
func (w *WAL) truncateAt(n int64) error {
	f := w.writer.f
	if err := f.Truncate(n); err != nil {
		return err
	}
	if _, err := f.Seek(n, io.SeekStart); err != nil {
		return err
	}
	// If we truncated away everything (including magic), rewrite the magic.
	if n < int64(len(fileMagic)) {
		if _, err := f.WriteAt(fileMagic[:], 0); err != nil {
			return err
		}
		if _, err := f.Seek(int64(len(fileMagic)), io.SeekStart); err != nil {
			return err
		}
		w.writer.size = int64(len(fileMagic))
	} else {
		w.writer.size = n
	}
	return nil
}
