package ackstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Wire format
//
// File starts with an 8-byte magic header, then a sequence of length-framed
// records:
//
//	[len u32][payload len bytes][crc32c u32 over payload]
//
// Length-framing (rather than fixed-size records) lets a single reader loop
// handle every op — fixed or variable — and detect torn writes: if the tail
// of the file cannot supply a full frame, replay truncates there. crc32c
// (Castagnoli, hardware-accelerated) guards against silent corruption; a bad
// CRC also truncates.
//
// payload[0] is the op byte; the rest is op-specific.

var fileMagic = [8]byte{'A', 'R', 'B', 'W', 'A', 'L', '0', '1'}

const (
	// op codes (payload[0])
	opRegister     byte = 1 // (slot_id, ts, stream, consumer) — symbol table entry
	opRecord       byte = 2 // (slot_id, ts, seq)  — "handler ran for this msg"
	opConfirm      byte = 3 // (slot_id, ts, seq)  — broker acked, drop it
	opExpire       byte = 4 // (slot_id, ts, seq)  — TTL swept, drop it
	opTombstone    byte = 5 // (slot_id, ts)       — forget this slot entirely
	opSnapshot     byte = 6 // (slot_id, ts, min, max, count, seqs[]) — checkpoint
	opConfirmUpTo  byte = 7 // (slot_id, ts, cursor) — drop ALL seq ≤ cursor in one record
)

// Framing constants.
const (
	frameLenSize = 4 // u32 payload length prefix
	frameCRCSize = 4 // u32 crc32c suffix
	frameOverhead = frameLenSize + frameCRCSize

	// Fixed payload sizes.
	seqPayloadSize  = 1 + 4 + 8 + 8 // op + slot_id + ts + seq  (Record/Confirm/Expire)
	tombPayloadSize = 1 + 4 + 8     // op + slot_id + ts
	regFixedPrefix  = 1 + 4 + 8 + 2 + 2 // op + slot_id + ts + stream_len + consumer_len
	snapFixedPrefix = 1 + 4 + 8 + 8 + 8 + 4 // op + slot_id + ts + min + max + count
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	// errShortFrame means the tail of the file is truncated (torn write) —
	// replay stops cleanly at the last good record.
	errShortFrame = errors.New("ackstore: short frame")
	// errBadCRC means a record's payload failed its checksum — replay stops
	// and the file is truncated at the record boundary.
	errBadCRC = errors.New("ackstore: crc mismatch")
	// errBadRecord means a payload was structurally invalid (wrong length for
	// its op). Treated like a torn write.
	errBadRecord = errors.New("ackstore: malformed record")
)

// --- encoders: write a full framed record into dst, return bytes written ---
//
// Each encoder writes [len][payload][crc]. dst must have cap >= the returned
// size; the hot-path caller sizes a stack buffer via the *Size helpers.

func recordFrameSize() int      { return frameOverhead + seqPayloadSize }
func tombstoneFrameSize() int   { return frameOverhead + tombPayloadSize }
func registerFrameSize(streamLen, consumerLen int) int {
	return frameOverhead + regFixedPrefix + streamLen + consumerLen
}
func snapshotFrameSize(count int) int {
	return frameOverhead + snapFixedPrefix + count*8
}

// encodeSeqOp writes a Record/Confirm/Expire frame.
func encodeSeqOp(dst []byte, op byte, slotID uint32, tsMs uint64, seq uint64) int {
	payload := dst[frameLenSize : frameLenSize+seqPayloadSize]
	payload[0] = op
	binary.LittleEndian.PutUint32(payload[1:5], slotID)
	binary.LittleEndian.PutUint64(payload[5:13], tsMs)
	binary.LittleEndian.PutUint64(payload[13:21], seq)
	return frameFinish(dst, seqPayloadSize)
}

// encodeTombstone writes a Tombstone frame.
func encodeTombstone(dst []byte, slotID uint32, tsMs uint64) int {
	payload := dst[frameLenSize : frameLenSize+tombPayloadSize]
	payload[0] = opTombstone
	binary.LittleEndian.PutUint32(payload[1:5], slotID)
	binary.LittleEndian.PutUint64(payload[5:13], tsMs)
	return frameFinish(dst, tombPayloadSize)
}

// encodeRegister writes a Register frame.
func encodeRegister(dst []byte, slotID uint32, tsMs uint64, stream, consumer []byte) int {
	plen := regFixedPrefix + len(stream) + len(consumer)
	payload := dst[frameLenSize : frameLenSize+plen]
	payload[0] = opRegister
	binary.LittleEndian.PutUint32(payload[1:5], slotID)
	binary.LittleEndian.PutUint64(payload[5:13], tsMs)
	binary.LittleEndian.PutUint16(payload[13:15], uint16(len(stream)))
	binary.LittleEndian.PutUint16(payload[15:17], uint16(len(consumer)))
	n := 17
	n += copy(payload[n:], stream)
	copy(payload[n:], consumer)
	return frameFinish(dst, plen)
}

// encodeSnapshot writes a Snapshot frame. seqs must be sorted ascending;
// min/max are seqs[0]/seqs[len-1] (caller supplies).
func encodeSnapshot(dst []byte, slotID uint32, tsMs uint64, minSeq, maxSeq uint64, seqs []uint64) int {
	plen := snapFixedPrefix + len(seqs)*8
	payload := dst[frameLenSize : frameLenSize+plen]
	payload[0] = opSnapshot
	binary.LittleEndian.PutUint32(payload[1:5], slotID)
	binary.LittleEndian.PutUint64(payload[5:13], tsMs)
	binary.LittleEndian.PutUint64(payload[13:21], minSeq)
	binary.LittleEndian.PutUint64(payload[21:29], maxSeq)
	binary.LittleEndian.PutUint32(payload[29:33], uint32(len(seqs)))
	off := 33
	for _, s := range seqs {
		binary.LittleEndian.PutUint64(payload[off:off+8], s)
		off += 8
	}
	return frameFinish(dst, plen)
}

// frameFinish stamps the length prefix and crc suffix around a payload that
// has already been written at dst[frameLenSize:frameLenSize+plen]. Returns
// total frame size.
func frameFinish(dst []byte, plen int) int {
	binary.LittleEndian.PutUint32(dst[0:4], uint32(plen))
	payload := dst[frameLenSize : frameLenSize+plen]
	crc := crc32.Checksum(payload, crcTable)
	binary.LittleEndian.PutUint32(dst[frameLenSize+plen:frameLenSize+plen+frameCRCSize], crc)
	return frameLenSize + plen + frameCRCSize
}

// --- decoders (replay) ---

// decodedRecord is the parsed view of one payload. Only the fields relevant
// to its op are populated.
type decodedRecord struct {
	op       byte
	slotID   uint32
	tsMs     uint64
	seq      uint64   // Record/Confirm/Expire
	stream   []byte   // Register (aliases into the payload buffer)
	consumer []byte   // Register
	minSeq   uint64   // Snapshot
	maxSeq   uint64   // Snapshot
	seqs     []uint64 // Snapshot
}

// decodePayload parses a validated payload (crc already checked). Returns
// errBadRecord if the structure is inconsistent.
func decodePayload(payload []byte, out *decodedRecord) error {
	if len(payload) < 1 {
		return errBadRecord
	}
	op := payload[0]
	out.op = op
	switch op {
	case opRecord, opConfirm, opExpire, opConfirmUpTo:
		// opConfirmUpTo reuses the seq field for the cursor value.
		if len(payload) != seqPayloadSize {
			return errBadRecord
		}
		out.slotID = binary.LittleEndian.Uint32(payload[1:5])
		out.tsMs = binary.LittleEndian.Uint64(payload[5:13])
		out.seq = binary.LittleEndian.Uint64(payload[13:21])
	case opTombstone:
		if len(payload) != tombPayloadSize {
			return errBadRecord
		}
		out.slotID = binary.LittleEndian.Uint32(payload[1:5])
		out.tsMs = binary.LittleEndian.Uint64(payload[5:13])
	case opRegister:
		if len(payload) < regFixedPrefix {
			return errBadRecord
		}
		out.slotID = binary.LittleEndian.Uint32(payload[1:5])
		out.tsMs = binary.LittleEndian.Uint64(payload[5:13])
		sl := int(binary.LittleEndian.Uint16(payload[13:15]))
		cl := int(binary.LittleEndian.Uint16(payload[15:17]))
		if 17+sl+cl != len(payload) {
			return errBadRecord
		}
		out.stream = payload[17 : 17+sl]
		out.consumer = payload[17+sl : 17+sl+cl]
	case opSnapshot:
		if len(payload) < snapFixedPrefix {
			return errBadRecord
		}
		out.slotID = binary.LittleEndian.Uint32(payload[1:5])
		out.tsMs = binary.LittleEndian.Uint64(payload[5:13])
		out.minSeq = binary.LittleEndian.Uint64(payload[13:21])
		out.maxSeq = binary.LittleEndian.Uint64(payload[21:29])
		count := int(binary.LittleEndian.Uint32(payload[29:33]))
		if 33+count*8 != len(payload) {
			return errBadRecord
		}
		seqs := make([]uint64, count)
		off := 33
		for i := 0; i < count; i++ {
			seqs[i] = binary.LittleEndian.Uint64(payload[off : off+8])
			off += 8
		}
		out.seqs = seqs
	default:
		return errBadRecord
	}
	return nil
}
