package proto

import "encoding/binary"

// EncodeAck builds a 32-byte Ack frame (header 16 + body 16).
func EncodeAck(seq uint64, consumerID uint32, subjectHash uint32, ackSeq uint64) []byte {
	frame := make([]byte, 32)
	EncodeHeader(frame, Header{
		Action: ActionAck,
		Flags:  FlagNone,
		MsgLen: 16,
		Seq:    seq,
	})
	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], consumerID)
	binary.LittleEndian.PutUint32(body[4:8], subjectHash)
	binary.LittleEndian.PutUint64(body[8:16], ackSeq)
	return frame
}

// EncodeNack builds a 32-byte Nack frame (header 16 + body 16).
func EncodeNack(seq uint64, consumerID uint32, subjectHash uint32, nackSeq uint64) []byte {
	frame := make([]byte, 32)
	EncodeHeader(frame, Header{
		Action: ActionNack,
		Flags:  FlagNone,
		MsgLen: 16,
		Seq:    seq,
	})
	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], consumerID)
	binary.LittleEndian.PutUint32(body[4:8], subjectHash)
	binary.LittleEndian.PutUint64(body[8:16], nackSeq)
	return frame
}

// AckEntry is one entry in a BatchAck or BatchNack.
type AckEntry struct {
	Seq         uint64
	SubjectHash uint32
}

// NackEntry is one entry in a BatchNack (with delay).
type NackEntry struct {
	Seq         uint64
	SubjectHash uint32
	DelayMs     uint32
}

// EncodeBatchAck builds a BatchAck frame.
// Body: consumer_id(4) + count(4) + entries(count * 16)
// Each entry: seq(8) + subject_hash(4) + pad(4)
func EncodeBatchAck(seq uint64, consumerID uint32, entries []AckEntry) []byte {
	bodyLen := 8 + len(entries)*16
	frame := make([]byte, HeaderSize+bodyLen)
	EncodeHeader(frame, Header{
		Action: ActionBatchAck,
		Flags:  FlagNone,
		MsgLen: uint32(bodyLen),
		Seq:    seq,
	})
	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], consumerID)
	binary.LittleEndian.PutUint32(body[4:8], uint32(len(entries)))
	off := 8
	for i := range entries {
		binary.LittleEndian.PutUint64(body[off:off+8], entries[i].Seq)
		binary.LittleEndian.PutUint32(body[off+8:off+12], entries[i].SubjectHash)
		binary.LittleEndian.PutUint32(body[off+12:off+16], 0) // pad
		off += 16
	}
	return frame
}

// EncodeBatchNack builds a BatchNack frame.
// Body: consumer_id(4) + count(4) + entries(count * 16)
// Each entry: seq(8) + subject_hash(4) + delay_ms(4)
func EncodeBatchNack(seq uint64, consumerID uint32, entries []NackEntry) []byte {
	bodyLen := 8 + len(entries)*16
	frame := make([]byte, HeaderSize+bodyLen)
	EncodeHeader(frame, Header{
		Action: ActionBatchNack,
		Flags:  FlagNone,
		MsgLen: uint32(bodyLen),
		Seq:    seq,
	})
	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], consumerID)
	binary.LittleEndian.PutUint32(body[4:8], uint32(len(entries)))
	off := 8
	for i := range entries {
		binary.LittleEndian.PutUint64(body[off:off+8], entries[i].Seq)
		binary.LittleEndian.PutUint32(body[off+8:off+12], entries[i].SubjectHash)
		binary.LittleEndian.PutUint32(body[off+12:off+16], entries[i].DelayMs)
		off += 16
	}
	return frame
}

// EncodePing builds a header-only Ping frame (16 bytes, no body).
func EncodePing(seq uint64) []byte {
	frame := make([]byte, HeaderSize)
	EncodeHeader(frame, Header{
		Action: ActionPing,
		Flags:  FlagNone,
		MsgLen: 0,
		Seq:    seq,
	})
	return frame
}
