package proto

import "encoding/binary"

// PublishWireSize returns the total frame size (header + body) for a Publish
// frame with the given payload/subject/msg_id sizes. Cheap: pure arithmetic,
// safe for hot-path sizing checks (stack buffer vs heap alloc branch).
func PublishWireSize(subjectLen, msgIDLen, payloadLen int) int {
	return HeaderSize + 8 + subjectLen + msgIDLen + payloadLen
}

// EncodePublishInto encodes a Publish frame directly into buf and returns the
// number of bytes written. buf MUST have at least PublishWireSize(...) capacity;
// no bounds check beyond a subslice — caller owns the buffer lifetime. Lets
// the hot path allocate a stack scratch array and skip the heap when the
// frame fits (mirrors Rust's WriteFrame::Inline for size ≤ INLINE_CAP).
func EncodePublishInto(buf []byte, seq uint64, streamID uint32, subject, msgID, payload []byte, flags byte) int {
	subjLen := len(subject)
	msgIDLen := len(msgID)
	bodyLen := 8 + subjLen + msgIDLen + len(payload)
	total := HeaderSize + bodyLen

	EncodeHeader(buf, Header{
		Action:     ActionPublish,
		Flags:      flags,
		EntryFlags: EntryFlagNone,
		MsgLen:     uint32(bodyLen),
		Seq:        seq,
	})

	body := buf[HeaderSize:total]
	binary.LittleEndian.PutUint32(body[0:4], streamID)
	binary.LittleEndian.PutUint16(body[4:6], uint16(subjLen))
	binary.LittleEndian.PutUint16(body[6:8], uint16(msgIDLen))
	copy(body[8:], subject)
	copy(body[8+subjLen:], msgID)
	copy(body[8+subjLen+msgIDLen:], payload)

	return total
}

// EncodePublish builds a full Publish frame (header + body). Convenience
// wrapper that always heap-allocates — prefer EncodePublishInto with a
// caller-owned buffer on the hot path.
// Body layout: stream_id(4) + subject_len(2) + msg_id_len(2) + subject + msg_id + payload
func EncodePublish(seq uint64, streamID uint32, subject, msgID, payload []byte, flags byte) []byte {
	frame := make([]byte, PublishWireSize(len(subject), len(msgID), len(payload)))
	EncodePublishInto(frame, seq, streamID, subject, msgID, payload, flags)
	return frame
}

// EncodePublishBatch builds a PublishBatch frame.
// Body layout: stream_id(4) + count(4) + entries...
// Each entry: subject_len(2) + msg_id_len(2) + payload_len(4) + subject + msg_id + payload
func EncodePublishBatch(seq uint64, streamID uint32, entries []BatchEntry, flags byte) []byte {
	// Calculate total body size
	bodyLen := 8 // stream_id + count
	for i := range entries {
		bodyLen += 8 + len(entries[i].Subject) + len(entries[i].MsgID) + len(entries[i].Payload)
	}

	frame := make([]byte, HeaderSize+bodyLen)
	EncodeHeader(frame, Header{
		Action:     ActionPublishBatch,
		Flags:      flags,
		EntryFlags: EntryFlagNone,
		MsgLen:     uint32(bodyLen),
		Seq:        seq,
	})

	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], streamID)
	binary.LittleEndian.PutUint32(body[4:8], uint32(len(entries)))

	off := 8
	for i := range entries {
		sl := len(entries[i].Subject)
		ml := len(entries[i].MsgID)
		pl := len(entries[i].Payload)

		binary.LittleEndian.PutUint16(body[off:off+2], uint16(sl))
		binary.LittleEndian.PutUint16(body[off+2:off+4], uint16(ml))
		binary.LittleEndian.PutUint32(body[off+4:off+8], uint32(pl))
		off += 8
		copy(body[off:], entries[i].Subject)
		off += sl
		copy(body[off:], entries[i].MsgID)
		off += ml
		copy(body[off:], entries[i].Payload)
		off += pl
	}

	return frame
}

// BatchEntry represents one entry in a batch publish.
type BatchEntry struct {
	Subject []byte
	MsgID   []byte
	Payload []byte
}

// EncodePublishDelayed builds a PublishDelayed frame.
// Body layout: stream_id(4) + subject_len(2) + _pad(2) + delay_ms(8) + subject + payload
func EncodePublishDelayed(seq uint64, streamID uint32, subject, payload []byte, delayMs uint64, flags byte) []byte {
	subjLen := len(subject)
	bodyLen := 16 + subjLen + len(payload)

	frame := make([]byte, HeaderSize+bodyLen)
	EncodeHeader(frame, Header{
		Action:     ActionPublishDelayed,
		Flags:      flags,
		EntryFlags: EntryFlagNone,
		MsgLen:     uint32(bodyLen),
		Seq:        seq,
	})

	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], streamID)
	binary.LittleEndian.PutUint16(body[4:6], uint16(subjLen))
	binary.LittleEndian.PutUint16(body[6:8], 0) // msg_id_len reserved
	binary.LittleEndian.PutUint64(body[8:16], delayMs)
	copy(body[16:], subject)
	copy(body[16+subjLen:], payload)

	return frame
}

// EncodePublishWithReply builds a PublishWithReply frame.
// Body layout (see arbitro-proto v2::ingress::pub_with_reply):
//   stream_id(4) + subject_len(2) + reply_len(2) + msg_id_len(2) + _pad(2) +
//   subject + reply_to + msg_id + payload
func EncodePublishWithReply(seq uint64, streamID uint32, subject, replyTo, msgID, payload []byte, flags byte) []byte {
	subjLen := len(subject)
	replyLen := len(replyTo)
	msgIDLen := len(msgID)
	bodyLen := 12 + subjLen + replyLen + msgIDLen + len(payload)

	frame := make([]byte, HeaderSize+bodyLen)
	EncodeHeader(frame, Header{
		Action:     ActionPublishWithReply,
		Flags:      flags,
		EntryFlags: EntryFlagNone,
		MsgLen:     uint32(bodyLen),
		Seq:        seq,
	})

	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], streamID)
	binary.LittleEndian.PutUint16(body[4:6], uint16(subjLen))
	binary.LittleEndian.PutUint16(body[6:8], uint16(replyLen))
	binary.LittleEndian.PutUint16(body[8:10], uint16(msgIDLen))
	binary.LittleEndian.PutUint16(body[10:12], 0) // reserved pad
	off := 12
	copy(body[off:], subject)
	off += subjLen
	copy(body[off:], replyTo)
	off += replyLen
	copy(body[off:], msgID)
	off += msgIDLen
	copy(body[off:], payload)

	return frame
}
