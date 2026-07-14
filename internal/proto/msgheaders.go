package proto

import "encoding/binary"

// User-facing message headers — zero-copy-on-the-wire TLV key/value pairs,
// ported from arbitro-proto's wire::msg_headers.
//
// Wire layout (stored as the "payload" field in the frame when
// entry_flag::HAS_HEADERS is set):
//
//	[payload_len : u32 LE]          user payload size
//	[user_payload : payload_len B]  untouched user data
//	[headers_len : u32 LE]          total headers section size (self-inclusive)
//	[count       : u16 LE]          number of header entries
//	[entries...  : HeaderEntry x N]
//
// Each HeaderEntry:
//
//	[key_len : u8]
//	[val_len : u16 LE]
//	[data    : key_len + val_len B]  key ++ value contiguous

// HdrMsgID is the well-known header key for the idempotency token. Its
// value is also placed in the frame's dedicated msg_id field — this
// replaces the separate msg_id_len field semantics for headers-bearing
// publishes. Mirrors arbitro-proto's HDR_MSG_ID.
const HdrMsgID = "msg-id"

// HeaderEntry is one user header key/value pair.
type HeaderEntry struct {
	Key []byte
	Val []byte
}

// headersSectionSize mirrors HeadersBlock::section_size: headers_len(4) +
// count(2) + sum(3 + len(key) + len(val)) per entry.
func headersSectionSize(entries []HeaderEntry) int {
	total := 6
	for _, e := range entries {
		total += 3 + len(e.Key) + len(e.Val)
	}
	return total
}

// EncodeExtendedPayload builds the ExtendedPayload TLV blob (user payload +
// headers block) that becomes the frame's payload when EntryFlagHasHeaders
// is set. Mirrors arbitro-proto's encode_extended_payload_vec.
func EncodeExtendedPayload(payload []byte, entries []HeaderEntry) []byte {
	section := headersSectionSize(entries)
	total := 4 + len(payload) + section

	buf := make([]byte, total)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	off := 4
	copy(buf[off:], payload)
	off += len(payload)

	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(section))
	binary.LittleEndian.PutUint16(buf[off+4:off+6], uint16(len(entries)))
	off += 6

	for _, e := range entries {
		buf[off] = byte(len(e.Key))
		binary.LittleEndian.PutUint16(buf[off+1:off+3], uint16(len(e.Val)))
		off += 3
		copy(buf[off:], e.Key)
		off += len(e.Key)
		copy(buf[off:], e.Val)
		off += len(e.Val)
	}
	return buf
}

// DecodeExtendedPayload parses an ExtendedPayload TLV blob back into the
// user payload and header entries. Returns an error if the blob is
// malformed or truncated.
func DecodeExtendedPayload(buf []byte) (payload []byte, entries []HeaderEntry, err error) {
	if len(buf) < 4 {
		return nil, nil, errShortExtendedPayload
	}
	plen := int(binary.LittleEndian.Uint32(buf[0:4]))
	if 4+plen > len(buf) {
		return nil, nil, errShortExtendedPayload
	}
	payload = buf[4 : 4+plen]

	rest := buf[4+plen:]
	if len(rest) < 6 {
		return nil, nil, errShortExtendedPayload
	}
	count := int(binary.LittleEndian.Uint16(rest[4:6]))
	off := 6
	entries = make([]HeaderEntry, 0, count)
	for i := 0; i < count; i++ {
		if off+3 > len(rest) {
			return nil, nil, errShortExtendedPayload
		}
		keyLen := int(rest[off])
		valLen := int(binary.LittleEndian.Uint16(rest[off+1 : off+3]))
		off += 3
		if off+keyLen+valLen > len(rest) {
			return nil, nil, errShortExtendedPayload
		}
		key := rest[off : off+keyLen]
		off += keyLen
		val := rest[off : off+valLen]
		off += valLen
		entries = append(entries, HeaderEntry{Key: key, Val: val})
	}
	return payload, entries, nil
}

var errShortExtendedPayload = &extendedPayloadError{"extended payload: buffer too short or malformed"}

type extendedPayloadError struct{ msg string }

func (e *extendedPayloadError) Error() string { return e.msg }

// EncodePublishWithHeaders builds a headers-bearing publish frame.
// extPayload is a pre-built ExtendedPayload blob (see EncodeExtendedPayload);
// msgID is the value extracted from a "msg-id" header entry, if present, and
// is placed in the frame's dedicated msg_id field for broker-side
// idempotency dedup.
//
// The wire action is the ordinary ActionPublish (0x0101) — arbitro-proto
// reserved and deleted a dedicated PublishWithHeaders action code (see
// arbitro-proto action.rs: "0x0105, 0x0106 reserved (deleted
// PublishWithHeaders/PublishBatchHeaders)"). The broker instead
// distinguishes a headers-bearing payload purely via EntryFlagHasHeaders in
// the frame header (PubFrame::encode_into always stamps Action::Publish
// regardless of entry_flags). Body layout matches EncodePublish's:
// stream_id(4) + subject_len(2) + msg_id_len(2) + subject + msg_id + extPayload.
func EncodePublishWithHeaders(seq uint64, streamID uint32, subject, msgID, extPayload []byte, flags byte) []byte {
	subjLen := len(subject)
	msgIDLen := len(msgID)
	bodyLen := 8 + subjLen + msgIDLen + len(extPayload)

	frame := make([]byte, HeaderSize+bodyLen)
	EncodeHeader(frame, Header{
		Action:     ActionPublish,
		Flags:      flags,
		EntryFlags: EntryFlagHasHeaders,
		MsgLen:     uint32(bodyLen),
		Seq:        seq,
	})

	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], streamID)
	binary.LittleEndian.PutUint16(body[4:6], uint16(subjLen))
	binary.LittleEndian.PutUint16(body[6:8], uint16(msgIDLen))
	off := 8
	copy(body[off:], subject)
	off += subjLen
	copy(body[off:], msgID)
	off += msgIDLen
	copy(body[off:], extPayload)

	return frame
}
