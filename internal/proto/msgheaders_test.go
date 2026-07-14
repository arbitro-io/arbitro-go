package proto

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncodeExtendedPayloadRoundtrip(t *testing.T) {
	payload := []byte("hello world")
	entries := []HeaderEntry{
		{Key: []byte("wf-id"), Val: []byte("order-process")},
		{Key: []byte("wf-step"), Val: []byte{2}},
		{Key: []byte(HdrMsgID), Val: []byte("abc-123")},
	}

	buf := EncodeExtendedPayload(payload, entries)

	gotPayload, gotEntries, err := DecodeExtendedPayload(buf)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload = %q, want %q", gotPayload, payload)
	}
	if len(gotEntries) != len(entries) {
		t.Fatalf("entries len = %d, want %d", len(gotEntries), len(entries))
	}
	for i, e := range entries {
		if !bytes.Equal(gotEntries[i].Key, e.Key) || !bytes.Equal(gotEntries[i].Val, e.Val) {
			t.Fatalf("entry %d = %+v, want %+v", i, gotEntries[i], e)
		}
	}
}

func TestEncodeExtendedPayloadEmptyHeaders(t *testing.T) {
	payload := []byte("data")
	buf := EncodeExtendedPayload(payload, nil)

	gotPayload, gotEntries, err := DecodeExtendedPayload(buf)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload = %q, want %q", gotPayload, payload)
	}
	if len(gotEntries) != 0 {
		t.Fatalf("entries = %v, want empty", gotEntries)
	}
	// headers_len must be present even with zero entries: 4B self-len + 2B count = 6.
	headersLenOff := 4 + len(payload)
	got := uint32(buf[headersLenOff]) | uint32(buf[headersLenOff+1])<<8 |
		uint32(buf[headersLenOff+2])<<16 | uint32(buf[headersLenOff+3])<<24
	if got != 6 {
		t.Fatalf("headers_len = %d, want 6", got)
	}
}

func TestDecodeExtendedPayloadTruncated(t *testing.T) {
	if _, _, err := DecodeExtendedPayload([]byte{1, 2}); err == nil {
		t.Fatal("expected error for truncated buffer")
	}
}

func TestEncodePublishWithHeadersWireFormat(t *testing.T) {
	extPayload := EncodeExtendedPayload([]byte("payload"), []HeaderEntry{
		{Key: []byte("k"), Val: []byte("v")},
	})
	frame := EncodePublishWithHeaders(5, 9, []byte("orders.created"), []byte("mid-1"), extPayload, FlagAckReq)

	hdr := DecodeHeader(frame)
	if hdr.Action != ActionPublish {
		t.Fatalf("action = %#x, want %#x", hdr.Action, ActionPublish)
	}
	if hdr.EntryFlags&EntryFlagHasHeaders == 0 {
		t.Fatal("EntryFlagHasHeaders not set")
	}
	if hdr.Flags != FlagAckReq {
		t.Fatalf("flags = %#x, want FlagAckReq", hdr.Flags)
	}

	body := frame[HeaderSize:]
	streamID := binary.LittleEndian.Uint32(body[0:4])
	if streamID != 9 {
		t.Fatalf("stream_id = %d, want 9", streamID)
	}
	subjLen := int(binary.LittleEndian.Uint16(body[4:6]))
	msgIDLen := int(binary.LittleEndian.Uint16(body[6:8]))
	if subjLen != len("orders.created") || msgIDLen != len("mid-1") {
		t.Fatalf("subjLen=%d msgIDLen=%d", subjLen, msgIDLen)
	}
	subj := body[8 : 8+subjLen]
	if string(subj) != "orders.created" {
		t.Fatalf("subject = %q", subj)
	}
	msgID := body[8+subjLen : 8+subjLen+msgIDLen]
	if string(msgID) != "mid-1" {
		t.Fatalf("msg_id = %q", msgID)
	}
	tail := body[8+subjLen+msgIDLen:]
	if !bytes.Equal(tail, extPayload) {
		t.Fatal("tail does not match extPayload")
	}
}
