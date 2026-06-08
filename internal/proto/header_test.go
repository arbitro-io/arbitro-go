package proto

import (
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{
		Action:     ActionPublish,
		Flags:      FlagAckReq,
		EntryFlags: EntryFlagRetain,
		MsgLen:     1234,
		Seq:        987654321,
	}

	buf := make([]byte, HeaderSize)
	EncodeHeader(buf, h)
	got := DecodeHeader(buf)

	if got.Action != h.Action {
		t.Errorf("Action: got %d, want %d", got.Action, h.Action)
	}
	if got.Flags != h.Flags {
		t.Errorf("Flags: got %d, want %d", got.Flags, h.Flags)
	}
	if got.EntryFlags != h.EntryFlags {
		t.Errorf("EntryFlags: got %d, want %d", got.EntryFlags, h.EntryFlags)
	}
	if got.MsgLen != h.MsgLen {
		t.Errorf("MsgLen: got %d, want %d", got.MsgLen, h.MsgLen)
	}
	if got.Seq != h.Seq {
		t.Errorf("Seq: got %d, want %d", got.Seq, h.Seq)
	}
}

func TestHelloEncoding(t *testing.T) {
	buf := make([]byte, HelloSize)
	EncodeHello(buf, CapReply)

	// Check magic "ARB2" (LE: 0x41, 0x52, 0x42, 0x32)
	if buf[0] != 0x41 || buf[1] != 0x52 || buf[2] != 0x42 || buf[3] != 0x32 {
		t.Errorf("magic bytes: got %x %x %x %x", buf[0], buf[1], buf[2], buf[3])
	}
	if buf[4] != 2 {
		t.Errorf("version: got %d, want 2", buf[4])
	}
	if buf[5] != RoleClient {
		t.Errorf("role: got %d, want %d", buf[5], RoleClient)
	}
	// caps = 0x0002 (CapReply) in LE
	if buf[6] != 0x02 || buf[7] != 0x00 {
		t.Errorf("caps: got %x %x, want 02 00", buf[6], buf[7])
	}
}

func TestPublishEncoding(t *testing.T) {
	frame := EncodePublish(42, 7, []byte("orders.new"), []byte("dedup-1"), []byte("hello"), FlagAckReq)

	// Verify header
	hdr := DecodeHeader(frame)
	if hdr.Action != ActionPublish {
		t.Errorf("action: got 0x%04X, want 0x%04X", hdr.Action, ActionPublish)
	}
	if hdr.Seq != 42 {
		t.Errorf("seq: got %d, want 42", hdr.Seq)
	}
	if hdr.Flags != FlagAckReq {
		t.Errorf("flags: got %d, want %d", hdr.Flags, FlagAckReq)
	}

	// body = 8 + 10 + 7 + 5 = 30
	if hdr.MsgLen != 30 {
		t.Errorf("msg_len: got %d, want 30", hdr.MsgLen)
	}

	// Verify body
	body := frame[HeaderSize:]
	// stream_id at offset 0
	streamID := uint32(body[0]) | uint32(body[1])<<8 | uint32(body[2])<<16 | uint32(body[3])<<24
	if streamID != 7 {
		t.Errorf("stream_id: got %d, want 7", streamID)
	}
	// subject_len at offset 4
	subjLen := uint16(body[4]) | uint16(body[5])<<8
	if subjLen != 10 {
		t.Errorf("subject_len: got %d, want 10", subjLen)
	}
	// msg_id_len at offset 6
	msgIDLen := uint16(body[6]) | uint16(body[7])<<8
	if msgIDLen != 7 {
		t.Errorf("msg_id_len: got %d, want 7", msgIDLen)
	}
	// subject at offset 8
	subj := string(body[8 : 8+10])
	if subj != "orders.new" {
		t.Errorf("subject: got %q, want %q", subj, "orders.new")
	}
	// msg_id at offset 18
	msgID := string(body[18 : 18+7])
	if msgID != "dedup-1" {
		t.Errorf("msg_id: got %q, want %q", msgID, "dedup-1")
	}
	// payload at offset 25
	payload := string(body[25:30])
	if payload != "hello" {
		t.Errorf("payload: got %q, want %q", payload, "hello")
	}
}

func TestAckEncoding(t *testing.T) {
	frame := EncodeAck(99, 5, 0xABCD1234, 777)

	if len(frame) != 32 {
		t.Fatalf("frame size: got %d, want 32", len(frame))
	}

	hdr := DecodeHeader(frame)
	if hdr.Action != ActionAck {
		t.Errorf("action: got 0x%04X, want 0x%04X", hdr.Action, ActionAck)
	}
	if hdr.Seq != 99 {
		t.Errorf("seq: got %d, want 99", hdr.Seq)
	}
	if hdr.MsgLen != 16 {
		t.Errorf("msg_len: got %d, want 16", hdr.MsgLen)
	}
}

func TestDeliverDecoding(t *testing.T) {
	// Simulate a Deliver body: consumer_id(4) + subject_hash(4) + subject_len(2) + pad(2) + subject + payload
	body := make([]byte, 12+5+6) // 12 header + "hello" subject + "world!" payload
	body[0] = 3  // consumer_id = 3
	body[4] = 42 // subject_hash = 42
	body[8] = 5  // subject_len = 5
	copy(body[12:], "hello")
	copy(body[17:], "world!")

	dh := DecodeDeliverHeader(body)
	if dh.ConsumerID != 3 {
		t.Errorf("consumer_id: got %d, want 3", dh.ConsumerID)
	}
	if dh.SubjectHash != 42 {
		t.Errorf("subject_hash: got %d, want 42", dh.SubjectHash)
	}
	if dh.SubjectLen != 5 {
		t.Errorf("subject_len: got %d, want 5", dh.SubjectLen)
	}

	subj := DeliverSubject(body, dh.SubjectLen)
	if string(subj) != "hello" {
		t.Errorf("subject: got %q, want %q", subj, "hello")
	}

	payload := DeliverPayload(body, dh.SubjectLen)
	if string(payload) != "world!" {
		t.Errorf("payload: got %q, want %q", payload, "world!")
	}
}
