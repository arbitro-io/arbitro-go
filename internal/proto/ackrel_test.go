package proto

import "testing"

func TestEncodeAckStateReqWireSize(t *testing.T) {
	frame := EncodeAckStateReq(1, 42, 7)
	if len(frame) != 24 {
		t.Fatalf("wire size = %d, want 24", len(frame))
	}
	hdr := DecodeHeader(frame)
	if hdr.Action != ActionAckStateReq {
		t.Fatalf("action = %#x, want %#x", hdr.Action, ActionAckStateReq)
	}
	body := frame[HeaderSize:]
	if got := decodeU32(body[0:4]); got != 42 {
		t.Fatalf("consumer_id = %d, want 42", got)
	}
	if got := decodeU32(body[4:8]); got != 7 {
		t.Fatalf("generation = %d, want 7", got)
	}
}

func TestEncodeAckBatchRoundtrip(t *testing.T) {
	seqs := []uint64{100, 101, 102}
	frame := EncodeAckBatch(7, 77, 3, 0, seqs)
	wantSize := HeaderSize + 16 + len(seqs)*8
	if len(frame) != wantSize {
		t.Fatalf("wire size = %d, want %d", len(frame), wantSize)
	}
	hdr := DecodeHeader(frame)
	if hdr.Action != ActionAckBatch {
		t.Fatalf("action = %#x, want %#x", hdr.Action, ActionAckBatch)
	}
	body := frame[HeaderSize:]
	if got := decodeU32(body[0:4]); got != 77 {
		t.Fatalf("consumer_id = %d, want 77", got)
	}
	if got := decodeU32(body[4:8]); got != 3 {
		t.Fatalf("generation = %d, want 3", got)
	}
	if got := decodeU32(body[12:16]); got != uint32(len(seqs)) {
		t.Fatalf("seq_count = %d, want %d", got, len(seqs))
	}
}

func TestDecodeAckStateRep(t *testing.T) {
	body := make([]byte, 40)
	putU32(body[0:4], 42)
	putU32(body[4:8], 3)
	putU64(body[8:16], 1000)
	putU64(body[16:24], 500)
	putU64(body[24:32], 2000)
	putU32(body[32:36], 0)

	cid, gen, cursor, low, high, status, err := DecodeAckStateRep(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cid != 42 || gen != 3 || cursor != 1000 || low != 500 || high != 2000 || status != 0 {
		t.Fatalf("got (%d,%d,%d,%d,%d,%d)", cid, gen, cursor, low, high, status)
	}
}

func TestDecodeAckStateRepTooShort(t *testing.T) {
	if _, _, _, _, _, _, err := DecodeAckStateRep(make([]byte, 10)); err == nil {
		t.Fatal("expected error for short body")
	}
}

func TestDecodeAckBatchResp(t *testing.T) {
	body := make([]byte, 32)
	putU32(body[0:4], 77)
	putU64(body[4:12], 3000)
	putU32(body[12:16], 10)
	putU32(body[16:20], 2)
	putU32(body[20:24], 1)
	putU32(body[24:28], 0)
	putU32(body[28:32], 0)

	cid, cursor, accepted, ignored, belowRet, stillPending, status, err := DecodeAckBatchResp(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cid != 77 || cursor != 3000 || accepted != 10 || ignored != 2 || belowRet != 1 || stillPending != 0 || status != 0 {
		t.Fatalf("got (%d,%d,%d,%d,%d,%d,%d)", cid, cursor, accepted, ignored, belowRet, stillPending, status)
	}
}

func TestDecodeAckBatchRespTooShort(t *testing.T) {
	if _, _, _, _, _, _, _, err := DecodeAckBatchResp(make([]byte, 10)); err == nil {
		t.Fatal("expected error for short body")
	}
}

// --- small helpers local to this test file ---

func decodeU32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func putU32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func putU64(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
}
