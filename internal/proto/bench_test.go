package proto

import "testing"

func BenchmarkEncodeHeader(b *testing.B) {
	buf := make([]byte, HeaderSize)
	h := Header{Action: ActionPublish, Flags: FlagAckReq, MsgLen: 1234, Seq: 99}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeHeader(buf, h)
	}
}

func BenchmarkDecodeHeader(b *testing.B) {
	buf := make([]byte, HeaderSize)
	EncodeHeader(buf, Header{Action: ActionPublish, Flags: FlagAckReq, MsgLen: 1234, Seq: 99})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecodeHeader(buf)
	}
}

func BenchmarkEncodePublish128(b *testing.B) {
	subject := []byte("orders.created")
	msgID := []byte("dedup-key-123")
	payload := make([]byte, 128)
	b.SetBytes(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodePublish(uint64(i), 1, subject, msgID, payload, FlagAckReq)
	}
}

func BenchmarkEncodePublish1K(b *testing.B) {
	subject := []byte("orders.created")
	msgID := []byte("")
	payload := make([]byte, 1024)
	b.SetBytes(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodePublish(uint64(i), 1, subject, msgID, payload, FlagAckReq)
	}
}

func BenchmarkEncodeAck(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeAck(uint64(i), 5, 0xABCD1234, 777)
	}
}

func BenchmarkEncodeBatchAck64(b *testing.B) {
	entries := make([]AckEntry, 64)
	for i := range entries {
		entries[i] = AckEntry{Seq: uint64(i), SubjectHash: uint32(i * 37)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeBatchAck(uint64(i), 1, entries)
	}
}

func BenchmarkDecodeDeliverHeader(b *testing.B) {
	body := make([]byte, 64)
	body[0] = 3
	body[4] = 42
	body[8] = 12
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecodeDeliverHeader(body)
	}
}

func BenchmarkEncodePublishBatch10(b *testing.B) {
	entries := make([]BatchEntry, 10)
	for i := range entries {
		entries[i] = BatchEntry{
			Subject: []byte("orders.created"),
			MsgID:   []byte(""),
			Payload: make([]byte, 128),
		}
	}
	b.SetBytes(128 * 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodePublishBatch(uint64(i), 1, entries, FlagAckReq)
	}
}
