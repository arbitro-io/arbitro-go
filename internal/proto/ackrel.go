package proto

import (
	"encoding/binary"
	"errors"
)

// Ack-reliability wire codecs (G01). Mirrors arbitro-proto v2::ingress::ack_state
// (AckStateReqFrame, AckBatchFrame) and v2::egress::ack_state (AckStateRepFrame,
// AckBatchRespFrame). All multi-byte fields are little-endian.

// AckBatchMaxSeqs mirrors arbitro-proto's ACK_BATCH_MAX_SEQS — the largest
// number of seqs allowed in a single AckBatch frame.
const AckBatchMaxSeqs = 1000

// Ack-reliability status codes, shared between AckStateRep and AckBatchResp.
const (
	AckStatusOK                 uint8 = 0
	AckStatusBatchTooLarge      uint8 = 1
	AckStatusGenerationMismatch uint8 = 2
	AckStatusConsumerUnknown    uint8 = 3
)

// EncodeAckStateReq builds an AckStateReq frame (header 16 + body 8).
// Body layout: consumer_id(4) + generation(4).
func EncodeAckStateReq(seq uint64, consumerID uint32, generation uint32) []byte {
	const bodyLen = 8
	frame := make([]byte, HeaderSize+bodyLen)
	EncodeHeader(frame, Header{
		Action: ActionAckStateReq,
		Flags:  FlagNone,
		MsgLen: bodyLen,
		Seq:    seq,
	})
	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], consumerID)
	binary.LittleEndian.PutUint32(body[4:8], generation)
	return frame
}

// EncodeAckBatch builds an AckBatch frame.
// Body layout: consumer_id(4) + generation(4) + flags(4) + seq_count(4) + seqs(count*8).
func EncodeAckBatch(seq uint64, consumerID uint32, generation uint32, flags uint32, seqs []uint64) []byte {
	bodyLen := 16 + len(seqs)*8
	frame := make([]byte, HeaderSize+bodyLen)
	EncodeHeader(frame, Header{
		Action: ActionAckBatch,
		Flags:  FlagNone,
		MsgLen: uint32(bodyLen),
		Seq:    seq,
	})
	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint32(body[0:4], consumerID)
	binary.LittleEndian.PutUint32(body[4:8], generation)
	binary.LittleEndian.PutUint32(body[8:12], flags)
	binary.LittleEndian.PutUint32(body[12:16], uint32(len(seqs)))
	off := 16
	for _, s := range seqs {
		binary.LittleEndian.PutUint64(body[off:off+8], s)
		off += 8
	}
	return frame
}

// DecodeAckStateRep parses an AckStateRep reply body (40 bytes, fixed size):
//
//	consumer_id(4) + generation(4) + cursor(8) + low_seq(8) + high_seq(8) + status(4) + _pad(4)
func DecodeAckStateRep(body []byte) (consumerID uint32, generation uint32, cursor uint64, lowSeq uint64, highSeq uint64, status uint8, err error) {
	if len(body) < 40 {
		return 0, 0, 0, 0, 0, 0, errors.New("ack state rep: body too short")
	}
	consumerID = binary.LittleEndian.Uint32(body[0:4])
	generation = binary.LittleEndian.Uint32(body[4:8])
	cursor = binary.LittleEndian.Uint64(body[8:16])
	lowSeq = binary.LittleEndian.Uint64(body[16:24])
	highSeq = binary.LittleEndian.Uint64(body[24:32])
	status = uint8(binary.LittleEndian.Uint32(body[32:36]))
	return consumerID, generation, cursor, lowSeq, highSeq, status, nil
}

// DecodeAckBatchResp parses an AckBatchResp reply body (32 bytes, fixed size):
//
//	consumer_id(4) + new_cursor(8) + accepted(4) + ignored(4) + below_retention(4) + still_pending(4) + status(4)
func DecodeAckBatchResp(body []byte) (consumerID uint32, newCursor uint64, accepted uint32, ignored uint32, belowRetention uint32, stillPending uint32, status uint8, err error) {
	if len(body) < 32 {
		return 0, 0, 0, 0, 0, 0, 0, errors.New("ack batch resp: body too short")
	}
	consumerID = binary.LittleEndian.Uint32(body[0:4])
	newCursor = binary.LittleEndian.Uint64(body[4:12])
	accepted = binary.LittleEndian.Uint32(body[12:16])
	ignored = binary.LittleEndian.Uint32(body[16:20])
	belowRetention = binary.LittleEndian.Uint32(body[20:24])
	stillPending = binary.LittleEndian.Uint32(body[24:28])
	status = uint8(binary.LittleEndian.Uint32(body[28:32]))
	return consumerID, newCursor, accepted, ignored, belowRetention, stillPending, status, nil
}
