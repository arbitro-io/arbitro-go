package proto

import "encoding/binary"

// DeliverHeader contains the parsed fields from a Deliver frame body.
//
// `SubID` is the subscription id the CLIENT chose and sent on Subscribe.
// The broker stamps it on every delivery and expects it back on the ack,
// which is how the ack reaches its pending in one hash instead of a scan
// over the consumer's bindings. The field used to carry a subject hash;
// only the name lagged behind.
type DeliverHeader struct {
	ConsumerID uint32
	SubID      uint32
	SubjectLen uint16
}

// DeliverBodyOffset is the fixed prefix of a Deliver body before the subject.
const DeliverBodyOffset = 12

// DecodeDeliverHeader parses the first 12 bytes of a Deliver body.
func DecodeDeliverHeader(body []byte) DeliverHeader {
	return DeliverHeader{
		ConsumerID: binary.LittleEndian.Uint32(body[0:4]),
		SubID:      binary.LittleEndian.Uint32(body[4:8]),
		SubjectLen: binary.LittleEndian.Uint16(body[8:10]),
		// body[10:12] is reserved padding
	}
}

// DeliverSubject returns the subject slice from a Deliver body.
func DeliverSubject(body []byte, subjLen uint16) []byte {
	return body[DeliverBodyOffset : DeliverBodyOffset+int(subjLen)]
}

// DeliverPayload returns the payload slice from a Deliver body.
func DeliverPayload(body []byte, subjLen uint16) []byte {
	return body[DeliverBodyOffset+int(subjLen):]
}

// RepOkRefSeq extracts the ref_seq from a RepOk body (8 bytes).
func RepOkRefSeq(body []byte) uint64 {
	return binary.LittleEndian.Uint64(body[0:8])
}

// RepErrorCode extracts the error_code from a RepError body.
// Body layout: ref_seq(8) + error_code(2) + pad(6)
func RepErrorCode(body []byte) uint16 {
	return binary.LittleEndian.Uint16(body[8:10])
}
