package proto

import (
	"encoding/binary"
	"errors"
)

// Cron fire/ack frames use a fixed BINARY layout (not JSON), per
// arbitro-proto wire::cron.rs. CreateCron/DeleteCron/ListCrons remain
// cold-path JSON (see manage.go), but CronFire (broker → client) and
// CronAck (client → broker) are latency-sensitive and use a minimal
// fixed-size binary encoding instead.

// cronFireFixed is the fixed portion of a CronFire body:
// [2 name_len LE][8 fire_time_ms LE][8 fire_count LE] = 18 bytes,
// followed by the variable-length name. Mirrors CRON_FIRE_FIXED in
// arbitro-proto wire/cron.rs.
const cronFireFixed = 2 + 8 + 8

// cronAckFixed is the fixed portion of a CronAck body:
// [2 name_len LE][1 status] = 3 bytes, followed by the variable-length
// name. Mirrors CRON_ACK_FIXED in arbitro-proto wire/cron.rs.
const cronAckFixed = 2 + 1

// DecodeCronFireBody parses a CronFire body (after the frame header has
// already been stripped by the caller):
//
//	[2 name_len LE][8 fire_time_ms LE][8 fire_count LE][name...]
//
// Mirrors arbitro-proto wire::cron::decode_cron_fire.
func DecodeCronFireBody(body []byte) (name string, fireTimeMs uint64, fireCount uint64, err error) {
	if len(body) < cronFireFixed {
		return "", 0, 0, errors.New("cron fire: body too short")
	}
	nameLen := int(binary.LittleEndian.Uint16(body[0:2]))
	fireTimeMs = binary.LittleEndian.Uint64(body[2:10])
	fireCount = binary.LittleEndian.Uint64(body[10:18])
	if len(body) < cronFireFixed+nameLen {
		return "", 0, 0, errors.New("cron fire: truncated name")
	}
	name = string(body[cronFireFixed : cronFireFixed+nameLen])
	return name, fireTimeMs, fireCount, nil
}

// EncodeCronAck builds a CronAck frame (binary body, client → broker).
// Layout: [2 name_len LE][1 status (0=ok, 1=error)][name...].
// Mirrors arbitro-proto wire::cron::encode_cron_ack.
func EncodeCronAck(seq uint64, name []byte, ok bool) ([]byte, error) {
	bodyLen := cronAckFixed + len(name)
	frame := make([]byte, HeaderSize+bodyLen)
	EncodeHeader(frame, Header{
		Action: ActionCronAck,
		Flags:  FlagNone,
		MsgLen: uint32(bodyLen),
		Seq:    seq,
	})
	body := frame[HeaderSize:]
	binary.LittleEndian.PutUint16(body[0:2], uint16(len(name)))
	if ok {
		body[2] = 0
	} else {
		body[2] = 1
	}
	copy(body[cronAckFixed:], name)
	return frame, nil
}
