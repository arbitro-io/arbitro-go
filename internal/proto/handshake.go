package proto

import "encoding/binary"

// HelloSize is the fixed size of the handshake frame (not a standard frame).
const HelloSize = 8

// Magic bytes: "ARB2" as uint32 LE = 0x32425241
const HelloMagic uint32 = 0x32425241

const (
	RoleClient byte = 0
	RoleServer byte = 1
)

// EncodeHello writes the 8-byte handshake into dst.
// Format (see arbitro-proto v2::ingress::hello::HelloFrame):
//   magic(4) + version(1) + role(1) + _pad(2 LE, reserved — must be 0)
// M9 removed the caps field; the trailing u16 is a reserved pad the client
// MUST write as 0 and the server ignores. Any future negotiation lives in a
// dedicated HelloAck frame.
func EncodeHello(dst []byte, _pad uint16) {
	binary.LittleEndian.PutUint32(dst[0:4], HelloMagic)
	dst[4] = 2 // version
	dst[5] = RoleClient
	binary.LittleEndian.PutUint16(dst[6:8], 0)
}

// DefaultCaps returns the value written into the Hello frame's reserved
// trailing u16. M9 removed capability bits from Hello — the field is now a
// pad that must be 0.
func DefaultCaps() uint16 {
	return 0
}
