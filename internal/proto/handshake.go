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
// Format: magic(4) + version(1) + role(1) + caps(2 LE)
func EncodeHello(dst []byte, caps uint16) {
	binary.LittleEndian.PutUint32(dst[0:4], HelloMagic)
	dst[4] = 2 // version
	dst[5] = RoleClient
	binary.LittleEndian.PutUint16(dst[6:8], caps)
}

// DefaultCaps returns the standard client capabilities (Reply support).
func DefaultCaps() uint16 {
	return CapReply
}
