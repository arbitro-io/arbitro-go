package proto

import "encoding/binary"

// HeaderSize is the fixed size of every V2 frame header.
const HeaderSize = 16

// Header represents a decoded V2 frame header.
type Header struct {
	Action     uint16
	Flags      byte
	EntryFlags byte
	MsgLen     uint32
	Seq        uint64
}

// EncodeHeader writes a 16-byte frame header into dst.
// dst must be at least HeaderSize bytes.
func EncodeHeader(dst []byte, h Header) {
	binary.LittleEndian.PutUint16(dst[0:2], h.Action)
	dst[2] = h.Flags
	dst[3] = h.EntryFlags
	binary.LittleEndian.PutUint32(dst[4:8], h.MsgLen)
	binary.LittleEndian.PutUint64(dst[8:16], h.Seq)
}

// DecodeHeader reads a 16-byte frame header from src.
// src must be at least HeaderSize bytes.
func DecodeHeader(src []byte) Header {
	return Header{
		Action:     binary.LittleEndian.Uint16(src[0:2]),
		Flags:      src[2],
		EntryFlags: src[3],
		MsgLen:     binary.LittleEndian.Uint32(src[4:8]),
		Seq:        binary.LittleEndian.Uint64(src[8:16]),
	}
}
