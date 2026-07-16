package arbitro

import "unsafe"

// unsafeStringBytes returns a []byte header pointing at the same backing
// memory as s. The caller MUST treat the result as read-only; writing to it
// mutates the string, which violates the language contract and may crash
// (Go 1.20+ places string data in read-only pages when it can).
//
// Used on the publish hot path so that `subject string` passed by callers
// can be encoded without a []byte(subject) copy (~30-80ns saved per publish
// with typical subject lengths). Safe because proto.EncodePublishInto only
// reads from the slice and never retains it past the call.
func unsafeStringBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
