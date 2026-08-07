package arbitro

import (
	"errors"
	"fmt"
)

// Error codes from the broker (wire-level uint16).
//
// These MUST track arbitro-proto's `ErrorCode` enum (crates/arbitro-proto/
// src/error.rs) byte for byte — they are compared against the code the
// broker puts on the wire, so a stale value silently turns every predicate
// below into `false`. The high byte is the family: 0x00 protocol, 0x01 auth,
// 0x02 stream, 0x03 consumer, 0x05 system.
const (
	// 0x00xx — protocol (the frame itself arrived malformed)
	ErrCodeUnknownAction     uint16 = 0x0001
	ErrCodeBufferTooShort    uint16 = 0x0002
	ErrCodeInvalidLength     uint16 = 0x0003
	ErrCodeInvalidEntryCount uint16 = 0x0004

	// 0x01xx — auth
	ErrCodeAuthRequired uint16 = 0x0101
	ErrCodeAuthFailed   uint16 = 0x0102

	// 0x02xx — stream
	ErrCodeStreamNotFound      uint16 = 0x0201
	ErrCodeStreamAlreadyExists uint16 = 0x0202
	ErrCodeStreamFull          uint16 = 0x0203
	// The publish carried a msg_id the broker has already stored for this
	// stream inside idempotency_window_ms. The message was NOT stored, and
	// the original write is what survives — safe to treat as success.
	ErrCodeIdempotencyDuplicate uint16 = 0x0206

	// 0x03xx — consumer
	ErrCodeConsumerNotFound      uint16 = 0x0301
	ErrCodeConsumerAlreadyExists uint16 = 0x0302
	// The CreateConsumer request carried an unusable configuration (most
	// often an empty group, which is mandatory). The consumer was NOT created.
	ErrCodeInvalidConsumerConfig uint16 = 0x0304

	// 0x05xx — system
	ErrCodeServerShuttingDown uint16 = 0x0501
	ErrCodeInternalError      uint16 = 0x0502
	// The broker recognised the action but has no handler wired in this
	// build — distinct from "I don't know this code" (ErrCodeUnknownAction).
	ErrCodeUnimplemented uint16 = 0x0503

	// Client-side only — never seen on the wire. Kept outside every broker
	// family so they can never collide with a future server code.
	ErrCodeTimeout       uint16 = 0xFF01 // request deadline elapsed locally
	ErrCodeInvalidConfig uint16 = 0xFF02 // ConsumerConfig.Validate() failure
)

// codeMessages maps error codes to human-readable descriptions.
var codeMessages = map[uint16]string{
	ErrCodeUnknownAction:         "unknown action",
	ErrCodeBufferTooShort:        "buffer too short",
	ErrCodeInvalidLength:         "invalid frame length",
	ErrCodeInvalidEntryCount:     "invalid entry count",
	ErrCodeAuthRequired:          "authentication required",
	ErrCodeAuthFailed:            "authentication failed",
	ErrCodeStreamNotFound:        "stream not found",
	ErrCodeStreamAlreadyExists:   "stream already exists",
	ErrCodeStreamFull:            "stream is full (max messages or bytes reached)",
	ErrCodeIdempotencyDuplicate:  "duplicate message (idempotency window)",
	ErrCodeConsumerNotFound:      "consumer not found",
	ErrCodeConsumerAlreadyExists: "consumer already exists",
	ErrCodeInvalidConsumerConfig: "invalid consumer config",
	ErrCodeServerShuttingDown:    "server shutting down",
	ErrCodeInternalError:         "internal server error",
	ErrCodeUnimplemented:         "action not implemented in this build",
	ErrCodeTimeout:               "request timed out",
	ErrCodeInvalidConfig:         "invalid client config",
}

// ArbitroError represents an error from the broker or client.
type ArbitroError struct {
	Code    uint16
	Message string
}

func (e *ArbitroError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("arbitro: [0x%04X] %s", e.Code, e.Message)
	}
	if msg, ok := codeMessages[e.Code]; ok {
		return fmt.Sprintf("arbitro: [0x%04X] %s", e.Code, msg)
	}
	return fmt.Sprintf("arbitro: error code 0x%04X", e.Code)
}

// code extracts the wire code from err, unwrapping anything wrapped with
// %w along the way. A direct type assertion would miss a wrapped error and
// report the wrong answer, which for these predicates means silently
// classifying a known broker reply as "not this one".
func code(err error) (uint16, bool) {
	var ae *ArbitroError
	if errors.As(err, &ae) {
		return ae.Code, true
	}
	return 0, false
}

// IsNotFound returns true if the error is a stream/consumer not found.
func IsNotFound(err error) bool {
	c, ok := code(err)
	return ok && (c == ErrCodeStreamNotFound || c == ErrCodeConsumerNotFound)
}

// IsAlreadyExists returns true if the entity already exists.
func IsAlreadyExists(err error) bool {
	c, ok := code(err)
	return ok && (c == ErrCodeStreamAlreadyExists || c == ErrCodeConsumerAlreadyExists)
}

// IsDuplicate returns true if the publish was rejected as an idempotency
// duplicate. The original write is stored, so callers retrying a publish can
// treat this as success.
func IsDuplicate(err error) bool {
	c, ok := code(err)
	return ok && c == ErrCodeIdempotencyDuplicate
}
