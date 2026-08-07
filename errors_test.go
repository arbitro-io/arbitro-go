package arbitro

import (
	"fmt"
	"strings"
	"testing"
)

func TestArbitroErrorWithCode(t *testing.T) {
	err := &ArbitroError{Code: ErrCodeStreamAlreadyExists}
	msg := err.Error()
	if !strings.Contains(msg, "0x0202") {
		t.Errorf("expected code in message, got: %s", msg)
	}
	if !strings.Contains(msg, "stream already exists") {
		t.Errorf("expected human message, got: %s", msg)
	}
}

func TestArbitroErrorWithCustomMessage(t *testing.T) {
	err := &ArbitroError{Code: ErrCodeInternalError, Message: "reply body too short"}
	msg := err.Error()
	if !strings.Contains(msg, "reply body too short") {
		t.Errorf("expected custom message, got: %s", msg)
	}
	// Custom message takes priority over code lookup
	if strings.Contains(msg, "internal server error") {
		t.Errorf("custom message should override code lookup, got: %s", msg)
	}
}

func TestArbitroErrorUnknownCode(t *testing.T) {
	err := &ArbitroError{Code: 0xFFFF}
	msg := err.Error()
	if !strings.Contains(msg, "0xFFFF") {
		t.Errorf("expected hex code, got: %s", msg)
	}
}

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		code uint16
		want bool
	}{
		{ErrCodeStreamNotFound, true},
		{ErrCodeConsumerNotFound, true},
		{ErrCodeStreamAlreadyExists, false},
		{ErrCodeInternalError, false},
	}
	for _, tc := range tests {
		err := &ArbitroError{Code: tc.code}
		if got := IsNotFound(err); got != tc.want {
			t.Errorf("IsNotFound(0x%04X) = %v, want %v", tc.code, got, tc.want)
		}
	}
}

func TestIsAlreadyExists(t *testing.T) {
	tests := []struct {
		code uint16
		want bool
	}{
		{ErrCodeStreamAlreadyExists, true},
		{ErrCodeConsumerAlreadyExists, true},
		{ErrCodeStreamNotFound, false},
		{ErrCodeInternalError, false},
	}
	for _, tc := range tests {
		err := &ArbitroError{Code: tc.code}
		if got := IsAlreadyExists(err); got != tc.want {
			t.Errorf("IsAlreadyExists(0x%04X) = %v, want %v", tc.code, got, tc.want)
		}
	}
}

func TestIsDuplicate(t *testing.T) {
	dup := &ArbitroError{Code: ErrCodeIdempotencyDuplicate}
	if !IsDuplicate(dup) {
		t.Error("IsDuplicate should be true for idempotency duplicate")
	}
	other := &ArbitroError{Code: ErrCodeStreamFull}
	if IsDuplicate(other) {
		t.Error("IsDuplicate should be false for stream full")
	}
}

func TestErrorHelpersWithNilAndNonArbitroError(t *testing.T) {
	if IsNotFound(nil) {
		t.Error("IsNotFound(nil) should be false")
	}
	if IsAlreadyExists(nil) {
		t.Error("IsAlreadyExists(nil) should be false")
	}
	if IsDuplicate(nil) {
		t.Error("IsDuplicate(nil) should be false")
	}

	// Non-ArbitroError
	generic := &struct{ error }{nil}
	if IsNotFound(generic) {
		t.Error("IsNotFound(generic) should be false")
	}
}

func TestAllCodesHaveMessages(t *testing.T) {
	codes := []uint16{
		ErrCodeUnknownAction,
		ErrCodeBufferTooShort,
		ErrCodeInvalidLength,
		ErrCodeInvalidEntryCount,
		ErrCodeAuthRequired,
		ErrCodeAuthFailed,
		ErrCodeStreamNotFound,
		ErrCodeStreamAlreadyExists,
		ErrCodeStreamFull,
		ErrCodeIdempotencyDuplicate,
		ErrCodeConsumerNotFound,
		ErrCodeConsumerAlreadyExists,
		ErrCodeInvalidConsumerConfig,
		ErrCodeServerShuttingDown,
		ErrCodeInternalError,
		ErrCodeUnimplemented,
		ErrCodeTimeout,
		ErrCodeInvalidConfig,
	}
	for _, code := range codes {
		if _, ok := codeMessages[code]; !ok {
			t.Errorf("code 0x%04X has no message in codeMessages map", code)
		}
	}
}

// The predicates exist to classify what the BROKER sent, so the constants
// they compare against have to be the values the broker actually puts on the
// wire. Pinning the numbers here rather than the names means a future edit to
// errors.go that drifts from arbitro-proto fails this test instead of quietly
// turning IsNotFound into a function that always returns false.
func TestWireCodesMatchProtocol(t *testing.T) {
	want := map[string]uint16{
		"StreamNotFound":        0x0201,
		"StreamAlreadyExists":   0x0202,
		"StreamFull":            0x0203,
		"IdempotencyDuplicate":  0x0206,
		"ConsumerNotFound":      0x0301,
		"ConsumerAlreadyExists": 0x0302,
		"InvalidConsumerConfig": 0x0304,
		"ServerShuttingDown":    0x0501,
		"InternalError":         0x0502,
		"Unimplemented":         0x0503,
	}
	got := map[string]uint16{
		"StreamNotFound":        ErrCodeStreamNotFound,
		"StreamAlreadyExists":   ErrCodeStreamAlreadyExists,
		"StreamFull":            ErrCodeStreamFull,
		"IdempotencyDuplicate":  ErrCodeIdempotencyDuplicate,
		"ConsumerNotFound":      ErrCodeConsumerNotFound,
		"ConsumerAlreadyExists": ErrCodeConsumerAlreadyExists,
		"InvalidConsumerConfig": ErrCodeInvalidConsumerConfig,
		"ServerShuttingDown":    ErrCodeServerShuttingDown,
		"InternalError":         ErrCodeInternalError,
		"Unimplemented":         ErrCodeUnimplemented,
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = 0x%04X, protocol says 0x%04X", name, got[name], w)
		}
	}
}

// A wrapped error still has to classify. Callers wrap with %w all the time,
// and a plain type assertion would report false for an error that is plainly
// a not-found.
func TestPredicatesSeeThroughWrapping(t *testing.T) {
	wrapped := fmt.Errorf("resolving stream: %w", &ArbitroError{Code: ErrCodeStreamNotFound})
	if !IsNotFound(wrapped) {
		t.Error("IsNotFound should unwrap %w-wrapped errors")
	}
	dup := fmt.Errorf("publish: %w", &ArbitroError{Code: ErrCodeIdempotencyDuplicate})
	if !IsDuplicate(dup) {
		t.Error("IsDuplicate should unwrap %w-wrapped errors")
	}
}
