package arbitro

import (
	"strings"
	"testing"
)

func TestArbitroErrorWithCode(t *testing.T) {
	err := &ArbitroError{Code: ErrCodeStreamAlreadyExists}
	msg := err.Error()
	if !strings.Contains(msg, "0x0011") {
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
		ErrCodeStreamNotFound,
		ErrCodeStreamAlreadyExists,
		ErrCodeStreamFull,
		ErrCodeStreamFilterOverlap,
		ErrCodeSubjectNotFound,
		ErrCodeIdempotencyDuplicate,
		ErrCodeConsumerNotFound,
		ErrCodeConsumerAlreadyExists,
		ErrCodeConsumerFilterOverlap,
		ErrCodeInvalidSequence,
		ErrCodeMaxInflightReached,
		ErrCodeAckTimeout,
		ErrCodeAuthRequired,
		ErrCodeAuthFailed,
		ErrCodeServerShuttingDown,
		ErrCodeInternalError,
	}
	for _, code := range codes {
		if _, ok := codeMessages[code]; !ok {
			t.Errorf("code 0x%04X has no message in codeMessages map", code)
		}
	}
}
