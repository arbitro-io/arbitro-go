package arbitro

import "fmt"

// Error codes from the broker (wire-level uint16).
const (
	ErrCodeUnknownAction      uint16 = 0x0001
	ErrCodeBufferTooShort     uint16 = 0x0002
	ErrCodeInvalidLength      uint16 = 0x0003
	ErrCodeStreamNotFound     uint16 = 0x0010
	ErrCodeStreamAlreadyExists uint16 = 0x0011
	ErrCodeStreamFull         uint16 = 0x0012
	ErrCodeStreamFilterOverlap uint16 = 0x0013
	ErrCodeSubjectNotFound    uint16 = 0x0014
	ErrCodeIdempotencyDuplicate uint16 = 0x0015
	ErrCodeConsumerNotFound     uint16 = 0x0020
	ErrCodeConsumerAlreadyExists uint16 = 0x0021
	ErrCodeConsumerFilterOverlap uint16 = 0x0022
	ErrCodeInvalidSequence    uint16 = 0x0030
	ErrCodeMaxInflightReached uint16 = 0x0031
	ErrCodeAckTimeout         uint16 = 0x0032
	ErrCodeAuthRequired       uint16 = 0x0040
	ErrCodeAuthFailed         uint16 = 0x0041
	ErrCodeServerShuttingDown uint16 = 0x0050
	ErrCodeInternalError      uint16 = 0x0051
)

// ArbitroError represents an error from the broker or client.
type ArbitroError struct {
	Code    uint16
	Message string
}

func (e *ArbitroError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("arbitro: [0x%04X] %s", e.Code, e.Message)
	}
	return fmt.Sprintf("arbitro: error code 0x%04X", e.Code)
}

// IsNotFound returns true if the error is a stream/consumer not found.
func IsNotFound(err error) bool {
	if ae, ok := err.(*ArbitroError); ok {
		return ae.Code == ErrCodeStreamNotFound || ae.Code == ErrCodeConsumerNotFound
	}
	return false
}

// IsAlreadyExists returns true if the entity already exists.
func IsAlreadyExists(err error) bool {
	if ae, ok := err.(*ArbitroError); ok {
		return ae.Code == ErrCodeStreamAlreadyExists || ae.Code == ErrCodeConsumerAlreadyExists
	}
	return false
}

// IsDuplicate returns true if the publish was rejected as an idempotency duplicate.
func IsDuplicate(err error) bool {
	if ae, ok := err.(*ArbitroError); ok {
		return ae.Code == ErrCodeIdempotencyDuplicate
	}
	return false
}
