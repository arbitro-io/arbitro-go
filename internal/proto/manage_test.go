package proto

import (
	"encoding/json"
	"testing"
)

func TestEncodeConsumerStats(t *testing.T) {
	frame, err := EncodeConsumerStats(3, 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hdr := DecodeHeader(frame)
	if hdr.Action != ActionConsumerStats {
		t.Fatalf("action = %#x, want %#x", hdr.Action, ActionConsumerStats)
	}
	if hdr.Flags != FlagAckReq {
		t.Fatalf("flags = %#x, want FlagAckReq (cold path)", hdr.Flags)
	}

	var payload consumerStatsPayload
	if err := json.Unmarshal(frame[HeaderSize:], &payload); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if payload.ConsumerID != 42 {
		t.Fatalf("consumer_id = %d, want 42", payload.ConsumerID)
	}
}

func TestEncodeCreateConsumerNilSubjectLimitsMarshalsAsEmptyArray(t *testing.T) {
	// Regression guard: a nil subjectLimits must NOT marshal to JSON `null`
	// (the broker's non-Option Vec<SubjectLimit> field rejects null and
	// replies with a generic 0x0502 InternalError). See EncodeCreateConsumer's
	// nil-normalization comment.
	frame, err := EncodeCreateConsumer(1, 9, []byte("name"), nil, []byte("subj.>"), 10, 1, 0, 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(frame[HeaderSize:], &raw); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	sl, ok := raw["subject_limits"]
	if !ok {
		t.Fatal("subject_limits field missing")
	}
	if string(sl) != "[]" {
		t.Fatalf("subject_limits = %s, want [] (not null)", sl)
	}
}
