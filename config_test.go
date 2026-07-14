package arbitro

import (
	"testing"
	"time"
)

func TestConsumerConfigValidateOK(t *testing.T) {
	cfgs := []ConsumerConfig{
		{}, // all zero values (AckNone + DeliverAll + no limits) is valid
		{AckPolicy: AckExplicit, MaxInflight: 100, AckWait: 10 * time.Second},
		{AckPolicy: AckExplicit, MaxSubjectInflights: []SubjectLimit{{Pattern: "a.*", Limit: 5}}},
		{DeliverPolicy: DeliverByStartSeq, StartSeq: 42},
		{AckPolicy: AckExplicit, DeliverPolicy: DeliverByStartSeq, StartSeq: 1, MaxInflight: 10, AckWait: time.Second},
	}
	for i, cfg := range cfgs {
		if err := cfg.Validate(); err != nil {
			t.Errorf("case %d: expected valid config, got error: %v", i, err)
		}
	}
}

func TestConsumerConfigValidateMaxInflightRequiresExplicit(t *testing.T) {
	cfg := ConsumerConfig{AckPolicy: AckNone, MaxInflight: 10}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for MaxInflight without AckExplicit")
	}
	ae, ok := err.(*ArbitroError)
	if !ok || ae.Code != ErrCodeInvalidConfig {
		t.Fatalf("expected ErrCodeInvalidConfig, got %v", err)
	}
}

func TestConsumerConfigValidateSubjectLimitsRequiresExplicit(t *testing.T) {
	cfg := ConsumerConfig{
		AckPolicy:           AckNone,
		MaxSubjectInflights: []SubjectLimit{{Pattern: "x.*", Limit: 1}},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for MaxSubjectInflights without AckExplicit")
	}
}

func TestConsumerConfigValidateAckWaitRequiresExplicit(t *testing.T) {
	cfg := ConsumerConfig{AckPolicy: AckNone, AckWait: 5 * time.Second}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for AckWait without AckExplicit")
	}
}

func TestConsumerConfigValidateByStartSeqRequiresStartSeq(t *testing.T) {
	cfg := ConsumerConfig{DeliverPolicy: DeliverByStartSeq, StartSeq: 0}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for DeliverByStartSeq with StartSeq=0")
	}
}
