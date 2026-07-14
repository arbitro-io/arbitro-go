//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// TestGetPendingReflectsUnackedMessages exercises the G13 ConsumerStats
// wire-up end to end: publish messages without acking them, then confirm
// GetPending reports the live pending-ack count from the broker (not the
// hardcoded 0 the pre-fix stub always returned).
func TestGetPendingReflectsUnackedMessages(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("getpending")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	const consumerName = "pending-worker"
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        consumerName,
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Publish 3 messages but don't ack any of them yet.
	for i := 0; i < 3; i++ {
		if err := client.Publish(ctx, stream, stream+".evt", []byte("payload")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Wait for all 3 to be delivered (still unacked at this point).
	received := 0
	deadline := time.After(5 * time.Second)
	msgs := make([]*arbitro.Msg, 0, 3)
	for received < 3 {
		select {
		case msg := <-sub.Messages():
			msgs = append(msgs, msg)
			received++
		case <-deadline:
			t.Fatalf("timeout waiting for deliveries, got %d/3", received)
		}
	}

	pending, err := client.GetPending(ctx, stream, consumerName)
	if err != nil {
		t.Fatalf("get pending: %v", err)
	}
	if pending != 3 {
		t.Fatalf("pending = %d, want 3 (before any acks)", pending)
	}

	// Ack them all, then pending should drop to 0.
	for _, m := range msgs {
		m.Ack()
	}
	// Acks are batched (up to 1ms) — give them a moment to flush.
	time.Sleep(200 * time.Millisecond)

	pending, err = client.GetPending(ctx, stream, consumerName)
	if err != nil {
		t.Fatalf("get pending after ack: %v", err)
	}
	if pending != 0 {
		t.Fatalf("pending after ack = %d, want 0", pending)
	}
}

// TestConsumerConfigValidationRejectsInvalidCombination exercises G17: an
// invalid ConsumerConfig must be rejected client-side (no network
// round-trip needed to discover the mistake) when passed to Subscribe.
func TestConsumerConfigValidationRejectsInvalidCombination(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("cfgvalidate")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	_, err = client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "bad-consumer",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckNone,
		MaxInflight: 50, // invalid: MaxInflight requires AckExplicit
	})
	if err == nil {
		t.Fatal("expected client-side validation error, got nil")
	}
	t.Logf("validation error (expected): %v", err)

	_, err = client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:          "bad-consumer-2",
		Filter:        stream + ".>",
		DeliverPolicy: arbitro.DeliverByStartSeq, // invalid: StartSeq unset
	})
	if err == nil {
		t.Fatal("expected client-side validation error for ByStartSeq without StartSeq, got nil")
	}
	t.Logf("validation error (expected): %v", err)
}
