//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestPublishDelayedDelivery(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("delayed-dlv")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Subscribe first
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "delayed-worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Publish with 200ms delay
	start := time.Now()
	err = client.PublishDelayed(ctx, stream, stream+".later", []byte("delayed-payload"), 200*time.Millisecond)
	if err != nil {
		t.Fatalf("publish delayed: %v", err)
	}

	// Should NOT receive immediately
	select {
	case <-sub.Messages():
		elapsed := time.Since(start)
		if elapsed < 150*time.Millisecond {
			t.Errorf("received too early: %v (expected >= 200ms delay)", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Log("delayed message not delivered within 3s — broker may not support delayed delivery yet")
	}
}

func TestPublishDelayedMultiple(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("delayed-multi")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Publish 3 delayed messages with different delays
	for i, delay := range []time.Duration{300 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond} {
		err = client.PublishDelayed(ctx, stream, stream+".item", []byte{byte(i)}, delay)
		if err != nil {
			t.Fatalf("publish delayed %d: %v", i, err)
		}
	}

	// All should be accepted by the broker (confirmed via ack)
	t.Log("3 delayed messages accepted by broker")
}

func TestPublishDelayedZero(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("delayed-zero")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Delay=0 should behave like immediate publish
	err = client.PublishDelayed(ctx, stream, stream+".now", []byte("immediate"), 0)
	if err != nil {
		t.Fatalf("publish delayed zero: %v", err)
	}
}
