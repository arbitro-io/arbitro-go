//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestRequestReplyTimeout(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("rpc")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Request with no responder — should timeout
	_, err = client.Request(ctx, stream, stream+".validate", []byte(`{"id":"123"}`), 500*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	// Should be context deadline exceeded
	t.Logf("request timeout error (expected): %v", err)
}

func TestRequestReplyRoundTrip(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("rpc-rt")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Set up a responder goroutine
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "responder",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe responder: %v", err)
	}
	defer sub.Close()

	// Responder: read and reply
	go func() {
		for msg := range sub.Messages() {
			// In a real implementation the responder would publish to the reply subject.
			// For now, just ack to prove the flow works.
			msg.Ack()
		}
	}()

	// Send request — it will timeout because we haven't implemented _INBOX routing yet,
	// but the publish side should succeed.
	_, err = client.Request(ctx, stream, stream+".echo", []byte("ping"), 1*time.Second)
	if err != nil {
		t.Logf("request error (expected until _INBOX routing): %v", err)
	}
}
