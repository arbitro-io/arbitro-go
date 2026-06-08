//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestPubSubRoundTrip(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("pubsub")

	// Create stream
	_, err := client.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter: name + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, name)

	// Subscribe
	sub, err := client.Subscribe(ctx, name, arbitro.ConsumerConfig{
		Name:        "worker",
		Filter:      name + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Publish
	payload := []byte("hello from Go")
	err = client.Publish(ctx, name, name+".test", payload)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Receive
	select {
	case msg := <-sub.Messages():
		if string(msg.Data()) != "hello from Go" {
			t.Errorf("data: got %q, want %q", msg.Data(), payload)
		}
		if msg.Subject() != name+".test" {
			t.Errorf("subject: got %q, want %q", msg.Subject(), name+".test")
		}
		msg.Ack()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Verify metrics
	m := client.Metrics()
	if m.PublishesSent < 1 {
		t.Errorf("publishes sent: got %d, want >= 1", m.PublishesSent)
	}
}

func TestPubSubBatch(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("batch")

	_, err := client.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter: name + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, name)

	// Subscribe
	sub, err := client.Subscribe(ctx, name, arbitro.ConsumerConfig{
		Name:        "batch-worker",
		Filter:      name + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Batch publish
	entries := []arbitro.BatchEntry{
		{Subject: name + ".a", Payload: []byte("msg-a")},
		{Subject: name + ".b", Payload: []byte("msg-b")},
		{Subject: name + ".c", Payload: []byte("msg-c")},
	}
	firstSeq, err := client.PublishBatch(ctx, name, entries)
	if err != nil {
		t.Fatalf("batch publish: %v", err)
	}
	if firstSeq == 0 {
		t.Error("expected non-zero first seq")
	}

	// Receive all 3
	received := 0
	timeout := time.After(5 * time.Second)
	for received < 3 {
		select {
		case msg := <-sub.Messages():
			msg.Ack()
			received++
		case <-timeout:
			t.Fatalf("timeout: received %d/3", received)
		}
	}
}

func TestPublishAsync(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("async")

	_, err := client.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter: name + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, name)

	// Fire-and-forget
	client.PublishAsync(name, name+".fast", []byte("fire"))
	// If we get here without panic, it worked
	m := client.Metrics()
	if m.PublishesSent < 1 {
		t.Errorf("expected at least 1 publish sent")
	}
}

func TestPublishDelayed(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("delayed")

	_, err := client.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter: name + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, name)

	// Publish with 100ms delay
	err = client.PublishDelayed(ctx, name, name+".later", []byte("delayed-msg"), 100*time.Millisecond)
	if err != nil {
		t.Fatalf("publish delayed: %v", err)
	}
}
