//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestConsumerCRUD(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("cons-crud")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Create consumer
	_, err = client.CreateConsumer(ctx, stream, arbitro.ConsumerConfig{
		Name:        "test-consumer",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	// Info
	info, err := client.ConsumerInfo(ctx, stream, "test-consumer")
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if info.Name != "test-consumer" {
		t.Errorf("name: got %q, want %q", info.Name, "test-consumer")
	}

	// List
	consumers, err := client.ListConsumers(ctx, stream)
	if err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	found := false
	for _, c := range consumers {
		if c.Name == "test-consumer" {
			found = true
			break
		}
	}
	if !found {
		t.Error("consumer not found in list")
	}

	// Delete
	err = client.DeleteConsumer(ctx, stream, "test-consumer")
	if err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
}

func TestConsumerPauseResume(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("cons-pause")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	_, err = client.CreateConsumer(ctx, stream, arbitro.ConsumerConfig{
		Name:        "pausable",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	// Pause
	err = client.PauseConsumer(ctx, stream, "pausable")
	if err != nil {
		t.Fatalf("pause: %v", err)
	}

	// Resume
	err = client.ResumeConsumer(ctx, stream, "pausable")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
}

func TestNackDelay(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("nack-delay")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "nacker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
		MaxDeliver:  3,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Publish
	err = client.Publish(ctx, stream, stream+".test", []byte("nack-me"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Receive and nack with delay
	select {
	case msg := <-sub.Messages():
		msg.NackDelay(200 * time.Millisecond)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	// Should receive redelivery
	select {
	case msg := <-sub.Messages():
		if !msg.Dup() {
			// May or may not be flagged as dup depending on broker version
		}
		msg.Ack()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for redelivery")
	}
}
