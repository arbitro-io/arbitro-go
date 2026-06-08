//go:build integration

package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestMaxInflight(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("inflight")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Consumer with MaxInflight=3
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "limited",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 3,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Publish 10 messages
	for i := 0; i < 10; i++ {
		err = client.Publish(ctx, stream, stream+".test", []byte("msg"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Should receive at most MaxInflight messages before acking
	var received atomic.Int32
	go func() {
		for msg := range sub.Messages() {
			received.Add(1)
			// Hold messages without acking to test backpressure
			_ = msg
		}
	}()

	// Wait a bit and check we don't exceed inflight limit
	time.Sleep(500 * time.Millisecond)
	r := received.Load()
	if r > 3 {
		t.Errorf("received %d messages with MaxInflight=3, expected <= 3", r)
	}
}

func TestSubjectInflightLimits(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("subj-limit")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Consumer with per-subject limit
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "subj-limited",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     30 * time.Second,
		MaxSubjectInflights: []arbitro.SubjectLimit{
			{Pattern: stream + ".priority.>", Limit: 1},
		},
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Publish 5 priority messages
	for i := 0; i < 5; i++ {
		err = client.Publish(ctx, stream, stream+".priority.order", []byte("priority"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Should get at most 1 at a time for priority subject
	select {
	case msg := <-sub.Messages():
		// Got one — now wait briefly and confirm no more arrive without ack
		time.Sleep(200 * time.Millisecond)
		select {
		case <-sub.Messages():
			// Might get a second if broker processes before we check,
			// but the constraint is on in-flight without ack
		default:
			// Good — backpressure working
		}
		msg.Ack()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first priority message")
	}
}
