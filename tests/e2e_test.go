//go:build integration

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// TestE2ECreatePublishConsumeAck is the complete lifecycle test:
// create stream → publish → consume → ack → verify pending=0
func TestE2ECreatePublishConsumeAck(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("e2e")

	// 1. Create stream
	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// 2. Subscribe
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "e2e-worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// 3. Publish N messages
	const N = 10
	for i := 0; i < N; i++ {
		payload := fmt.Sprintf("msg-%d", i)
		err = client.Publish(ctx, stream, stream+".order", []byte(payload))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// 4. Consume and ack all
	received := 0
	timeout := time.After(10 * time.Second)
	for received < N {
		select {
		case msg := <-sub.Messages():
			expected := fmt.Sprintf("msg-%d", received)
			if string(msg.Data()) != expected {
				t.Errorf("msg %d: got %q, want %q", received, msg.Data(), expected)
			}
			msg.Ack()
			received++
		case <-timeout:
			t.Fatalf("timeout: received %d/%d", received, N)
		}
	}

	// 5. Verify pending = 0
	time.Sleep(100 * time.Millisecond) // let acks propagate
	pending, err := client.GetPending(ctx, stream, "e2e-worker")
	if err != nil {
		t.Logf("get pending: %v (may not be supported)", err)
	} else if pending != 0 {
		t.Errorf("pending: got %d, want 0", pending)
	}

	// 6. Verify metrics
	m := client.Metrics()
	if m.PublishesSent < N {
		t.Errorf("publishes: got %d, want >= %d", m.PublishesSent, N)
	}
	if m.AcksSent < N {
		t.Errorf("acks: got %d, want >= %d", m.AcksSent, N)
	}
	if m.DeliveriesRecv < N {
		t.Errorf("deliveries: got %d, want >= %d", m.DeliveriesRecv, N)
	}
}

// TestStreamHelper verifies the Stream/Consumer sugar API.
func TestStreamHelper(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("sugar")

	_, err := client.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter: name + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, name)

	s := client.Stream(name)

	// Publish via stream helper
	err = s.Publish(ctx, name+".test", []byte("sugar-payload"))
	if err != nil {
		t.Fatalf("stream publish: %v", err)
	}

	// Batch via stream helper
	_, err = s.PublishBatch(ctx, []arbitro.BatchEntry{
		{Subject: name + ".a", Payload: []byte("a")},
		{Subject: name + ".b", Payload: []byte("b")},
	})
	if err != nil {
		t.Fatalf("stream batch: %v", err)
	}

	// Async via stream helper
	s.PublishAsync(name+".c", []byte("async"))

	// Info
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.Name != name {
		t.Errorf("info name: got %q, want %q", info.Name, name)
	}

	// Consumer helper
	c := s.Consumer(arbitro.ConsumerConfig{
		Name:        "sugar-consumer",
		Filter:      name + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})

	sub, err := c.Subscribe(ctx)
	if err != nil {
		t.Fatalf("consumer subscribe: %v", err)
	}
	defer sub.Close()

	// Should receive at least the first message
	select {
	case msg := <-sub.Messages():
		msg.Ack()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}
