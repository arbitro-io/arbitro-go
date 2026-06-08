//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestDeleteMessage(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("delmsg")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Publish 3 messages
	for i := 0; i < 3; i++ {
		err = client.Publish(ctx, stream, stream+".test", []byte("msg"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Delete message seq 2
	ok, err := client.DeleteMessage(ctx, stream, 2)
	if err != nil {
		t.Fatalf("delete message: %v", err)
	}
	if !ok {
		t.Log("delete returned false (message may have already been consumed)")
	}

	// Subscribe and verify message 2 is skipped
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "del-worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	received := 0
	timeout := time.After(3 * time.Second)
loop:
	for {
		select {
		case msg := <-sub.Messages():
			received++
			// Seq 2 should be skipped (tombstoned)
			if msg.Seq() == 2 {
				t.Error("received tombstoned message seq=2")
			}
			msg.Ack()
		case <-timeout:
			break loop
		}
	}

	// Should get 2 messages (1 and 3), not 3
	if received > 2 {
		t.Errorf("received %d messages, expected at most 2", received)
	}
}

func TestDeleteMessageViaStream(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("delmsg-stream")

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
	err = s.Publish(ctx, name+".a", []byte("data"))
	if err != nil {
		t.Fatalf("stream publish: %v", err)
	}

	ok, err := s.DeleteMessage(ctx, 1)
	if err != nil {
		t.Fatalf("stream delete message: %v", err)
	}
	_ = ok
}
