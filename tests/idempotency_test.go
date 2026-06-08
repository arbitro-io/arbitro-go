//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestIdempotencyDedup(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("idemp")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter:     stream + ".>",
		MaxMsgs:           10000,
		Journal:           arbitro.JournalTolerant,
		IdempotencyWindow: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Publish with same msg_id twice
	msgID := "dedup-test-" + uniqueName("")
	err = client.Publish(ctx, stream, stream+".test", []byte("first"), arbitro.WithMsgID(msgID))
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}

	err = client.Publish(ctx, stream, stream+".test", []byte("second"), arbitro.WithMsgID(msgID))
	if err != nil {
		// Should get duplicate error
		if !arbitro.IsDuplicate(err) {
			t.Fatalf("expected duplicate error, got: %v", err)
		}
	}

	// Subscribe and verify only one message delivered
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "idemp-worker",
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
	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case msg := <-sub.Messages():
			received++
			msg.Ack()
			if string(msg.Data()) != "first" {
				t.Errorf("expected 'first', got %q", msg.Data())
			}
		case <-timeout:
			break loop
		}
	}

	if received != 1 {
		t.Errorf("received %d messages, expected 1 (dedup should prevent second)", received)
	}
}
