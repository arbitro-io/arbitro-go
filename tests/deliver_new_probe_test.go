//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// Cross-client check for the C client's `deliver New only` failure: C sees all
// 10 historical messages where it expects 0. This runs the identical scenario
// through a second, independent client against the same broker, so the answer
// is about the broker's contract rather than about either client.
func TestDeliverNewSkipsHistorical(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("dnew")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	for i := 0; i < 10; i++ {
		if err := client.Publish(ctx, stream, stream+".h", []byte("historical")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Consumer created AFTER the writes, asking for new messages only.
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:          "dnew-worker",
		Filter:        stream + ".>",
		AckPolicy:     arbitro.AckExplicit,
		MaxInflight:   100,
		AckWait:       10 * time.Second,
		DeliverPolicy: arbitro.DeliverNew,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	historical := 0
	deadline := time.After(2 * time.Second)
drain:
	for {
		select {
		case msg := <-sub.Messages():
			historical++
			msg.Ack()
		case <-deadline:
			break drain
		}
	}

	if historical != 0 {
		t.Errorf("DeliverNew delivered %d historical messages, want 0 "+
			"— matches what the C suite reports, so the contract is the broker's, not C's",
			historical)
	} else {
		t.Log("DeliverNew skipped all 10 historical messages")
	}
}
