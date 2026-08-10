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

	// Exactly MaxInflight, not "at most". `<= 3` is satisfied by 0, so a
	// delivery path that stopped working entirely passed this test — it could
	// not tell an enforced cap apart from no delivery at all. 10 were
	// published, none are acked, so 3 is the only correct answer. The sibling
	// clients assert the same equality (Rust stage00, C stage 0).
	//
	// Waiting for the cap to fill rather than sleeping a fixed 500ms: that
	// sleep was a race, and it loses often enough to matter — a run of this
	// test observed 0 delivered inside the window and would have failed an
	// exact assertion for a reason that has nothing to do with the cap.
	deadline := time.Now().Add(5 * time.Second)
	for received.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if r := received.Load(); r != 3 {
		t.Fatalf("received %d messages with MaxInflight=3, expected exactly 3", r)
	}

	// Still 3 after the queue has had time to push a fourth: the cap holds,
	// it did not merely lag.
	time.Sleep(300 * time.Millisecond)
	if r := received.Load(); r != 3 {
		t.Errorf("cap exceeded: %d delivered with MaxInflight=3 and nothing acked", r)
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

	// The cap is 1 in flight for this subject, so with nothing acked the
	// second message must not arrive. Both branches of this check used to be
	// empty — whether a second showed up or not, the test did exactly nothing,
	// so the only way it could fail was the first message never arriving. It
	// asserted that delivery works, not that the cap does.
	select {
	case msg := <-sub.Messages():
		time.Sleep(200 * time.Millisecond)
		select {
		case extra := <-sub.Messages():
			t.Errorf("second message delivered (seq %d) while the first was "+
				"unacked and maxSubjectInflight is 1", extra.Seq())
		default:
		}
		msg.Ack()

		// And it must resume once the slot frees, otherwise "nothing else
		// arrives" would also be satisfied by a cap that never releases.
		select {
		case next := <-sub.Messages():
			next.Ack()
		case <-time.After(5 * time.Second):
			t.Fatal("no delivery after acking — the subject slot never released")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first priority message")
	}
}
