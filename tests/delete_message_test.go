//go:build integration

package tests

import (
	"context"
	"fmt"
	"reflect"
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

	// Publish 3 distinguishable messages.
	for i := 0; i < 3; i++ {
		err = client.Publish(ctx, stream, stream+".test", []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Learn the real sequence numbers instead of assuming the stream owns
	// 1..3. Sequence numbers are assigned by the SHARD store, which holds
	// every stream mapped to that shard — so on a broker that has served
	// other streams, "the second message I published" is not seq 2. Reading
	// them off a delivery is the only way to name the right entry.
	seqOf := make(map[string]uint64, 3)
	probe, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "del-probe",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe probe: %v", err)
	}
	for len(seqOf) < 3 {
		select {
		case msg := <-probe.Messages():
			seqOf[string(msg.Data())] = msg.Seq()
			msg.Ack()
		case <-time.After(5 * time.Second):
			probe.Close()
			t.Fatalf("probe saw %d/3 messages: %v", len(seqOf), seqOf)
		}
	}
	probe.Close()

	victim, ok := seqOf["msg-1"]
	if !ok {
		t.Fatalf("probe never saw msg-1: %v", seqOf)
	}

	deleted, err := client.DeleteMessage(ctx, stream, victim)
	if err != nil {
		t.Fatalf("delete message seq %d: %v", victim, err)
	}
	if !deleted {
		t.Fatalf("delete of seq %d reported not-found", victim)
	}

	// A consumer created AFTER the tombstone reads the stream from the
	// start. This is the path that skips per-consumer delivery state and
	// asks the store directly, so it is the one that proves the tombstone
	// is honoured rather than merely already-acked.
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

	// Drain for a bounded window rather than stopping at the expected count:
	// the claim is that a third delivery never arrives, and breaking early
	// would pass by not having waited.
	var received []string
	timeout := time.After(3 * time.Second)
loop:
	for {
		select {
		case msg := <-sub.Messages():
			if msg.Seq() == victim {
				t.Errorf("received tombstoned message seq=%d (%q)", victim, msg.Data())
			}
			received = append(received, string(msg.Data()))
			msg.Ack()
		case <-timeout:
			break loop
		}
	}

	want := []string{"msg-0", "msg-2"}
	if !reflect.DeepEqual(received, want) {
		t.Errorf("received %v, want %v", received, want)
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
