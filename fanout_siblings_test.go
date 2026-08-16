package arbitro

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Several filtered subscriptions sharing ONE consumer over ONE connection.
//
// The broker collapses that into a single wire copy per (connection,
// consumer) and stamps it with whichever subscription won the round; the
// client is what turns it back into one delivery per matching filter. Before
// the registry was keyed by subscription this topology was unreachable from
// Go at all — the dispatch table was keyed by consumer, so the second
// subscribe overwrote the first.
//
// The Rust twin is arbitro-e2e/tests/one_consumer_many_filters.rs.
//
// Skipped unless a broker is reachable; point it elsewhere with ARBITRO_ADDR.

// unique keeps parallel runs and repeated local runs from colliding on
// durable stream and consumer names.
func unique(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// collect drains up to want messages, or gives up after the deadline.
// Returns what it got so a shortfall is reported as a count, not a hang.
func collect(sub *Subscription, want int, timeout time.Duration) []*Msg {
	got := make([]*Msg, 0, want)
	deadline := time.After(timeout)
	for len(got) < want {
		select {
		case m, ok := <-sub.Messages():
			if !ok {
				return got
			}
			got = append(got, m)
		case <-deadline:
			return got
		}
	}
	return got
}

// subjectsOf is a readable failure message rather than a pile of pointers.
func subjectsOf(msgs []*Msg) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Subject()
	}
	return out
}

// TestFanoutSiblingSubscriptionsEachGetTheirFilter proves the local fan-out:
// one consumer, three subscriptions, three different filters, and every
// message reaches exactly the subscriptions whose filter accepts it.
func TestFanoutSiblingSubscriptionsEachGetTheirFilter(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stream := unique("gofan")
	if _, err := c.CreateStream(ctx, stream, StreamConfig{
		SubjectFilter: stream + ".>",
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// One consumer — the same Name for all three subscribes resolves to it.
	cfg := ConsumerConfig{
		Name:      unique("fanconsumer"),
		Fanout:    true,
		AckPolicy: 0, // None: this test measures routing, not ack accounting
	}

	subA, err := c.Subscribe(ctx, stream, cfg, withSubFilter(stream+".a"))
	if err != nil {
		t.Fatalf("subscribe a: %v", err)
	}
	defer subA.Close()

	subB, err := c.Subscribe(ctx, stream, cfg, withSubFilter(stream+".b"))
	if err != nil {
		t.Fatalf("subscribe b: %v", err)
	}
	defer subB.Close()

	subAll, err := c.Subscribe(ctx, stream, cfg, withSubFilter(stream+".>"))
	if err != nil {
		t.Fatalf("subscribe all: %v", err)
	}
	defer subAll.Close()

	// Distinct ids are the whole premise: same id would mean the broker
	// cannot tell the siblings apart on ack.
	if subA.ID() == subB.ID() || subB.ID() == subAll.ID() || subA.ID() == subAll.ID() {
		t.Fatalf("sibling subscriptions share an id: %d %d %d",
			subA.ID(), subB.ID(), subAll.ID())
	}

	if err := c.Publish(ctx, stream, stream+".a", []byte("A")); err != nil {
		t.Fatalf("publish a: %v", err)
	}
	if err := c.Publish(ctx, stream, stream+".b", []byte("B")); err != nil {
		t.Fatalf("publish b: %v", err)
	}

	gotA := collect(subA, 1, 5*time.Second)
	gotB := collect(subB, 1, 5*time.Second)
	gotAll := collect(subAll, 2, 5*time.Second)

	if len(gotA) != 1 || gotA[0].Subject() != stream+".a" {
		t.Errorf("filtered sub a: got %v, want [%s.a]", subjectsOf(gotA), stream)
	}
	if len(gotB) != 1 || gotB[0].Subject() != stream+".b" {
		t.Errorf("filtered sub b: got %v, want [%s.b]", subjectsOf(gotB), stream)
	}
	if len(gotAll) != 2 {
		t.Errorf("wildcard sub: got %v, want both subjects", subjectsOf(gotAll))
	}

	// Every copy must name its OWN subscription, not the one the broker
	// happened to stamp on the wire.
	for _, m := range gotA {
		if m.subID != subA.ID() {
			t.Errorf("copy delivered to sub a carries sub_id %d, want %d", m.subID, subA.ID())
		}
	}
	for _, m := range gotAll {
		if m.subID != subAll.ID() {
			t.Errorf("copy delivered to wildcard sub carries sub_id %d, want %d", m.subID, subAll.ID())
		}
	}
}

// TestFanoutSiblingsAckTheirOwnPending is the same topology under explicit
// acks. The broker opens ONE pending per subscription, so every sibling owes
// an ack under its own id; a client that acked once per wire copy would leave
// the others open and see them redelivered once ack_wait expires.
func TestFanoutSiblingsAckTheirOwnPending(t *testing.T) {
	c := dialOrSkip(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stream := unique("gofanack")
	if _, err := c.CreateStream(ctx, stream, StreamConfig{
		SubjectFilter: stream + ".>",
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// A short ack deadline so an unreleased pending shows up as a
	// redelivery inside the test's lifetime instead of after it.
	cfg := ConsumerConfig{
		Name:        unique("fanackconsumer"),
		Fanout:      true,
		AckPolicy:   1, // Explicit
		AckWait:     2 * time.Second,
		MaxInflight: 1024,
	}

	subA, err := c.Subscribe(ctx, stream, cfg, withSubFilter(stream+".a"))
	if err != nil {
		t.Fatalf("subscribe a: %v", err)
	}
	defer subA.Close()

	subAll, err := c.Subscribe(ctx, stream, cfg, withSubFilter(stream+".>"))
	if err != nil {
		t.Fatalf("subscribe all: %v", err)
	}
	defer subAll.Close()

	if err := c.Publish(ctx, stream, stream+".a", []byte("A")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Both siblings match `<stream>.a`, so both must see it.
	gotA := collect(subA, 1, 5*time.Second)
	gotAll := collect(subAll, 1, 5*time.Second)
	if len(gotA) != 1 {
		t.Fatalf("filtered sub never received its copy")
	}
	if len(gotAll) != 1 {
		t.Fatalf("wildcard sub never received its copy")
	}

	// Each releases its own pending.
	gotA[0].Ack()
	gotAll[0].Ack()

	// Past the ack deadline, an unreleased pending would come back.
	if extra := collect(subA, 1, 4*time.Second); len(extra) != 0 {
		t.Errorf("filtered sub saw a redelivery after acking: %v", subjectsOf(extra))
	}
	if extra := collect(subAll, 1, 1*time.Second); len(extra) != 0 {
		t.Errorf("wildcard sub saw a redelivery after acking: %v", subjectsOf(extra))
	}
}

// TestSubscriptionIDsAreNonZeroAndUnique pins the wire contract itself: a
// zero id is the broker's "unnamed" sentinel and sends every ack down the
// binding-scan path, so the allocator must never hand one out.
func TestSubscriptionIDsAreNonZeroAndUnique(t *testing.T) {
	c := &Client{}
	seen := make(map[uint32]struct{}, 64)
	for i := 0; i < 64; i++ {
		id := c.allocSubID()
		if id == 0 {
			t.Fatalf("allocator handed out the unnamed sentinel at iteration %d", i)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("allocator repeated id %d", id)
		}
		seen[id] = struct{}{}
	}
}
