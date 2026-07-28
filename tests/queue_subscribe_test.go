//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// Mirrors the Rust e2e suite for QueueSubscribe. These run against a real
// broker; compiling is not evidence that a queue distributes anything.

func createQueueStream(t *testing.T, client *arbitro.Client, ctx context.Context, name string) {
	t.Helper()
	_, err := client.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter: name + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	t.Cleanup(func() { client.DeleteStream(context.Background(), name) })
}

// drainQueue collects up to want messages, acking each, giving up after budget.
func drainQueue(sub *arbitro.Subscription, want int, budget time.Duration) [][]byte {
	deadline := time.After(budget)
	got := make([][]byte, 0, want)
	for len(got) < want {
		select {
		case msg := <-sub.Messages():
			data := append([]byte(nil), msg.Data()...)
			got = append(got, data)
			msg.Ack()
		case <-deadline:
			return got
		}
	}
	return got
}

// Same group from three connections: every message lands on exactly one
// worker. A duplicate means queue dedup broke; a shortfall means loss.
func TestQueueSubscribeLoadBalances(t *testing.T) {
	admin := connectT(t)
	ctx := context.Background()
	stream := uniqueName("qs-lb")
	createQueueStream(t, admin, ctx, stream)

	const total = 30
	subs := make([]*arbitro.Subscription, 3)
	for i := range subs {
		worker := connectT(t)
		sub, err := worker.QueueSubscribe(ctx, stream, arbitro.QueueOptions{
			Group:  "workers",
			Filter: stream + ".>",
		})
		if err != nil {
			t.Fatalf("worker %d join: %v", i, err)
		}
		defer sub.Close()
		subs[i] = sub
	}

	for i := 0; i < total; i++ {
		if err := admin.Publish(ctx, stream, stream+".job", []byte{byte(i)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	type result struct{ msgs [][]byte }
	results := make(chan result, len(subs))
	for _, sub := range subs {
		go func(s *arbitro.Subscription) {
			results <- result{drainQueue(s, total, 5*time.Second)}
		}(sub)
	}

	seen := make(map[byte]int)
	count := 0
	for range subs {
		r := <-results
		for _, m := range r.msgs {
			if len(m) != 1 {
				t.Fatalf("unexpected payload %v", m)
			}
			seen[m[0]]++
			count++
		}
	}

	if count != total {
		t.Fatalf("queue delivered %d messages, want exactly %d "+
			"(fewer means loss, more means the queue duplicated)", count, total)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("message %d delivered %d times; a queue must deliver each job once", k, n)
		}
	}
}

// One durable consumer per queue NAME, counted against the broker's own
// ListConsumers -- not per subscription and not per connection.
func TestQueueSubscribeOneConsumerPerQueueName(t *testing.T) {
	admin := connectT(t)
	ctx := context.Background()
	stream := uniqueName("qs-count")
	createQueueStream(t, admin, ctx, stream)

	countConsumers := func() int {
		list, err := admin.ListConsumers(ctx, stream)
		if err != nil {
			t.Fatalf("list consumers: %v", err)
		}
		return len(list)
	}

	if n := countConsumers(); n != 0 {
		t.Fatalf("expected no consumers before any join, got %d", n)
	}

	for i := 0; i < 3; i++ {
		worker := connectT(t)
		sub, err := worker.QueueSubscribe(ctx, stream, arbitro.QueueOptions{Group: "workers"})
		if err != nil {
			t.Fatalf("worker %d join: %v", i, err)
		}
		defer sub.Close()
	}
	if n := countConsumers(); n != 1 {
		t.Fatalf("3 workers on one queue name must share ONE consumer, got %d "+
			"(a consumer per subscription would report 3)", n)
	}

	other := connectT(t)
	subB, err := other.QueueSubscribe(ctx, stream, arbitro.QueueOptions{Group: "audit"})
	if err != nil {
		t.Fatalf("audit join: %v", err)
	}
	defer subB.Close()
	if n := countConsumers(); n != 2 {
		t.Fatalf("a distinct queue name must add exactly one consumer, got %d", n)
	}

	rejoin := connectT(t)
	subC, err := rejoin.QueueSubscribe(ctx, stream, arbitro.QueueOptions{Group: "workers"})
	if err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	defer subC.Close()
	if n := countConsumers(); n != 2 {
		t.Fatalf("re-joining an existing queue must not create a consumer, got %d", n)
	}
}

// A different Group is an independent durable queue: each gets its own full
// copy of the stream.
func TestQueueSubscribeDistinctGroupsAreIndependent(t *testing.T) {
	admin := connectT(t)
	ctx := context.Background()
	stream := uniqueName("qs-indep")
	createQueueStream(t, admin, ctx, stream)

	billing := connectT(t)
	subA, err := billing.QueueSubscribe(ctx, stream, arbitro.QueueOptions{Group: "billing"})
	if err != nil {
		t.Fatalf("billing join: %v", err)
	}
	defer subA.Close()

	audit := connectT(t)
	subB, err := audit.QueueSubscribe(ctx, stream, arbitro.QueueOptions{Group: "audit"})
	if err != nil {
		t.Fatalf("audit join: %v", err)
	}
	defer subB.Close()

	const n = 5
	for i := 0; i < n; i++ {
		if err := admin.Publish(ctx, stream, stream+".evt", []byte{byte(i)}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	gotA := drainQueue(subA, n, 5*time.Second)
	gotB := drainQueue(subB, n, 5*time.Second)
	if len(gotA) != n {
		t.Fatalf("queue 'billing' must get its own full copy of %d, got %d", n, len(gotA))
	}
	if len(gotB) != n {
		t.Fatalf("queue 'audit' must get its own full copy of %d, got %d "+
			"(a distinct group is an INDEPENDENT queue, not a member of the same one)", n, len(gotB))
	}
}

// An empty Group falls back to the stream name, so the zero value of
// QueueOptions is a usable queue.
func TestQueueSubscribeGroupDefaultsToStreamName(t *testing.T) {
	admin := connectT(t)
	ctx := context.Background()
	stream := uniqueName("qs-default")
	createQueueStream(t, admin, ctx, stream)

	worker := connectT(t)
	sub, err := worker.QueueSubscribe(ctx, stream, arbitro.QueueOptions{})
	if err != nil {
		t.Fatalf("join with zero-value options: %v", err)
	}
	defer sub.Close()

	list, err := admin.ListConsumers(ctx, stream)
	if err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected exactly one consumer, got %d", len(list))
	}
	// ListConsumers carries ids, not names, so resolve the name directly:
	// looking it up AS the stream name is what proves the fallback happened.
	info, err := admin.ConsumerInfo(ctx, stream, stream)
	if err != nil {
		t.Fatalf("an empty Group must create a consumer named after the stream "+
			"%q, but looking it up by that name failed: %v", stream, err)
	}
	if info.Name != stream {
		t.Fatalf("consumer name %q, want the stream name %q", info.Name, stream)
	}

	if err := admin.Publish(ctx, stream, stream+".job", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := drainQueue(sub, 1, 5*time.Second); len(got) != 1 {
		t.Fatal("the default-group queue must actually deliver")
	}
}

// A job the worker never acked must come back. This is the half of
// at-least-once that separates a queue from fire-and-forget.
func TestQueueSubscribeRedeliversUnackedJob(t *testing.T) {
	admin := connectT(t)
	ctx := context.Background()
	stream := uniqueName("qs-redeliver")
	createQueueStream(t, admin, ctx, stream)

	worker := connectT(t)
	sub, err := worker.QueueSubscribe(ctx, stream, arbitro.QueueOptions{Group: "workers"})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	defer sub.Close()

	if err := admin.Publish(ctx, stream, stream+".job", []byte("job-1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case msg := <-sub.Messages():
		if string(msg.Data()) != "job-1" {
			t.Fatalf("first delivery: got %q", msg.Data())
		}
		msg.Nack()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}

	select {
	case msg := <-sub.Messages():
		if string(msg.Data()) != "job-1" {
			t.Fatalf("redelivery: got %q, want the same job", msg.Data())
		}
		msg.Ack()
	case <-time.After(8 * time.Second):
		t.Fatal("a nacked job MUST be redelivered -- without this the queue " +
			"silently loses work whenever a worker fails")
	}
}
