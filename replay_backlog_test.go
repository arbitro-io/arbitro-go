package arbitro

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"
)

// The Go twin of the Rust replay bench (arbitro-e2e/benches/throughput.rs,
// BENCH_MODE=replay) and of tests/34-replay-backlog.test.ts.
//
// That bench is what caught a tail of publishes the Rust client accepted and
// never sent: the store was short by exactly one batch and the replay consumer
// waited forever for messages that had never arrived. The shape that exposes
// it is prefill-then-drain, never publish-while-consuming — a live consumer
// hides a missing tail behind a slow drain.
//
// Skipped unless a broker is reachable, so `go test ./...` stays runnable with
// no infrastructure. Point it somewhere else with ARBITRO_ADDR.

const (
	replayStreams  = 4
	replayBatches  = 40
	replayPerBatch = 256
	replayPerSream = replayBatches * replayPerBatch
)

func replayAddr() string {
	if a := os.Getenv("ARBITRO_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:9898"
}

func dialOrSkip(t *testing.T) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Connect(ctx, replayAddr())
	if err != nil {
		t.Skipf("no broker at %s: %v", replayAddr(), err)
	}
	return c
}

// TestReplayBacklogConservesEveryBatch fills several streams as fast as the
// client will take the work, then reads all of it back. A shortfall means the
// client accepted publishes it never delivered.
func TestReplayBacklogConservesEveryBatch(t *testing.T) {
	admin := dialOrSkip(t)
	defer admin.Close()

	ctx := context.Background()
	names := make([]string, replayStreams)
	for i := range names {
		names[i] = fmt.Sprintf("go-replay-%d-%d-%d", os.Getpid(), time.Now().UnixNano()%1e6, i)
		if _, err := admin.CreateStream(ctx, names[i], StreamConfig{
			SubjectFilter: names[i] + ".>",
			Journal:       JournalMemory,
		}); err != nil {
			t.Fatalf("create stream %s: %v", names[i], err)
		}
		defer func(n string) { _ = admin.DeleteStream(context.Background(), n) }(names[i])
	}

	// One connection per stream, all in this process — the arrangement that
	// made the Rust client drop its tail.
	done := make(chan error, replayStreams)
	for s, name := range names {
		go func(s int, name string) {
			pub, err := Connect(ctx, replayAddr())
			if err != nil {
				done <- fmt.Errorf("publisher %d: %w", s, err)
				return
			}
			defer pub.Close()
			for b := 0; b < replayBatches; b++ {
				entries := make([]BatchEntry, replayPerBatch)
				for i := range entries {
					p := make([]byte, 4)
					binary.LittleEndian.PutUint32(p, uint32(b*replayPerBatch+i))
					entries[i] = BatchEntry{Subject: name + ".evt", Payload: p}
				}
				if _, err := pub.PublishBatch(ctx, name, entries); err != nil {
					done <- fmt.Errorf("stream %s batch %d: %w", name, b, err)
					return
				}
			}
			done <- nil
		}(s, name)
	}
	for range names {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	// Everything is published and confirmed before a single consumer exists.
	for _, name := range names {
		seen := drainExactly(t, name, replayPerSream, 60*time.Second)
		if len(seen) != replayPerSream {
			t.Fatalf("%s: drained %d/%d, missing %d",
				name, len(seen), replayPerSream, replayPerSream-len(seen))
		}
		// The exact set, not just the count: a duplicate covering for a loss
		// would otherwise pass.
		for i := 0; i < replayPerSream; i++ {
			if !seen[uint32(i)] {
				t.Fatalf("%s: sequence %d never arrived", name, i)
			}
		}
	}
}

func drainExactly(t *testing.T, stream string, expected int, timeout time.Duration) map[uint32]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	reader, err := Connect(ctx, replayAddr())
	if err != nil {
		t.Fatalf("reader connect: %v", err)
	}
	defer reader.Close()

	sub, err := reader.Subscribe(ctx, stream, ConsumerConfig{
		Name:          stream,
		Filter:        stream + ".>",
		AckPolicy:     AckExplicit,
		DeliverPolicy: DeliverAll,
		MaxInflight:   4096,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe %s: %v", stream, err)
	}
	defer sub.Close()

	seen := make(map[uint32]bool, expected)
	deadline := time.After(timeout)
	for len(seen) < expected {
		select {
		case m := <-sub.Messages():
			if m == nil {
				t.Fatalf("%s: subscription closed at %d/%d", stream, len(seen), expected)
			}
			if d := m.Data(); len(d) >= 4 {
				seen[binary.LittleEndian.Uint32(d)] = true
			}
			m.Ack()
		case <-deadline:
			return seen
		}
	}
	return seen
}

// TestReplaySaturationNeverDrops hammers the fire-and-forget path without ever
// awaiting the broker — the case that fills the outbound queue. Go answers
// backpressure by blocking rather than erroring, so every message the call
// accepts must land.
func TestReplaySaturationNeverDrops(t *testing.T) {
	admin := dialOrSkip(t)
	defer admin.Close()

	ctx := context.Background()
	name := fmt.Sprintf("go-replay-sat-%d-%d", os.Getpid(), time.Now().UnixNano()%1e6)
	if _, err := admin.CreateStream(ctx, name, StreamConfig{
		SubjectFilter: name + ".>",
		Journal:       JournalMemory,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer func() { _ = admin.DeleteStream(context.Background(), name) }()

	pub, err := Connect(ctx, replayAddr())
	if err != nil {
		t.Fatalf("publisher connect: %v", err)
	}
	defer pub.Close()

	const total = 20000
	for i := 0; i < total; i++ {
		p := make([]byte, 64)
		binary.LittleEndian.PutUint32(p, uint32(i))
		if err := pub.Publish(ctx, name, name+".evt", p); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	seen := drainExactly(t, name, total, 60*time.Second)
	if len(seen) != total {
		t.Fatalf("%s: drained %d/%d, missing %d", name, len(seen), total, total-len(seen))
	}
}
