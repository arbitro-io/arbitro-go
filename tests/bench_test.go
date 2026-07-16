//go:build integration

package tests

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

// envInt returns the int value of environment variable name, or def if unset
// or unparseable. Used by benchmarks to tune msgCount / MaxInflight from the
// shell (ARBITRO_BENCH_MSGS, ARBITRO_BENCH_INFLIGHT) without recompiling.
func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// BenchmarkPublishSync measures synchronous publish throughput.
func BenchmarkPublishSync(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()
	stream := benchStream(b, client)

	payload := make([]byte, 128)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(128)

	for i := 0; i < b.N; i++ {
		err := client.Publish(ctx, stream, stream+".bench", payload)
		if err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
}

// BenchmarkPublishAsync measures fire-and-forget throughput via
// Client.PublishAsync (map-lookup path).
func BenchmarkPublishAsync(b *testing.B) {
	client := benchClient(b)
	stream := benchStream(b, client)

	payload := make([]byte, 128)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(128)

	for i := 0; i < b.N; i++ {
		client.PublishAsync(stream, stream+".bench", payload)
	}
}

// BenchmarkStreamPublishAsync measures the same fire-and-forget path via
// Stream.PublishAsync — cached streamID + single-alloc encode + unsafe
// string→bytes for the subject. Should approach Rust client.publish() cost.
func BenchmarkStreamPublishAsync(b *testing.B) {
	client := benchClient(b)
	streamName := benchStream(b, client)
	// Prime the streamCache so Stream.PublishAsync hits the fast path.
	ctx := context.Background()
	if _, err := client.ResolveStreamID(ctx, streamName); err != nil {
		b.Fatalf("resolve: %v", err)
	}
	stream := client.Stream(streamName)

	payload := make([]byte, 128)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	subject := streamName + ".bench"

	b.ResetTimer()
	b.SetBytes(128)

	for i := 0; i < b.N; i++ {
		stream.PublishAsync(subject, payload)
	}
}

// BenchmarkPublishBatch measures batch publish throughput (10 msgs per batch).
func BenchmarkPublishBatch(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()
	stream := benchStream(b, client)

	payload := make([]byte, 128)
	entries := make([]arbitro.BatchEntry, 10)
	for i := range entries {
		entries[i] = arbitro.BatchEntry{
			Subject: stream + ".batch",
			Payload: payload,
		}
	}

	b.ResetTimer()
	b.SetBytes(128 * 10)

	for i := 0; i < b.N; i++ {
		_, err := client.PublishBatch(ctx, stream, entries)
		if err != nil {
			b.Fatalf("batch publish: %v", err)
		}
	}
}

// BenchmarkPubSubE2E measures end-to-end latency: publish → deliver → ack.
func BenchmarkPubSubE2E(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()
	stream := benchStream(b, client)

	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "bench-e2e",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 1000,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	payload := make([]byte, 64)
	b.ResetTimer()
	b.SetBytes(64)

	for i := 0; i < b.N; i++ {
		err := client.Publish(ctx, stream, stream+".e2e", payload)
		if err != nil {
			b.Fatalf("publish: %v", err)
		}
		msg := <-sub.Messages()
		msg.Ack()
	}
}

// BenchmarkPublishParallel measures concurrent publish from multiple goroutines.
func BenchmarkPublishParallel(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()
	stream := benchStream(b, client)

	payload := make([]byte, 128)

	b.ResetTimer()
	b.SetBytes(128)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = client.Publish(ctx, stream, stream+".parallel", payload)
		}
	})
}

// BenchmarkThroughput1K measures sustained end-to-end throughput
// (publish → deliver → ack). Tunable at runtime via:
//
//	ARBITRO_BENCH_MSGS       — messages published per iteration (default 5000)
//	ARBITRO_BENCH_INFLIGHT   — consumer MaxInflight window   (default 1000)
//	ARBITRO_BENCH_PAYLOAD    — payload size in bytes         (default 256)
//	ARBITRO_BENCH_TIMEOUT_MS — per-iteration consume timeout (default 10000)
func BenchmarkThroughput1K(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()
	streamName := benchStream(b, client)

	msgCount := envInt("ARBITRO_BENCH_MSGS", 5_000)
	maxInflight := envInt("ARBITRO_BENCH_INFLIGHT", 1_000)
	payloadSize := envInt("ARBITRO_BENCH_PAYLOAD", 256)
	timeoutMs := envInt("ARBITRO_BENCH_TIMEOUT_MS", 10_000)

	if maxInflight > 65535 {
		b.Fatalf("ARBITRO_BENCH_INFLIGHT=%d exceeds uint16 max (65535)", maxInflight)
	}

	payload := make([]byte, payloadSize)

	// Ack mode: "explicit" (default, per-msg Ack) matches at-least-once
	// semantics; "none" mirrors the Rust replay_drain bench (AckPolicy::None)
	// for apples-to-apples comparison against server drain rate.
	ackMode := os.Getenv("ARBITRO_BENCH_ACK")
	ackPolicy := arbitro.AckExplicit
	consumerInflight := uint16(maxInflight)
	consumerAckWait := 30 * time.Second
	if ackMode == "none" {
		ackPolicy = arbitro.AckNone
		consumerInflight = 0
		consumerAckWait = 0
	}

	sub, err := client.Subscribe(ctx, streamName, arbitro.ConsumerConfig{
		Name:        "bench-1k",
		Filter:      streamName + ".>",
		AckPolicy:   ackPolicy,
		MaxInflight: consumerInflight,
		AckWait:     consumerAckWait,
	})
	if err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Prime the stream cache and grab a Stream handle so the publisher goroutine
	// hits the fast path (cached streamID, single-alloc encode).
	if _, err := client.ResolveStreamID(ctx, streamName); err != nil {
		b.Fatalf("resolve: %v", err)
	}
	stream := client.Stream(streamName)
	subject := streamName + ".throughput"

	b.Logf("throughput bench: msgs=%d inflight=%d payload=%dB timeout=%dms ack=%s seen_disabled=%v",
		msgCount, maxInflight, payloadSize, timeoutMs, ackMode,
		os.Getenv("ARBITRO_GO_DISABLE_SEEN") == "1")

	ackPerMsg := ackPolicy == arbitro.AckExplicit

	b.ResetTimer()

	for iter := 0; iter < b.N; iter++ {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < msgCount; i++ {
				stream.PublishAsync(subject, payload)
			}
		}()

		var acked atomic.Int32
		target := int32(msgCount)

		// A single Timer, Reset per message — avoids the time.After() Go
		// antipattern which allocates a new channel+timer per iteration
		// and leaks them into the runtime timer heap for the full 10s TTL.
		// This was a serious contributor to the throughput ceiling.
		iterTimeout := time.Duration(timeoutMs) * time.Millisecond
		timer := time.NewTimer(iterTimeout)
		defer timer.Stop()

		for acked.Load() < target {
			// Reset the timer for THIS message; drain any leftover fire so
			// the next select doesn't see a stale value.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(iterTimeout)

			select {
			case msg := <-sub.Messages():
				if ackPerMsg {
					msg.Ack()
				}
				acked.Add(1)
			case <-timer.C:
				b.Fatalf("timeout at %d/%d", acked.Load(), target)
			}
		}
		wg.Wait()
	}

	b.SetBytes(int64(payloadSize * msgCount))
}

// BenchmarkReplayDrain mirrors Rust's arbitro-e2e/benches/throughput.rs
// `replay_drain`: pre-load N msgs onto the stream, THEN subscribe and drain.
// Consumer does not compete with a live publisher — this isolates pure
// broker→client delivery throughput. Matches the Rust bench 1:1 for a fair
// cross-client comparison.
func BenchmarkReplayDrain(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()
	streamName := benchStream(b, client)

	msgCount := envInt("ARBITRO_BENCH_MSGS", 5_000)
	payloadSize := envInt("ARBITRO_BENCH_PAYLOAD", 64)

	if _, err := client.ResolveStreamID(ctx, streamName); err != nil {
		b.Fatalf("resolve: %v", err)
	}
	stream := client.Stream(streamName)
	payload := make([]byte, payloadSize)
	subject := streamName + ".replay"

	// Pre-load N msgs into the stream (fire-and-forget) and wait for them
	// to be durably visible via a sync flush publish.
	for i := 0; i < msgCount; i++ {
		stream.PublishAsync(subject, payload)
	}
	if err := client.Publish(ctx, streamName, subject, payload); err != nil {
		b.Fatalf("flush publish: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(payloadSize * msgCount))

	for iter := 0; iter < b.N; iter++ {
		sub, err := client.Subscribe(ctx, streamName, arbitro.ConsumerConfig{
			Name:        fmt.Sprintf("replay-drain-%d-%d", time.Now().UnixNano(), iter),
			Filter:      streamName + ".>",
			AckPolicy:   arbitro.AckNone,
			MaxInflight: 0,
			DeliverPolicy: arbitro.DeliverAll,
		})
		if err != nil {
			b.Fatalf("subscribe iter %d: %v", iter, err)
		}

		received := 0
		timer := time.NewTimer(15 * time.Second)
		for received < msgCount {
			select {
			case <-sub.Messages():
				received++
			case <-timer.C:
				b.Fatalf("iter %d timeout at %d/%d", iter, received, msgCount)
			}
		}
		timer.Stop()
		sub.Close()
	}
}

// BenchmarkPublishBatchAsync measures fire-and-forget batch publish throughput.
// This mirrors the Rust throughput bench: batch-256, fire-and-forget, write coalescing.
func BenchmarkPublishBatchAsync(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()
	stream := benchStream(b, client)

	// Pre-resolve stream ID for zero-overhead path
	streamID, err := client.ResolveStreamID(ctx, stream)
	if err != nil {
		b.Fatalf("resolve stream id: %v", err)
	}

	payload := make([]byte, 64)
	entries := make([]arbitro.BatchEntry, 256)
	for i := range entries {
		entries[i] = arbitro.BatchEntry{
			Subject: stream + ".batch",
			Payload: payload,
		}
	}

	b.ResetTimer()
	b.SetBytes(64 * 256)

	for i := 0; i < b.N; i++ {
		client.PublishBatchAsync(stream, entries)
	}
	b.StopTimer()
	// Final sync to ensure all frames are flushed — outside timing.
	_ = client.Publish(ctx, stream, stream+".flush", payload)
	_ = streamID
}

// BenchmarkPublishFireAndForget measures the raw fire-and-forget path
// with pre-resolved stream ID (equivalent to Rust's client.publish()).
func BenchmarkPublishFireAndForget(b *testing.B) {
	client := benchClient(b)
	stream := benchStream(b, client)

	ctx := context.Background()
	streamID, err := client.ResolveStreamID(ctx, stream)
	if err != nil {
		b.Fatalf("resolve stream id: %v", err)
	}

	payload := make([]byte, 64)
	subject := stream + ".fire"

	b.ResetTimer()
	b.SetBytes(64)

	for i := 0; i < b.N; i++ {
		_ = client.PublishFireAndForget(streamID, subject, payload)
	}
}

// BenchmarkPublishParallelFireAndForget measures concurrent fire-and-forget from N goroutines.
func BenchmarkPublishParallelFireAndForget(b *testing.B) {
	client := benchClient(b)
	stream := benchStream(b, client)

	ctx := context.Background()
	streamID, err := client.ResolveStreamID(ctx, stream)
	if err != nil {
		b.Fatalf("resolve stream id: %v", err)
	}

	payload := make([]byte, 64)
	subject := stream + ".parallel"

	b.ResetTimer()
	b.SetBytes(64)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = client.PublishFireAndForget(streamID, subject, payload)
		}
	})
}

// --- chaos/stress ---

func TestChaosRapidConnectDisconnect(t *testing.T) {
	ctx := context.Background()
	const N = 20

	for i := 0; i < N; i++ {
		c, err := arbitro.Connect(ctx, brokerAddr(), arbitro.WithTimeout(3*time.Second))
		if err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
		c.Close()
	}
	t.Logf("rapid connect/disconnect: %d cycles completed", N)
}

func TestChaosConcurrentPublishers(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("chaos-pub")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       100000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// 10 goroutines each publishing 100 messages
	const workers = 10
	const perWorker = 100
	var wg sync.WaitGroup
	var errs atomic.Int32

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				err := client.Publish(ctx, stream, stream+".chaos", []byte(fmt.Sprintf("w%d-m%d", id, i)))
				if err != nil {
					errs.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	if e := errs.Load(); e > 0 {
		t.Errorf("%d/%d publishes failed", e, workers*perWorker)
	} else {
		t.Logf("%d concurrent publishes succeeded", workers*perWorker)
	}
}

func TestChaosPublishAfterClose(t *testing.T) {
	ctx := context.Background()
	c, err := arbitro.Connect(ctx, brokerAddr(), arbitro.WithTimeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	stream := uniqueName("chaos-close")
	_, _ = c.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})

	c.Close()

	// Publish after close should error, not panic
	err = c.Publish(ctx, stream, stream+".test", []byte("after-close"))
	if err == nil {
		t.Error("expected error publishing after close")
	}
}

func TestChaosMaxInflightStarvation(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("chaos-starve")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// MaxInflight=1, publish 10 messages, ack one at a time
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "starve-worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 1,
		AckWait:     5 * time.Second,
		MaxDeliver:  3,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	for i := 0; i < 10; i++ {
		err = client.Publish(ctx, stream, stream+".test", []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Should be able to consume all 10 one at a time
	received := 0
	timeout := time.After(10 * time.Second)
	for received < 10 {
		select {
		case msg := <-sub.Messages():
			msg.Ack()
			received++
		case <-timeout:
			t.Fatalf("starvation: only received %d/10", received)
		}
	}
	t.Logf("all 10 messages consumed serially (MaxInflight=1)")
}

// --- helpers ---

func benchClient(b *testing.B) *arbitro.Client {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := arbitro.Connect(ctx, brokerAddr(), arbitro.WithTimeout(5*time.Second))
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	b.Cleanup(func() { client.Close() })
	return client
}

func benchStream(b *testing.B, client *arbitro.Client) string {
	b.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("bench-%d", time.Now().UnixNano()%100000)

	_, err := client.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter: name + ".>",
		MaxMsgs:       1_000_000,
		Journal:       arbitro.JournalMemory, // fastest for benchmarks
	})
	if err != nil && !arbitro.IsAlreadyExists(err) {
		b.Fatalf("create bench stream: %v", err)
	}
	b.Cleanup(func() { client.DeleteStream(ctx, name) })
	return name
}
