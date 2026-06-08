//go:build integration

package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

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

// BenchmarkPublishAsync measures fire-and-forget throughput.
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

// BenchmarkThroughput1K measures sustained throughput with 1000 messages.
func BenchmarkThroughput1K(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()
	stream := benchStream(b, client)

	const msgCount = 1000
	payload := make([]byte, 256)

	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "bench-1k",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 1000,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	b.ResetTimer()

	for iter := 0; iter < b.N; iter++ {
		// Publish 1000 messages
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < msgCount; i++ {
				_ = client.Publish(ctx, stream, stream+".throughput", payload)
			}
		}()

		// Consume and ack all
		var acked atomic.Int32
		for acked.Load() < msgCount {
			select {
			case msg := <-sub.Messages():
				msg.Ack()
				acked.Add(1)
			case <-time.After(10 * time.Second):
				b.Fatalf("timeout at %d/%d", acked.Load(), msgCount)
			}
		}
		wg.Wait()
	}

	b.SetBytes(256 * msgCount)
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
