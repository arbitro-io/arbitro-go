//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestMetricsCounters(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("metrics")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Baseline
	m0 := client.Metrics()
	if m0.PublishesSent != 0 {
		t.Errorf("initial publishes: %d", m0.PublishesSent)
	}

	// Subscribe
	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "metrics-worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	m1 := client.Metrics()
	if m1.ActiveSubs < 1 {
		t.Errorf("active subs after subscribe: %d, want >= 1", m1.ActiveSubs)
	}

	// Publish 5 messages
	const N = 5
	for i := 0; i < N; i++ {
		err = client.Publish(ctx, stream, stream+".test", []byte("msg"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	m2 := client.Metrics()
	if m2.PublishesSent < N {
		t.Errorf("publishes after %d sends: got %d", N, m2.PublishesSent)
	}

	// Consume and ack
	for i := 0; i < N; i++ {
		select {
		case msg := <-sub.Messages():
			msg.Ack()
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout at msg %d", i)
		}
	}

	m3 := client.Metrics()
	if m3.AcksSent < N {
		t.Errorf("acks after %d acks: got %d", N, m3.AcksSent)
	}
	if m3.DeliveriesRecv < N {
		t.Errorf("deliveries: got %d, want >= %d", m3.DeliveriesRecv, N)
	}
}

func TestMetricsNack(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("metrics-nack")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	sub, err := client.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "nack-worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 100,
		AckWait:     10 * time.Second,
		MaxDeliver:  3,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	err = client.Publish(ctx, stream, stream+".test", []byte("nack-me"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Nack it
	select {
	case msg := <-sub.Messages():
		msg.Nack()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	m := client.Metrics()
	if m.NacksSent < 1 {
		t.Errorf("nacks: got %d, want >= 1", m.NacksSent)
	}
}

func TestMetricsAsync(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("metrics-async")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// PublishAsync should still count
	for i := 0; i < 10; i++ {
		client.PublishAsync(stream, stream+".fast", []byte("fire"))
	}

	time.Sleep(100 * time.Millisecond)
	m := client.Metrics()
	if m.PublishesSent < 10 {
		t.Errorf("async publishes: got %d, want >= 10", m.PublishesSent)
	}
}
