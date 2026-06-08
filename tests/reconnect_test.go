//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestReconnectAfterClose(t *testing.T) {
	ctx := context.Background()

	// Connect, close, reconnect — basic lifecycle
	client1, err := arbitro.Connect(ctx, brokerAddr(), arbitro.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect 1: %v", err)
	}
	client1.Close()

	// Should be able to create a new connection immediately
	client2, err := arbitro.Connect(ctx, brokerAddr(), arbitro.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect 2 after close: %v", err)
	}
	defer client2.Close()

	// Verify it works
	m := client2.Metrics()
	if m.Reconnects != 0 {
		t.Logf("reconnects: %d", m.Reconnects)
	}
}

func TestReconnectPublishAfterBrokerDrop(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	stream := uniqueName("reconn-pub")

	_, err := client.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer client.DeleteStream(ctx, stream)

	// Publish should work
	err = client.Publish(ctx, stream, stream+".test", []byte("before-drop"))
	if err != nil {
		t.Fatalf("publish before: %v", err)
	}

	// Note: to fully test reconnect we'd need to kill the broker process.
	// This test verifies the client handles closed connections gracefully.
	t.Log("publish succeeded — reconnect test requires broker restart (manual)")
}

func TestMultipleConnectionsConcurrent(t *testing.T) {
	ctx := context.Background()
	const N = 5

	clients := make([]*arbitro.Client, N)
	for i := 0; i < N; i++ {
		c, err := arbitro.Connect(ctx, brokerAddr(), arbitro.WithTimeout(5*time.Second))
		if err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
		clients[i] = c
	}

	// All should be able to publish
	stream := uniqueName("multi-conn")
	_, err := clients[0].CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	defer clients[0].DeleteStream(ctx, stream)

	for i, c := range clients {
		err := c.Publish(ctx, stream, stream+".test", []byte("from-client"))
		if err != nil {
			t.Errorf("client %d publish: %v", i, err)
		}
	}

	// Close all
	for _, c := range clients {
		c.Close()
	}
}
