//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestConnectAndClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := arbitro.Connect(ctx, brokerAddr())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestConnectRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := arbitro.Connect(ctx, "127.0.0.1:19999", arbitro.WithTimeout(1*time.Second))
	if err == nil {
		t.Fatal("expected connection refused error")
	}
}

func TestConnectMetrics(t *testing.T) {
	client := connectT(t)
	m := client.Metrics()

	if m.PublishesSent != 0 {
		t.Errorf("initial publishes: got %d, want 0", m.PublishesSent)
	}
	if m.ActiveSubs != 0 {
		t.Errorf("initial subs: got %d, want 0", m.ActiveSubs)
	}
}
