//go:build integration

package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestRequestReplyTimeout(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	svc, err := client.Service("rpc-timeout").Build(ctx)
	if err != nil {
		t.Fatalf("build service: %v", err)
	}
	defer svc.Close()

	// Request with no responder — should timeout
	_, err = svc.Request(ctx, "rpc-timeout", "noop", []byte(`{"id":"123"}`), 500*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	t.Logf("request timeout error (expected): %v", err)
}

func TestRequestReplyRoundTrip(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	// Build the service
	svc, err := client.Service("echo-svc").Build(ctx)
	if err != nil {
		t.Fatalf("build service: %v", err)
	}
	defer svc.Close()

	// Register echo handler
	svc.Handle("ping", func(req *arbitro.Request) ([]byte, error) {
		return append([]byte("pong:"), req.Data()...), nil
	})

	// Send request
	resp, err := svc.Request(ctx, "echo-svc", "ping", []byte("hello"), 5*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(resp) != "pong:hello" {
		t.Fatalf("expected 'pong:hello', got '%s'", resp)
	}
}

func TestServiceMultipleHandlers(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	svc, err := client.Service("multi-svc").Build(ctx)
	if err != nil {
		t.Fatalf("build service: %v", err)
	}
	defer svc.Close()

	svc.Handle("add", func(req *arbitro.Request) ([]byte, error) {
		return []byte("added"), nil
	})
	svc.Handle("sub", func(req *arbitro.Request) ([]byte, error) {
		return []byte("subtracted"), nil
	})

	resp1, err := svc.Request(ctx, "multi-svc", "add", []byte("1"), 5*time.Second)
	if err != nil {
		t.Fatalf("add request: %v", err)
	}
	if string(resp1) != "added" {
		t.Fatalf("expected 'added', got '%s'", resp1)
	}

	resp2, err := svc.Request(ctx, "multi-svc", "sub", []byte("1"), 5*time.Second)
	if err != nil {
		t.Fatalf("sub request: %v", err)
	}
	if string(resp2) != "subtracted" {
		t.Fatalf("expected 'subtracted', got '%s'", resp2)
	}
}

func TestServiceConcurrentRequests(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	svc, err := client.Service("conc-svc").Build(ctx)
	if err != nil {
		t.Fatalf("build service: %v", err)
	}
	defer svc.Close()

	svc.Handle("echo", func(req *arbitro.Request) ([]byte, error) {
		return req.Data(), nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			payload := []byte(time.Now().String())
			resp, err := svc.Request(ctx, "conc-svc", "echo", payload, 5*time.Second)
			if err != nil {
				t.Errorf("request %d: %v", n, err)
				return
			}
			if string(resp) != string(payload) {
				t.Errorf("request %d: expected '%s', got '%s'", n, payload, resp)
			}
		}(i)
	}
	wg.Wait()
}

func TestServiceCrossConnection(t *testing.T) {
	clientA := connectT(t)
	clientB := connectT(t)
	ctx := context.Background()

	// Service on client A
	svcA, err := clientA.Service("cross-svc").Build(ctx)
	if err != nil {
		t.Fatalf("build service A: %v", err)
	}
	defer svcA.Close()

	svcA.Handle("greet", func(req *arbitro.Request) ([]byte, error) {
		return append([]byte("hello "), req.Data()...), nil
	})

	// Requester on client B
	svcB, err := clientB.Service("requester-svc").Build(ctx)
	if err != nil {
		t.Fatalf("build service B: %v", err)
	}
	defer svcB.Close()

	resp, err := svcB.Request(ctx, "cross-svc", "greet", []byte("world"), 5*time.Second)
	if err != nil {
		t.Fatalf("cross-connection request: %v", err)
	}
	if string(resp) != "hello world" {
		t.Fatalf("expected 'hello world', got '%s'", resp)
	}
}

func TestRequestDisconnectTimeout(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()

	svc, err := client.Service("disc-svc").Build(ctx)
	if err != nil {
		t.Fatalf("build service: %v", err)
	}
	defer svc.Close()

	// Request with no handler registered for this method — will timeout
	_, err = svc.Request(ctx, "disc-svc", "nonexistent", []byte("data"), 500*time.Millisecond)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	t.Logf("timeout error (expected): %v", err)
}
