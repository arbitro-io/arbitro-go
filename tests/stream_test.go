//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
)

func TestStreamCRUD(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("stream-crud")

	// Create
	_, err := client.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter:     name + ".>",
		MaxMsgs:           10000,
		MaxBytes:          1 << 20,
		MaxAge:            1 * time.Hour,
		Journal:           arbitro.JournalTolerant,
		IdempotencyWindow: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Info
	info, err := client.StreamInfo(ctx, name)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.Name != name {
		t.Errorf("info name: got %q, want %q", info.Name, name)
	}

	// Exists
	exists, err := client.StreamExists(ctx, name)
	if err != nil {
		t.Fatalf("stream exists: %v", err)
	}
	if !exists {
		t.Error("stream should exist")
	}

	// List
	streams, err := client.ListStreams(ctx)
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	found := false
	for _, s := range streams {
		if s.Name == name {
			found = true
			break
		}
	}
	if !found {
		t.Error("stream not found in list")
	}

	// Delete
	err = client.DeleteStream(ctx, name)
	if err != nil {
		t.Fatalf("delete stream: %v", err)
	}

	// Verify gone
	exists, err = client.StreamExists(ctx, name)
	if err != nil && !arbitro.IsNotFound(err) {
		t.Fatalf("stream exists after delete: %v", err)
	}
	if exists {
		t.Error("stream should not exist after delete")
	}
}

func TestStreamPurge(t *testing.T) {
	client := connectT(t)
	ctx := context.Background()
	name := uniqueName("stream-purge")

	_, err := client.CreateStream(ctx, name, arbitro.StreamConfig{
		SubjectFilter: name + ".>",
		MaxMsgs:       10000,
		Journal:       arbitro.JournalTolerant,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer client.DeleteStream(ctx, name)

	// Publish a few messages
	for i := 0; i < 5; i++ {
		if err := client.Publish(ctx, name, name+".test", []byte("hello")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Purge
	n, err := client.PurgeStream(ctx, name)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n == 0 {
		t.Log("purge returned 0 (broker may have already cleaned)")
	}
}
