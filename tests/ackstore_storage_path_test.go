//go:build integration

package tests

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arbitro-io/arbitro-go"
	"github.com/arbitro-io/arbitro-go/internal/ackstore"
)

// TestAckStoreDirOptionAgainstBroker proves the storage path is real client
// configuration end-to-end: WithAckStoreDir puts the WAL exactly where the
// caller asked, against a live broker, and the dedup guarantee still holds
// across a restart on that directory.
func TestAckStoreDirOptionAgainstBroker(t *testing.T) {
	ctx := context.Background()
	stream := uniqueName("ackstore-dir")
	// Nested and non-existent on purpose: the client must create the tree.
	walDir := filepath.Join(t.TempDir(), "var", "lib", "myapp", "ackstore")

	c1, err := arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckStoreDir(walDir),
	)
	if err != nil {
		t.Fatalf("connect with WithAckStoreDir: %v", err)
	}

	if _, err := os.Stat(filepath.Join(walDir, "ackstore.log")); err != nil {
		t.Fatalf("the WAL must materialize at the configured path: %v", err)
	}

	if _, err := c1.CreateStream(ctx, stream, arbitro.StreamConfig{
		SubjectFilter: stream + ".>",
		MaxMsgs:       100000,
		Journal:       arbitro.JournalTolerant,
	}); err != nil && !arbitro.IsAlreadyExists(err) {
		t.Fatalf("create stream: %v", err)
	}

	const total = 20
	for i := 0; i < total; i++ {
		if err := c1.Publish(ctx, stream, stream+".job", []byte("job")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	sub1, err := c1.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 1000,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe 1: %v", err)
	}

	processed := 0
	timer := time.NewTimer(15 * time.Second)
	for processed < total {
		select {
		case msg := <-sub1.Messages():
			processed++
			msg.Ack()
		case <-timer.C:
			t.Fatalf("session1 timeout at %d/%d", processed, total)
		}
	}
	time.Sleep(200 * time.Millisecond)
	if err := c1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Restart on the SAME configured directory: nothing may be reprocessed.
	c2, err := arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckStoreDir(walDir),
	)
	if err != nil {
		t.Fatalf("connect 2: %v", err)
	}
	defer c2.Close()

	sub2, err := c2.Subscribe(ctx, stream, arbitro.ConsumerConfig{
		Name:        "worker",
		Filter:      stream + ".>",
		AckPolicy:   arbitro.AckExplicit,
		MaxInflight: 1000,
		AckWait:     30 * time.Second,
	})
	if err != nil {
		t.Fatalf("subscribe 2: %v", err)
	}

	reprocessed := 0
	drain := time.NewTimer(3 * time.Second)
	draining := true
	for draining {
		select {
		case msg := <-sub2.Messages():
			reprocessed++
			msg.Ack()
		case <-drain.C:
			draining = false
		}
	}
	if reprocessed != 0 {
		t.Fatalf("dedup across restart failed: %d reprocessed", reprocessed)
	}
	c2.DeleteStream(ctx, stream)
}

// TestAckStoreEnvOverrideAgainstBroker: an operator can relocate the store with
// ARBITRO_ACKSTORE_DIR alone, no code change.
func TestAckStoreEnvOverrideAgainstBroker(t *testing.T) {
	ctx := context.Background()
	walDir := filepath.Join(t.TempDir(), "env-located")
	t.Setenv(ackstore.EnvDir, walDir)

	resolved, err := arbitro.DefaultAckStoreDir()
	if err != nil {
		t.Fatalf("DefaultAckStoreDir: %v", err)
	}
	if resolved != walDir {
		t.Fatalf("DefaultAckStoreDir = %q, want %q", resolved, walDir)
	}

	c, err := arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckStoreDir(""), // "" == platform default == the env value
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	if _, err := os.Stat(filepath.Join(walDir, "ackstore.log")); err != nil {
		t.Fatalf("the env override must decide the location: %v", err)
	}
}

// TestAckStoreTwoClientsOneDirRefused: two live clients pointed at one store
// directory must be refused, not silently allowed to interleave writes into one
// log (which would misattribute records after a restart and skip real work).
func TestAckStoreTwoClientsOneDirRefused(t *testing.T) {
	ctx := context.Background()
	walDir := filepath.Join(t.TempDir(), "shared")

	c1, err := arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckStoreDir(walDir),
	)
	if err != nil {
		t.Fatalf("connect 1: %v", err)
	}

	_, err = arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckStoreDir(walDir),
	)
	if !errors.Is(err, arbitro.ErrAckStoreLocked) {
		t.Fatalf("second client must be refused with ErrAckStoreLocked, got: %v", err)
	}
	if !strings.Contains(err.Error(), walDir) {
		t.Fatalf("the error must name the directory, got: %v", err)
	}

	// After the first client closes, the directory is reusable.
	if err := c1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}
	c2, err := arbitro.Connect(ctx, brokerAddr(),
		arbitro.WithTimeout(5*time.Second),
		arbitro.WithAckStoreDir(walDir),
	)
	if err != nil {
		t.Fatalf("directory must be free after close: %v", err)
	}
	c2.Close()
}
