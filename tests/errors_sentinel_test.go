package tests

import (
	"errors"
	"fmt"
	"testing"

	"github.com/arbitro-io/arbitro-go"
)

// TestErrQueueFullIsReachableFromOutside is a compile-and-identity guard, not
// a behaviour test — it needs no broker, which is why it carries no
// integration tag.
//
// The sentinel returned by the publish paths is created in internal/conn, and
// nothing outside this module can import that package. The root re-export is
// the only handle an external caller has, so this file exists to fail loudly
// if it is ever unexported, renamed, or turned into a fresh errors.New that no
// longer matches what the write queue actually returns. Same guard the
// integration suite gives ErrAckStoreLocked (ackstore_storage_path_test.go).
func TestErrQueueFullIsReachableFromOutside(t *testing.T) {
	if arbitro.ErrQueueFull == nil {
		t.Fatal("arbitro.ErrQueueFull must be a usable sentinel, got nil")
	}

	// The message is part of the contract: it is what a caller logs when it
	// sheds a message, and what the README quotes.
	if got, want := arbitro.ErrQueueFull.Error(), "arbitro: outgoing queue full"; got != want {
		t.Fatalf("sentinel message drifted: got %q, want %q", got, want)
	}

	// Callers rarely see the bare sentinel — it arrives wrapped by whatever
	// layer annotated it, so errors.Is has to survive %w.
	wrapped := fmt.Errorf("publish orders.created: %w", arbitro.ErrQueueFull)
	if !errors.Is(wrapped, arbitro.ErrQueueFull) {
		t.Fatalf("errors.Is must match through a %%w wrap, got: %v", wrapped)
	}

	// And it must not swallow the other re-exported sentinel: the whole point
	// of this error is telling a transient condition apart from a terminal one.
	if errors.Is(arbitro.ErrAckStoreLocked, arbitro.ErrQueueFull) {
		t.Fatal("ErrQueueFull must not match unrelated sentinels")
	}
}
