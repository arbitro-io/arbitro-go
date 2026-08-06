package arbitro

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/arbitro-io/arbitro-go/internal/ackstore"
)

// applyOpts runs the option functions the way Connect does, so these tests
// exercise the real option surface without needing a broker. Note that
// WithAckStoreDir/WithAckPersistence open the WAL eagerly, which is what lets
// Connect fail before dialling.
func applyOpts(t *testing.T, opts ...Option) clientOptions {
	t.Helper()
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}
	t.Cleanup(func() {
		if o.ackStore != nil {
			_ = o.ackStore.Close()
		}
	})
	return o
}

func TestDefaultOptionsOpenNoStoreOnDisk(t *testing.T) {
	o := applyOpts(t)
	if o.ackStore != nil {
		t.Fatal("default options must not open a durable store")
	}
	if !o.ackDedup {
		t.Fatal("default options still dedup in memory")
	}
}

func TestWithAckStoreDirUsesTheGivenPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "store") // also proves mkdir -p
	t.Setenv(ackstore.EnvDir, filepath.Join(t.TempDir(), "should-be-ignored"))

	o := applyOpts(t, WithAckStoreDir(dir))
	if o.ackStoreErr != nil {
		t.Fatalf("WithAckStoreDir: %v", o.ackStoreErr)
	}
	if o.ackStore == nil || !o.ackDedup {
		t.Fatal("WithAckStoreDir must enable a durable store")
	}
	if _, err := os.Stat(filepath.Join(dir, "ackstore.log")); err != nil {
		t.Fatalf("the WAL must land at the configured path, not somewhere derived: %v", err)
	}
}

func TestWithAckStoreDirEmptyUsesDefaultDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "defaulted")
	t.Setenv(ackstore.EnvDir, dir)

	got, err := DefaultAckStoreDir()
	if err != nil {
		t.Fatalf("DefaultAckStoreDir: %v", err)
	}
	if got != dir {
		t.Fatalf("DefaultAckStoreDir = %q, want %q", got, dir)
	}

	o := applyOpts(t, WithAckStoreDir(""))
	if o.ackStoreErr != nil {
		t.Fatalf("WithAckStoreDir(\"\"): %v", o.ackStoreErr)
	}
	if _, err := os.Stat(filepath.Join(dir, "ackstore.log")); err != nil {
		t.Fatalf("empty dir must resolve to the default path: %v", err)
	}
}

func TestWithAckPersistenceCarriesTTLAndFsync(t *testing.T) {
	dir := t.TempDir()
	o := applyOpts(t, WithAckPersistence(dir, 0, true))
	if o.ackStoreErr != nil {
		t.Fatalf("WithAckPersistence: %v", o.ackStoreErr)
	}
	w, ok := o.ackStore.(*ackstore.WAL)
	if !ok {
		t.Fatalf("expected a *ackstore.WAL, got %T", o.ackStore)
	}
	if w.Dir() != dir {
		t.Fatalf("Dir() = %q, want %q", w.Dir(), dir)
	}
}

func TestConnectRejectsAnUnusableStoreDirBeforeDialling(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Address is deliberately unroutable: if Connect dialled first, this would
	// time out with a connection error instead of the configuration error.
	_, err := Connect(context.Background(), "127.0.0.1:1", WithAckStoreDir(file))
	if err == nil {
		t.Fatal("expected Connect to reject a store dir that is a regular file")
	}
	if !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("Connect must surface the store-dir error, got: %v", err)
	}
}

func TestConnectRejectsADirectoryAlreadyHeldByAnotherWriter(t *testing.T) {
	dir := t.TempDir()
	first := applyOpts(t, WithAckStoreDir(dir))
	if first.ackStoreErr != nil {
		t.Fatalf("first open: %v", first.ackStoreErr)
	}

	second := applyOpts(t, WithAckStoreDir(dir))
	if !errors.Is(second.ackStoreErr, ErrAckStoreLocked) {
		t.Fatalf("a second writer on one dir must be refused, got: %v", second.ackStoreErr)
	}
	if !strings.Contains(second.ackStoreErr.Error(), dir) {
		t.Fatalf("the error must name the directory, got: %v", second.ackStoreErr)
	}

	// And Connect propagates it rather than dialling.
	_, err := Connect(context.Background(), "127.0.0.1:1", WithAckStoreDir(dir))
	if !errors.Is(err, ErrAckStoreLocked) {
		t.Fatalf("Connect must surface ErrAckStoreLocked, got: %v", err)
	}
}

func TestFailedDialReleasesTheStoreDirectory(t *testing.T) {
	dir := t.TempDir()
	// Nothing listens on port 1; the dial fails after the WAL was opened.
	if _, err := Connect(context.Background(), "127.0.0.1:1", WithAckStoreDir(dir)); err == nil {
		t.Fatal("expected the dial to fail")
	}
	// The directory must be free again — otherwise a retry loop would turn one
	// transient network failure into a permanent ErrLocked.
	w, err := ackstore.OpenWAL(ackstore.Config{Dir: dir})
	if err != nil {
		t.Fatalf("directory must be released after a failed dial, got: %v", err)
	}
	_ = w.Close()
}

func TestWithoutAckDedupOpensNothing(t *testing.T) {
	o := applyOpts(t, WithoutAckDedup())
	if o.ackStore != nil || o.ackDedup {
		t.Fatal("WithoutAckDedup must leave no store")
	}
}

// A superseding option must close the WAL it replaces. Otherwise the discarded
// store keeps its directory lock for the whole process lifetime and the next
// Connect on that directory fails with ErrLocked for no reason.
func TestSupersedingOptionReleasesTheReplacedStore(t *testing.T) {
	cases := []struct {
		name string
		last Option
	}{
		{"WithoutAckDedup", WithoutAckDedup()},
		{"WithAckStore(nil)", WithAckStore(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			o := applyOpts(t, WithAckStoreDir(dir), tc.last)
			if o.ackStore != nil {
				t.Fatalf("%s must leave no store", tc.name)
			}
			w, err := ackstore.OpenWAL(ackstore.Config{Dir: dir})
			if err != nil {
				t.Fatalf("the replaced store must have released %s: %v", dir, err)
			}
			_ = w.Close()
		})
	}
}

func TestReconfiguringTheDirReleasesThePreviousOne(t *testing.T) {
	first := t.TempDir()
	second := t.TempDir()
	o := applyOpts(t, WithAckStoreDir(first), WithAckStoreDir(second))
	if o.ackStoreErr != nil {
		t.Fatalf("second WithAckStoreDir: %v", o.ackStoreErr)
	}
	w, ok := o.ackStore.(*ackstore.WAL)
	if !ok || w.Dir() != second {
		t.Fatalf("the last option must win, got %v", o.ackStore)
	}
	reopened, err := ackstore.OpenWAL(ackstore.Config{Dir: first})
	if err != nil {
		t.Fatalf("the superseded directory %s must be released: %v", first, err)
	}
	_ = reopened.Close()
}
