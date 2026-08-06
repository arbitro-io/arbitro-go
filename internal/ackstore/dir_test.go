package ackstore

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// --- default directory resolution ---

func TestDefaultDirUsesEnvOverride(t *testing.T) {
	want := filepath.Join(t.TempDir(), "custom store")
	t.Setenv(EnvDir, want)

	got, err := DefaultDir()
	if err != nil {
		t.Fatalf("DefaultDir: %v", err)
	}
	if got != want {
		t.Fatalf("DefaultDir = %q, want %q", got, want)
	}
}

func TestDefaultDirIgnoresBlankEnvOverride(t *testing.T) {
	t.Setenv(EnvDir, "   ")
	got, err := DefaultDir()
	if err != nil {
		// Acceptable in an environment with no HOME — but it must be the
		// explicit error, never a silent cwd/temp fallback.
		if !errors.Is(err, ErrNoDefaultDir) {
			t.Fatalf("want ErrNoDefaultDir, got %v", err)
		}
		return
	}
	if strings.TrimSpace(got) != got || got == "" {
		t.Fatalf("blank override must be ignored, got %q", got)
	}
}

func TestDefaultDirIsAbsoluteAndNamespaced(t *testing.T) {
	t.Setenv(EnvDir, "")
	got, err := DefaultDir()
	if err != nil {
		if !errors.Is(err, ErrNoDefaultDir) {
			t.Fatalf("want ErrNoDefaultDir, got %v", err)
		}
		return
	}
	if !filepath.IsAbs(got) {
		t.Fatalf("default dir must be absolute, got %q", got)
	}
	if want := filepath.Join("arbitro", "ackstore"); !strings.HasSuffix(got, want) {
		t.Fatalf("default dir %q must end with %q", got, want)
	}
	// Never the working directory: a service started from elsewhere would
	// silently get a different store.
	if cwd, err := os.Getwd(); err == nil && strings.HasPrefix(got, cwd) {
		t.Fatalf("default dir %q must not live under the cwd %q", got, cwd)
	}
}

func TestDefaultDirHonoursXDGStateHome(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip("XDG_STATE_HOME only applies to Linux/BSD")
	}
	base := t.TempDir()
	t.Setenv(EnvDir, "")
	t.Setenv("XDG_STATE_HOME", base)

	got, err := DefaultDir()
	if err != nil {
		t.Fatalf("DefaultDir: %v", err)
	}
	if want := filepath.Join(base, "arbitro", "ackstore"); got != want {
		t.Fatalf("DefaultDir = %q, want %q", got, want)
	}

	// A relative XDG value must be ignored (per the XDG spec), falling back to
	// $HOME/.local/state rather than resolving against the cwd.
	t.Setenv("XDG_STATE_HOME", "relative/path")
	t.Setenv("HOME", base)
	got, err = DefaultDir()
	if err != nil {
		t.Fatalf("DefaultDir: %v", err)
	}
	if want := filepath.Join(base, ".local", "state", "arbitro", "ackstore"); got != want {
		t.Fatalf("relative XDG_STATE_HOME must be ignored: got %q, want %q", got, want)
	}
}

// --- OpenWAL: explicit dir, defaulted dir ---

func TestOpenWALUsesExplicitDirVerbatim(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "store") // also proves mkdir -p
	t.Setenv(EnvDir, filepath.Join(t.TempDir(), "should-be-ignored"))

	w, err := OpenWAL(Config{Dir: dir})
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	if w.Dir() != dir {
		t.Fatalf("Dir() = %q, want %q (explicit dir must outrank %s)", w.Dir(), dir, EnvDir)
	}
	if _, err := os.Stat(filepath.Join(dir, "ackstore.log")); err != nil {
		t.Fatalf("log must materialize at the configured path: %v", err)
	}
}

func TestOpenWALEmptyDirResolvesDefault(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "defaulted")
	t.Setenv(EnvDir, dir)

	w, err := OpenWAL(Config{})
	if err != nil {
		t.Fatalf("OpenWAL with empty Dir: %v", err)
	}
	defer w.Close()

	if w.Dir() != dir {
		t.Fatalf("Dir() = %q, want %q", w.Dir(), dir)
	}
	if _, err := os.Stat(filepath.Join(dir, "ackstore.log")); err != nil {
		t.Fatalf("stat log: %v", err)
	}
}

// --- failure modes ---

func TestOpenWALRejectsDirThatIsARegularFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := OpenWAL(Config{Dir: file})
	if err == nil {
		t.Fatal("expected an error when the store dir is a regular file")
	}
	if !strings.Contains(err.Error(), "not a directory") || !strings.Contains(err.Error(), file) {
		t.Fatalf("error must name the path and the problem, got: %v", err)
	}
}

func TestOpenWALRejectsDirUnderARegularFile(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := OpenWAL(Config{Dir: filepath.Join(blocker, "store")})
	if err == nil {
		t.Fatal("expected an error when a parent component is a file")
	}
	if !strings.Contains(err.Error(), "store directory") {
		t.Fatalf("error must be the store-directory error, got: %v", err)
	}
}

func TestOpenWALReportsUnwritableDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode bits do not govern writability on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root ignores the mode bits")
	}
	dir := filepath.Join(t.TempDir(), "ro")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	_, err := OpenWAL(Config{Dir: dir})
	if err == nil {
		t.Fatal("expected an error for a read-only store dir")
	}
	if !strings.Contains(err.Error(), dir) || !strings.Contains(err.Error(), LockFile) {
		t.Fatalf("error must name the path and the lock file, got: %v", err)
	}
}

// --- single writer per directory ---

func TestSecondWriterIsRefusedThenAllowedAfterClose(t *testing.T) {
	dir := t.TempDir()

	first, err := OpenWAL(Config{Dir: dir})
	if err != nil {
		t.Fatalf("first OpenWAL: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, LockFile)); err != nil {
		t.Fatalf("open must create the lock file: %v", err)
	}

	_, err = OpenWAL(Config{Dir: dir})
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second OpenWAL must fail with ErrLocked, got: %v", err)
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("ErrLocked must name the directory, got: %v", err)
	}

	// Closing hands the directory over cleanly.
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	second, err := OpenWAL(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSeparateDirsDoNotContend(t *testing.T) {
	base := t.TempDir()
	a, err := OpenWAL(Config{Dir: filepath.Join(base, "a")})
	if err != nil {
		t.Fatalf("open a: %v", err)
	}
	defer a.Close()
	b, err := OpenWAL(Config{Dir: filepath.Join(base, "b")})
	if err != nil {
		t.Fatalf("open b (a distinct dir must be usable concurrently): %v", err)
	}
	defer b.Close()

	mustSlot(t, a, "s", "c").Record(1)
	mustSlot(t, b, "s", "c").Record(1)
}
