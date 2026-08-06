package ackstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// EnvDir is the environment variable that overrides the default WAL directory.
// The Rust and TS clients honour the same name, so one operator setting moves
// every language's store together.
const EnvDir = "ARBITRO_ACKSTORE_DIR"

// ErrNoDefaultDir is returned when no directory was configured and the platform
// exposes no home/state directory to default to (a container with no HOME, a
// bare systemd unit). Callers must then set a path explicitly.
var ErrNoDefaultDir = errors.New("ackstore: cannot resolve a default store directory")

// DefaultDir reports where the WAL lives when no explicit directory is
// configured. Resolution order:
//
//  1. $ARBITRO_ACKSTORE_DIR.
//  2. The platform state directory, <state>/arbitro/ackstore:
//     Linux/BSD  $XDG_STATE_HOME, else $HOME/.local/state
//     macOS      $HOME/Library/Application Support
//     Windows    %LOCALAPPDATA%
//  3. Otherwise ErrNoDefaultDir.
//
// There is deliberately no fallback to the working directory or to a temp dir.
// A cwd-relative store silently moves when systemd, a container entrypoint, or
// a cron job starts the process from somewhere else; a temp store is wiped on
// reboot. Both look perfectly healthy while re-running work that was already
// done — exactly what this store exists to prevent — so an explicit error
// naming the two ways to fix it is the better failure.
func DefaultDir() (string, error) {
	if d := envDirOverride(); d != "" {
		return d, nil
	}
	base, err := stateBaseDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "arbitro", "ackstore"), nil
}

// envDirOverride returns the ARBITRO_ACKSTORE_DIR value, treating an
// all-whitespace value as unset. The value itself is never trimmed — trailing
// spaces are legal in a POSIX path.
func envDirOverride() string {
	v := os.Getenv(EnvDir)
	if strings.TrimSpace(v) == "" {
		return ""
	}
	return v
}

func stateBaseDir() (string, error) {
	switch runtime.GOOS {
	case "windows":
		if d := os.Getenv("LOCALAPPDATA"); d != "" {
			return d, nil
		}
		return "", fmt.Errorf("%w (%%LOCALAPPDATA%% is not set); set the store dir explicitly or export %s", ErrNoDefaultDir, EnvDir)
	case "darwin", "ios":
		home, err := homeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, "Library", "Application Support"), nil
	default:
		// XDG requires a relative value to be ignored as if unset.
		if d := os.Getenv("XDG_STATE_HOME"); filepath.IsAbs(d) {
			return d, nil
		}
		home, err := homeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, ".local", "state"), nil
	}
}

func homeDir() (string, error) {
	if h := os.Getenv("HOME"); h != "" {
		return h, nil
	}
	return "", fmt.Errorf("%w ($HOME is not set); set the store dir explicitly or export %s", ErrNoDefaultDir, EnvDir)
}

// prepareDir makes dir usable as a store directory, or explains precisely why
// it is not. Distinguishes the failure modes that would otherwise arrive as one
// opaque *PathError: the path is a regular file, a parent component is a file,
// or the process cannot create it.
func prepareDir(dir string) error {
	fi, err := os.Stat(dir)
	switch {
	case err == nil && fi.IsDir():
		return nil
	case err == nil:
		return fmt.Errorf("ackstore: store directory %s: path exists but is not a directory", dir)
	case os.IsNotExist(err):
		if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
			return fmt.Errorf("ackstore: store directory %s: cannot create directory: %w", dir, mkErr)
		}
		return nil
	default:
		return fmt.Errorf("ackstore: store directory %s: cannot stat: %w", dir, err)
	}
}
