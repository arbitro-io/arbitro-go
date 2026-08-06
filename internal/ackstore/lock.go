package ackstore

import (
	"errors"
	"fmt"
	"path/filepath"
)

// LockFile is the name of the single-writer lock inside a store directory.
const LockFile = "ackstore.lock"

// ErrLocked reports that another live process already holds the single-writer
// lock on a store directory.
//
// # Why this is detected rather than documented away
//
// ackstore.log is one append-only file whose in-memory symbol table is
// authoritative. Two processes sharing it do not merely interleave bytes: each
// allocates slot ids from its own nextID counter, so process B writes
// Register(slot 0 = orders/worker) into a log where process A already means
// something else by slot 0. After a restart, replay attributes A's Record
// frames to B's slot — and a false Seen() hit is a message whose handler never
// runs. That is silent, unrecoverable work loss, not a corrupt-file error the
// next open would notice.
//
// Detection costs one file handle and one syscall at open, so it is detected.
// It matters more now that an unconfigured store resolves to a shared default
// directory where two services on one host would otherwise collide by accident.
//
// # Mechanism
//
// An OS advisory lock held on <dir>/ackstore.lock for the WAL's lifetime:
// flock(LOCK_EX|LOCK_NB) on unix, an exclusive (share mode 0) open on Windows.
// Both are released by the kernel when the handle closes — including on SIGKILL
// — so a crashed process never leaves the store permanently unopenable, which a
// plain O_EXCL pid file would. The guarantee is machine-local: a WAL on a
// network filesystem shared between hosts is not a supported configuration.
var ErrLocked = errors.New("ackstore: store directory is already open by another process (a WAL directory allows exactly one writer)")

// lockPath is the lock file for dir.
func lockPath(dir string) string { return filepath.Join(dir, LockFile) }

// acquireDirLock takes the exclusive lock on dir, which must already exist.
func acquireDirLock(dir string) (*dirLock, error) {
	l, err := lockDir(dir)
	if err != nil {
		if errors.Is(err, ErrLocked) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
		}
		return nil, fmt.Errorf("ackstore: store directory %s: cannot open %s: %w", dir, LockFile, err)
	}
	return l, nil
}
