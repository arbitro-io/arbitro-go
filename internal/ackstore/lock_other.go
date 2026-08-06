//go:build !windows && !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly

package ackstore

import "os"

// Platforms with neither flock nor Windows share modes (js/wasm, plan9,
// solaris, aix). The lock file is still created so the directory layout is
// identical, but single-writer discipline is the caller's responsibility —
// documented on Config.Dir.

type dirLock struct{ f *os.File }

func lockDir(dir string) (*dirLock, error) {
	f, err := os.OpenFile(lockPath(dir), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &dirLock{f: f}, nil
}

func (l *dirLock) release() {
	if l == nil || l.f == nil {
		return
	}
	_ = l.f.Close()
	l.f = nil
}
