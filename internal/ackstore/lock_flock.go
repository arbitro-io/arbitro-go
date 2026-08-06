//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package ackstore

import (
	"os"
	"syscall"
)

// dirLock holds the open lock file. Closing it releases the flock.
type dirLock struct{ f *os.File }

func lockDir(dir string) (*dirLock, error) {
	f, err := os.OpenFile(lockPath(dir), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		// EWOULDBLOCK == EAGAIN on Linux; both spellings are checked so
		// "someone else has it" is never reported as a generic io error.
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return nil, ErrLocked
		}
		return nil, err
	}
	return &dirLock{f: f}, nil
}

func (l *dirLock) release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
	l.f = nil
}
