//go:build windows

package ackstore

import (
	"os"
	"syscall"
)

// errSharingViolation is ERROR_SHARING_VIOLATION: another handle already holds
// the file with an incompatible share mode. Spelled out here rather than pulled
// from golang.org/x/sys/windows to keep this module dependency-free.
const errSharingViolation = syscall.Errno(32)

// dirLock holds the exclusive handle. Closing it releases the lock.
type dirLock struct{ f *os.File }

func lockDir(dir string) (*dirLock, error) {
	p, err := syscall.UTF16PtrFromString(lockPath(dir))
	if err != nil {
		return nil, err
	}
	// Share mode 0: no other process may open this file at all. Windows
	// releases the handle (and therefore the lock) when the process exits.
	h, err := syscall.CreateFile(
		p,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0,
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		if err == errSharingViolation {
			return nil, ErrLocked
		}
		return nil, err
	}
	return &dirLock{f: os.NewFile(uintptr(h), lockPath(dir))}, nil
}

func (l *dirLock) release() {
	if l == nil || l.f == nil {
		return
	}
	_ = l.f.Close()
	l.f = nil
}
