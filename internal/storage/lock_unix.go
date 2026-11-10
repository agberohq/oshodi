//go:build !windows

package storage

import (
	"github.com/agberohq/oshodi/internal/consts"
	"io"
	"os"
	"syscall"
)

// AcquireFileLock takes an exclusive, non-blocking advisory lock on the
// database lock sidecar (path + FilenameSuffixLock), preventing two
// processes from opening the same database file. Nil closer releases it.
// Unsupported platforms (see lock_windows.go) return a no-op closer.
func AcquireFileLock(path string) (io.Closer, error) {
	f, err := os.OpenFile(path+FilenameSuffixLock, os.O_CREATE|os.O_RDWR, consts.FileMode)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, ErrFileBusy
	}
	return &fileLock{f: f}, nil
}

// fileLock is a closer that holds a flock for the process lifetime.
type fileLock struct {
	f *os.File
}

func (l *fileLock) Close() error {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}
