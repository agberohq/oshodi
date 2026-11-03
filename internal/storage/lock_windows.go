//go:build windows

package storage

import (
	"io"
	"os"
)

// AcquireFileLock is a no-op on Windows (see lock_unix.go for the contract).
// Windows file locking is wired separately once needed.
func AcquireFileLock(path string) (io.Closer, error) {
	f, err := os.OpenFile(path+FilenameSuffixLock, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return &fileLock{f: f}, nil
}

type fileLock struct {
	f *os.File
}

func (l *fileLock) Close() error { return l.f.Close() }
