package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"os"

	"github.com/alialaee/logfile"
)

var (
	ErrWALClosed   = errors.New("wal: closed")
	ErrKeyNotInWAL = errors.New("wal: key not found at offset")
)

// WAL provides fast durable writes via group commit.
// It wraps logfile for optimized SSD append-only writes.
type WAL struct {
	lf     *logfile.LogFile
	file   *os.File
	closed bool
}

// NewWAL creates or opens a WAL file at the given path.
func NewWAL(path string, maxBufSize int) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	lf, err := logfile.New(f, info.Size(), maxBufSize, true)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &WAL{
		lf:   lf,
		file: f,
	}, nil
}

// Write appends a key-value pair to the WAL using group commit.
// Returns the WAL offset where the record starts (for later reading).
func (w *WAL) Write(ctx context.Context, key, value []byte) (int64, error) {
	if w.closed {
		return 0, ErrWALClosed
	}

	// Encode: keylen(4) + key + valuelen(4) + value
	record := make([]byte, 4+len(key)+4+len(value))
	binary.BigEndian.PutUint32(record[0:4], uint32(len(key)))
	copy(record[4:4+len(key)], key)
	binary.BigEndian.PutUint32(record[4+len(key):4+len(key)+4], uint32(len(value)))
	copy(record[4+len(key)+4:], value)

	return w.lf.Write(ctx, record)
}

// Read retrieves a record from the WAL at the given offset.
// Returns the key and value stored at that offset.
func (w *WAL) Read(ctx context.Context, offset int64) (key, value []byte, err error) {
	if w.closed {
		return nil, nil, ErrWALClosed
	}

	reader := logfile.NewReader(w.file, offset)
	data, err := reader.ReadNext(nil)
	if err != nil {
		return nil, nil, err
	}

	// Decode
	if len(data) < 8 {
		return nil, nil, errors.New("wal: corrupted record")
	}
	keyLen := binary.BigEndian.Uint32(data[0:4])
	if uint32(len(data)) < 8+keyLen {
		return nil, nil, errors.New("wal: corrupted record")
	}
	key = data[4 : 4+keyLen]
	valueLen := binary.BigEndian.Uint32(data[4+keyLen : 4+keyLen+4])
	if uint32(len(data)) < 8+keyLen+valueLen {
		return nil, nil, errors.New("wal: corrupted record")
	}
	value = data[4+keyLen+4 : 4+keyLen+4+valueLen]
	return key, value, nil
}

// NewReader creates a new reader starting at the given offset for recovery.
func (w *WAL) NewReader(offset int64) *logfile.Reader {
	return logfile.NewReader(w.file, offset)
}

// Flush forces all buffered writes to disk.
func (w *WAL) Flush(ctx context.Context) error {
	if w.closed {
		return ErrWALClosed
	}
	return w.lf.Flush(ctx)
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	err := w.lf.Close()
	if cerr := w.file.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// FileSize returns the current WAL file size
func (w *WAL) FileSize() int64 {
	if w.closed {
		return 0
	}
	info, err := w.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}
