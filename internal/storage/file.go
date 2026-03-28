package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/ll"
	"golang.org/x/exp/mmap"
)

var (
	ErrClosed    = errors.New("storage: closed")
	ErrShortRead = errors.New("storage: short read")
	ErrCorrupt   = errors.New("storage: corrupt record")
)

// File manages file operations with mmap support and optional compression.
// The mmap is a read-performance optimisation only. The authoritative source
// for all reads is the underlying *os.File — mmap is used when available and
// current; the file is always the fallback.
type File struct {
	file   *os.File
	mmap   atomic.Pointer[mmap.ReaderAt]
	mmapMu sync.RWMutex
	path   string

	logicalSize   atomic.Int64
	allocatedSize atomic.Int64

	mu     sync.RWMutex
	closed atomic.Bool

	compressor    *Compressor
	logger        *ll.Logger
	retryAttempts int
	retryBackoff  time.Duration

	stopRetry chan struct{}
	wg        sync.WaitGroup
}

func NewFile(cfg *FileConfig) (*File, error) {
	if cfg == nil {
		return nil, errors.New("storage: config is nil")
	}

	f, err := os.OpenFile(cfg.Path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	fileSize := info.Size()
	file := &File{
		file:          f,
		path:          cfg.Path,
		compressor:    cfg.Compressor,
		logger:        cfg.Logger,
		retryAttempts: cfg.RetryAttempts,
		retryBackoff:  cfg.RetryBackoff,
		stopRetry:     make(chan struct{}),
	}

	if file.logger == nil {
		file.logger = ll.New("storage").Disable()
	}

	if fileSize == 0 && cfg.InitialSize > 0 {
		if err := f.Truncate(cfg.InitialSize); err != nil {
			f.Close()
			return nil, err
		}
		file.allocatedSize.Store(cfg.InitialSize)
		file.logicalSize.Store(0)
	} else {
		file.allocatedSize.Store(fileSize)
		file.logicalSize.Store(fileSize)
	}

	// Initial mmap — best-effort; reads fall back to os.File if unavailable.
	_ = file.remap()

	file.wg.Add(1)
	go file.remapLoop()

	return file, nil
}

// remap replaces the mmap reader. Must NOT be called with mmapMu held.
func (f *File) remap() error {
	reader, err := mmap.Open(f.path)
	if err != nil {
		return err
	}
	f.mmapMu.Lock()
	if old := f.mmap.Load(); old != nil {
		old.Close()
	}
	f.mmap.Store(reader)
	f.mmapMu.Unlock()
	return nil
}

// remapLoop periodically refreshes the mmap so reads eventually benefit from it.
func (f *File) remapLoop() {
	defer f.wg.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !f.closed.Load() {
				_ = f.remap()
			}
		case <-f.stopRetry:
			return
		}
	}
}

// readRecord reads a record from any io.ReaderAt at the given offset.
func (f *File) readRecord(reader io.ReaderAt, offset int64) (key, value []byte, tombstone bool, n int64, err error) {
	var keyLenBuf [4]byte
	if _, err = reader.ReadAt(keyLenBuf[:], offset); err != nil {
		return
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf[:])
	offset += 4
	n = 4

	if keyLen == 0 {
		var valLenBuf [4]byte
		if _, err = reader.ReadAt(valLenBuf[:], offset); err != nil {
			return nil, nil, false, n, err
		}
		valLen := binary.BigEndian.Uint32(valLenBuf[:])
		if valLen == 0 {
			return nil, nil, true, n + 4, nil
		}
		return nil, nil, false, n, ErrCorrupt
	}

	key = make([]byte, keyLen)
	if _, err = reader.ReadAt(key, offset); err != nil {
		return
	}
	offset += int64(keyLen)
	n += int64(keyLen)

	var valLenBuf [4]byte
	if _, err = reader.ReadAt(valLenBuf[:], offset); err != nil {
		return nil, nil, false, n, err
	}
	valLen := binary.BigEndian.Uint32(valLenBuf[:])
	offset += 4
	n += 4

	if valLen == 0 {
		return key, nil, true, n, nil
	}

	var flagBuf [1]byte
	if _, err = reader.ReadAt(flagBuf[:], offset); err != nil {
		return nil, nil, false, n, err
	}
	compressed := flagBuf[0] == 1
	offset += 1
	n += 1

	value = make([]byte, valLen)
	if _, err = reader.ReadAt(value, offset); err != nil {
		return nil, nil, false, n, err
	}
	n += int64(valLen)

	if compressed {
		if f.compressor == nil {
			return nil, nil, false, n, ErrCorrupt
		}
		value, err = f.compressor.Decode(value)
		if err != nil {
			return nil, nil, false, n, err
		}
	}

	return key, value, false, n, nil
}

// ReadAt reads a value record at offset, validating the key.
// Tries mmap first; falls back to direct file read.
func (f *File) ReadAt(off int64, key []byte) ([]byte, error) {
	if f.closed.Load() {
		return nil, ErrClosed
	}

	// Try mmap (fast path).
	f.mmapMu.RLock()
	mm := f.mmap.Load()
	if mm != nil {
		rKey, rVal, tombstone, _, err := f.readRecord(mm, off)
		f.mmapMu.RUnlock()
		if err == nil && !tombstone && bytes.Equal(rKey, key) {
			return rVal, nil
		}
	} else {
		f.mmapMu.RUnlock()
	}

	// Fallback: direct file read — always reflects current written data.
	rKey, rVal, tombstone, _, err := f.readRecord(f.file, off)
	if err != nil {
		return nil, err
	}
	if tombstone {
		return nil, ErrCorrupt
	}
	if !bytes.Equal(rKey, key) {
		return nil, ErrCorrupt
	}
	return rVal, nil
}

// ReadRecordAt reads a full record at offset.
// Tries mmap first; falls back to direct file read.
func (f *File) ReadRecordAt(offset int64) (key, value []byte, tombstone bool, n int64, err error) {
	if f.closed.Load() {
		return nil, nil, false, 0, ErrClosed
	}

	f.mmapMu.RLock()
	mm := f.mmap.Load()
	if mm != nil {
		k, v, ts, sz, e := f.readRecord(mm, offset)
		f.mmapMu.RUnlock()
		if e == nil {
			return k, v, ts, sz, nil
		}
	} else {
		f.mmapMu.RUnlock()
	}

	return f.readRecord(f.file, offset)
}

// Write appends p at the current logical offset.
func (f *File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed.Load() {
		return 0, ErrClosed
	}
	off := f.logicalSize.Load()
	n, err := f.file.WriteAt(p, off)
	if err != nil {
		return n, err
	}
	f.logicalSize.Add(int64(n))
	f.allocatedSize.Add(int64(n))
	return n, nil
}

// WriteRecord serialises and appends a key-value record.
// Returns the offset at which the record starts.
func (f *File) WriteRecord(key, value []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed.Load() {
		return 0, ErrClosed
	}

	off := f.logicalSize.Load()
	keyLen := len(key)
	valLen := len(value)
	compressed := byte(0)

	if f.compressor != nil && valLen >= f.compressor.Threshold() {
		if enc, err := f.compressor.Encode(value); err == nil {
			value = enc
			valLen = len(value)
			compressed = 1
		}
	}

	totalSize := 4 + keyLen + 4 + 1 + valLen
	buf := make([]byte, totalSize)
	pos := 0
	binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
	pos += 4
	copy(buf[pos:], key)
	pos += keyLen
	binary.BigEndian.PutUint32(buf[pos:], uint32(valLen))
	pos += 4
	buf[pos] = compressed
	pos++
	copy(buf[pos:], value)

	n, err := f.file.WriteAt(buf, off)
	if err != nil {
		return 0, err
	}
	f.logicalSize.Add(int64(n))
	f.allocatedSize.Add(int64(n))
	return off, nil
}

// WriteTombstone writes a deletion marker for key.
func (f *File) WriteTombstone(key []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed.Load() {
		return ErrClosed
	}

	off := f.logicalSize.Load()
	keyLen := len(key)
	buf := make([]byte, 4+keyLen+4)
	binary.BigEndian.PutUint32(buf[0:], uint32(keyLen))
	copy(buf[4:], key)
	binary.BigEndian.PutUint32(buf[4+keyLen:], 0)

	n, err := f.file.WriteAt(buf, off)
	if err != nil {
		return err
	}
	f.logicalSize.Add(int64(n))
	f.allocatedSize.Add(int64(n))
	return nil
}

// Sync flushes the OS page cache to disk.
func (f *File) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed.Load() {
		return ErrClosed
	}
	return f.file.Sync()
}

// Size returns the allocated file size.
func (f *File) Size() int64 {
	return f.allocatedSize.Load()
}

// LogicalSize returns the end of written record data.
func (f *File) LogicalSize() int64 {
	return f.logicalSize.Load()
}

// Truncate truncates the file and remaps immediately.
func (f *File) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed.Load() {
		return ErrClosed
	}
	if err := f.file.Truncate(size); err != nil {
		return err
	}
	f.allocatedSize.Store(size)
	if f.logicalSize.Load() > size {
		f.logicalSize.Store(size)
	}
	return f.remap()
}

// IsEmpty returns true if no records have been written.
func (f *File) IsEmpty() bool {
	if f.closed.Load() {
		return true
	}
	if f.logicalSize.Load() == 0 {
		return true
	}
	// Try to read the first record directly from the file.
	_, _, _, _, err := f.readRecord(f.file, 0)
	return err != nil
}

// Close stops background goroutines and releases resources.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return nil
	}
	f.closed.Store(true)

	close(f.stopRetry)
	f.wg.Wait()

	f.mmapMu.Lock()
	if mm := f.mmap.Load(); mm != nil {
		mm.Close()
	}
	f.mmapMu.Unlock()

	return f.file.Close()
}

// Compressor returns the compressor instance.
func (f *File) Compressor() *Compressor {
	return f.compressor
}
