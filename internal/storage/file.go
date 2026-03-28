package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/mmap"
)

var (
	ErrClosed    = errors.New("storage: closed")
	ErrShortRead = errors.New("storage: short read")
	ErrCorrupt   = errors.New("storage: corrupt record")
)

// File manages file operations with mmap support and optional compression.
// Read operations are lock-free using atomic pointer to mmap data.
type File struct {
	file   *os.File
	mmap   atomic.Pointer[mmap.ReaderAt] // RCU pattern for mmap
	path   string
	size   atomic.Int64
	mu     sync.RWMutex // Only for write operations and mmap remapping
	closed atomic.Bool

	// Compression settings (nil = disabled)
	compressor        *Compressor
	compressThreshold int
}

// NewFile creates a new storage file.
func NewFile(path string, initialSize int64, bufferSize int, compressor *Compressor) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	size := info.Size()
	if size == 0 && initialSize > 0 {
		if err := f.Truncate(initialSize); err != nil {
			f.Close()
			return nil, err
		}
		size = initialSize
	}

	reader, err := mmap.Open(path)
	if err != nil {
		f.Close()
		return nil, err
	}

	file := &File{
		file:       f,
		path:       path,
		compressor: compressor,
		compressThreshold: func() int {
			if compressor != nil {
				return compressor.Threshold()
			}
			return 0
		}(),
	}
	file.mmap.Store(reader)
	file.size.Store(size)

	return file, nil
}

// ReadAt reads data at offset - LOCK-FREE path
func (f *File) ReadAt(off int64, key []byte) ([]byte, error) {
	if f.closed.Load() {
		return nil, ErrClosed
	}

	// Atomic load of mmap - no lock!
	reader := f.mmap.Load()
	if reader == nil {
		return nil, ErrClosed
	}

	// Read the full record
	readKey, value, tombstone, _, err := f.readRecord(reader, off)
	if err != nil {
		return nil, err
	}

	if tombstone {
		return nil, ErrCorrupt
	}

	// Verify key matches
	if !bytes.Equal(readKey, key) {
		return nil, ErrCorrupt
	}

	return value, nil
}

// WriteAt writes data at specific offset (requires write lock)
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return 0, ErrClosed
	}

	return f.file.WriteAt(p, off)
}

// Sync syncs the file to disk
func (f *File) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return ErrClosed
	}

	return f.file.Sync()
}

// Size returns the file size (lock-free)
func (f *File) Size() int64 {
	return f.size.Load()
}

// Truncate truncates the file
func (f *File) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return ErrClosed
	}

	if err := f.file.Truncate(size); err != nil {
		return err
	}

	// Remap mmap
	oldMmap := f.mmap.Load()
	if oldMmap != nil {
		oldMmap.Close()
	}

	reader, err := mmap.Open(f.path)
	if err != nil {
		return err
	}
	f.mmap.Store(reader)
	f.size.Store(size)

	return nil
}

// IsEmpty checks if file is empty
func (f *File) IsEmpty() bool {
	if f.closed.Load() {
		return true
	}

	reader := f.mmap.Load()
	if reader == nil {
		return true
	}

	// Check first few bytes
	buf := make([]byte, 1024)
	n, _ := reader.ReadAt(buf, 0)
	for i := 0; i < n; i++ {
		if buf[i] != 0 {
			return false
		}
	}
	return true
}

// Close closes the file
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return nil
	}
	f.closed.Store(true)

	if mm := f.mmap.Load(); mm != nil {
		mm.Close()
	}
	return f.file.Close()
}

// WriteRecord writes a key‑value record
// internal/storage/file.go - Fix WriteRecord to return correct offset
func (f *File) WriteRecord(key, value []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return 0, ErrClosed
	}

	off := f.size.Load() // This is the current file size, which is the correct offset for append

	keyLen := len(key)
	valLen := len(value)
	compressed := byte(0)

	if f.compressor != nil && len(value) >= f.compressThreshold {
		compressed = 1
		compressedValue, err := f.compressor.Encode(value)
		if err != nil {
			return 0, err
		}
		value = compressedValue
		valLen = len(value)
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
	pos += 1
	copy(buf[pos:], value)

	n, err := f.file.WriteAt(buf, off) // Write at the current offset
	if err != nil {
		return 0, err
	}
	f.size.Add(int64(n))
	return off, nil
}

// WriteTombstone similarly
func (f *File) WriteTombstone(key []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return ErrClosed
	}

	off := f.size.Load()
	keyLen := len(key)
	totalSize := 4 + keyLen + 4
	buf := make([]byte, totalSize)
	pos := 0

	binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
	pos += 4
	copy(buf[pos:], key)
	pos += keyLen
	binary.BigEndian.PutUint32(buf[pos:], 0)

	_, err := f.file.WriteAt(buf, off)
	if err != nil {
		return err
	}
	f.size.Add(int64(totalSize))
	return nil
}

// Write method for general writes
func (f *File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return 0, ErrClosed
	}

	off := f.size.Load()
	n, err := f.file.WriteAt(p, off)
	if err != nil {
		return n, err
	}
	f.size.Add(int64(n))
	return n, nil
}

// readRecord reads a record from the file at given offset (lock-free, uses mmap directly)
func (f *File) readRecord(reader *mmap.ReaderAt, offset int64) (key, value []byte, tombstone bool, n int64, err error) {
	// Read key length
	var keyLenBuf [4]byte
	if _, err := reader.ReadAt(keyLenBuf[:], offset); err != nil {
		return nil, nil, false, 0, err
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf[:])
	offset += 4
	n = 4

	if keyLen == 0 {
		// Tombstone: read value length (should be 0)
		var valLenBuf [4]byte
		if _, err := reader.ReadAt(valLenBuf[:], offset); err != nil {
			return nil, nil, false, n, err
		}
		return nil, nil, true, n + 4, nil
	}

	// Read key
	key = make([]byte, keyLen)
	if _, err := reader.ReadAt(key, offset); err != nil {
		return nil, nil, false, n, err
	}
	offset += int64(keyLen)
	n += int64(keyLen)

	// Read value length
	var valLenBuf [4]byte
	if _, err := reader.ReadAt(valLenBuf[:], offset); err != nil {
		return nil, nil, false, n, err
	}
	valLen := binary.BigEndian.Uint32(valLenBuf[:])
	offset += 4
	n += 4

	// Read compression flag
	var flagBuf [1]byte
	if _, err := reader.ReadAt(flagBuf[:], offset); err != nil {
		return nil, nil, false, n, err
	}
	compressed := flagBuf[0] == 1
	offset += 1
	n += 1

	// Read value
	if valLen > 0 {
		value = make([]byte, valLen)
		if _, err := reader.ReadAt(value, offset); err != nil {
			return nil, nil, false, n, err
		}
		n += int64(valLen)

		if compressed {
			if f.compressor == nil {
				return nil, nil, false, n, ErrCorrupt
			}
			// Decompress value
			decompressed, err := f.compressor.Decode(value)
			if err != nil {
				return nil, nil, false, n, err
			}
			value = decompressed
		}
	}

	return key, value, false, n, nil
}

// ReadRecordAt reads a record from the file at given offset (requires lock for consistency)
func (f *File) ReadRecordAt(offset int64) (key, value []byte, tombstone bool, n int64, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed.Load() {
		return nil, nil, false, 0, ErrClosed
	}

	reader := f.mmap.Load()
	if reader == nil {
		return nil, nil, false, 0, ErrClosed
	}

	return f.readRecord(reader, offset)
}

// Compressor returns the compressor instance
func (f *File) Compressor() *Compressor {
	return f.compressor
}
