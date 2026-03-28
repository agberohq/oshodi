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
type File struct {
	file *os.File
	mmap atomic.Pointer[mmap.ReaderAt]
	path string
	// logicalSize: end of written record data (where next write goes)
	logicalSize atomic.Int64
	// allocatedSize: actual file size on disk (for Size() API compatibility)
	allocatedSize atomic.Int64
	mu            sync.RWMutex
	closed        atomic.Bool

	compressor    *Compressor
	logger        *ll.Logger
	retryAttempts int
	retryBackoff  time.Duration

	// Retry queue for failed mmap operations
	retryMu    sync.Mutex
	retryQueue []func() error
	stopRetry  chan struct{}
	wg         sync.WaitGroup
}

// NewFile creates a new storage file with configuration.
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

	// Handle pre-allocation for new files
	if fileSize == 0 && cfg.InitialSize > 0 {
		if err := f.Truncate(cfg.InitialSize); err != nil {
			f.Close()
			return nil, err
		}
		file.allocatedSize.Store(cfg.InitialSize)
		file.logicalSize.Store(0) // No records written yet
	} else {
		// Existing file: conservative assumption that all space contains valid data
		file.allocatedSize.Store(fileSize)
		file.logicalSize.Store(fileSize)
	}

	// Initial mmap
	if err := file.remap(); err != nil {
		f.Close()
		return nil, err
	}

	// Start retry processor
	file.wg.Add(1)
	go file.processRetryQueue()

	return file, nil
}

// remap creates a new mmap reader (must be called with write lock)
func (f *File) remap() error {
	// Close old mmap
	if old := f.mmap.Load(); old != nil {
		old.Close()
	}

	// Open new mmap
	reader, err := mmap.Open(f.path)
	if err != nil {
		return err
	}
	f.mmap.Store(reader)
	return nil
}

// remapWithRetry attempts to remap with retries
func (f *File) remapWithRetry() error {
	var lastErr error
	for i := 0; i < f.retryAttempts; i++ {
		if err := f.remap(); err == nil {
			return nil
		} else {
			lastErr = err
			if i < f.retryAttempts-1 {
				time.Sleep(f.retryBackoff)
			}
		}
	}
	return lastErr
}

// queueRemap adds a remap operation to the retry queue
func (f *File) queueRemap() {
	f.retryMu.Lock()
	defer f.retryMu.Unlock()

	f.retryQueue = append(f.retryQueue, func() error {
		return f.remapWithRetry()
	})
}

// processRetryQueue processes pending retry operations
func (f *File) processRetryQueue() {
	defer f.wg.Done()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.retryMu.Lock()
			if len(f.retryQueue) > 0 {
				queue := f.retryQueue
				f.retryQueue = nil
				f.retryMu.Unlock()

				for _, op := range queue {
					if err := op(); err != nil {
						f.logger.Fields("error", err).Error("failed to execute queued operation")
						// Re-queue on failure
						f.retryMu.Lock()
						f.retryQueue = append(f.retryQueue, op)
						f.retryMu.Unlock()
					}
				}
			} else {
				f.retryMu.Unlock()
			}
		case <-f.stopRetry:
			return
		}
	}
}

// ReadAt reads data at offset - LOCK-FREE path
func (f *File) ReadAt(off int64, key []byte) ([]byte, error) {
	if f.closed.Load() {
		return nil, ErrClosed
	}

	reader := f.mmap.Load()
	if reader == nil {
		return nil, ErrClosed
	}

	readKey, value, tombstone, _, err := f.readRecord(reader, off)
	if err != nil {
		return nil, err
	}

	if tombstone {
		return nil, ErrCorrupt
	}

	if !bytes.Equal(readKey, key) {
		return nil, ErrCorrupt
	}

	return value, nil
}

// Write writes data at current position (requires write lock)
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

	// Queue remap for eventual consistency
	f.queueRemap()

	return n, nil
}

// WriteAt writes data at specific offset (requires write lock)
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return 0, ErrClosed
	}

	n, err := f.file.WriteAt(p, off)
	if err != nil {
		return n, err
	}

	// Update sizes if writing at/after current end
	endPos := off + int64(n)
	if curAlloc := f.allocatedSize.Load(); endPos > curAlloc {
		f.allocatedSize.Store(endPos)
	}
	if curLogical := f.logicalSize.Load(); endPos > curLogical {
		f.logicalSize.Store(endPos)
	}

	// Queue remap for eventual consistency
	f.queueRemap()

	return n, nil
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

// Size returns the allocated file size (API compatibility)
func (f *File) Size() int64 {
	return f.allocatedSize.Load()
}

// LogicalSize returns the end position of written record data (internal use)
func (f *File) LogicalSize() int64 {
	return f.logicalSize.Load()
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
	f.allocatedSize.Store(size)
	// Logical size cannot exceed allocated size
	if f.logicalSize.Load() > size {
		f.logicalSize.Store(size)
	}

	// Synchronous remap for truncation
	return f.remapWithRetry()
}

// IsEmpty checks if file has any actual data beyond initial allocation
func (f *File) IsEmpty() bool {
	if f.closed.Load() {
		return true
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	logicalSize := f.logicalSize.Load()
	if logicalSize == 0 {
		return true
	}

	reader := f.mmap.Load()
	if reader == nil {
		return true
	}

	// Try to read the first record to see if there's any data
	var offset int64
	for offset < logicalSize {
		_, _, _, n, err := f.readRecord(reader, offset)
		if err != nil {
			// If we can't read, assume empty or corrupted
			break
		}
		// If we successfully read any record (even tombstone), file has data
		return false
		offset += n
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

	close(f.stopRetry)
	f.wg.Wait()

	if mm := f.mmap.Load(); mm != nil {
		mm.Close()
	}
	return f.file.Close()
}

// WriteRecord writes a key-value record
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

	if f.compressor != nil && len(value) >= f.compressor.Threshold() {
		compressed = 1
		compressedValue, err := f.compressor.Encode(value)
		if err != nil {
			f.logger.Fields("error", err).Error("compression failed")
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

	n, err := f.file.WriteAt(buf, off)
	if err != nil {
		f.logger.Fields("error", err, "offset", off).Error("write failed")
		return 0, err
	}

	f.logicalSize.Add(int64(n))
	f.allocatedSize.Add(int64(n))

	// Queue remap for eventual consistency (non-blocking)
	f.queueRemap()

	f.logger.Fields("offset", off, "size", totalSize).Debug("record written")
	return off, nil
}

// WriteTombstone writes a tombstone record
func (f *File) WriteTombstone(key []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed.Load() {
		return ErrClosed
	}

	off := f.logicalSize.Load()
	keyLen := len(key)
	totalSize := 4 + keyLen + 4
	buf := make([]byte, totalSize)
	pos := 0

	binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
	pos += 4
	copy(buf[pos:], key)
	pos += keyLen
	binary.BigEndian.PutUint32(buf[pos:], 0)

	n, err := f.file.WriteAt(buf, off)
	if err != nil {
		f.logger.Fields("error", err, "offset", off).Error("tombstone write failed")
		return err
	}

	// Use actual bytes written (n) for size tracking
	f.logicalSize.Add(int64(n))
	f.allocatedSize.Add(int64(n))

	// Queue remap for eventual consistency
	f.queueRemap()

	f.logger.Fields("key", string(key), "offset", off).Debug("tombstone written")
	return nil
}

// readRecord reads a record from the file at given offset
// Accepts io.ReaderAt to support both mmap.ReaderAt and *os.File
func (f *File) readRecord(reader io.ReaderAt, offset int64) (key, value []byte, tombstone bool, n int64, err error) {
	// Read key length
	var keyLenBuf [4]byte
	if _, err := reader.ReadAt(keyLenBuf[:], offset); err != nil {
		return nil, nil, false, 0, err
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf[:])
	offset += 4
	n = 4

	if keyLen == 0 {
		// Check if this is a tombstone
		var valLenBuf [4]byte
		if _, err := reader.ReadAt(valLenBuf[:], offset); err != nil {
			return nil, nil, false, n, err
		}
		valLen := binary.BigEndian.Uint32(valLenBuf[:])
		if valLen == 0 {
			return nil, nil, true, n + 4, nil
		}
		return nil, nil, false, n, ErrCorrupt
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

	if valLen == 0 {
		return key, nil, true, n, nil
	}

	// Read compression flag
	var flagBuf [1]byte
	if _, err := reader.ReadAt(flagBuf[:], offset); err != nil {
		return nil, nil, false, n, err
	}
	compressed := flagBuf[0] == 1
	offset += 1
	n += 1

	// Read value
	value = make([]byte, valLen)
	if _, err := reader.ReadAt(value, offset); err != nil {
		return nil, nil, false, n, err
	}
	n += int64(valLen)

	if compressed {
		if f.compressor == nil {
			return nil, nil, false, n, ErrCorrupt
		}
		decompressed, err := f.compressor.Decode(value)
		if err != nil {
			return nil, nil, false, n, err
		}
		value = decompressed
	}

	return key, value, false, n, nil
}

// ReadRecordAt reads a record from the file at given offset
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

	key, value, tombstone, n, err = f.readRecord(reader, offset)

	// Fallback to direct file read if mmap is stale (eventual consistency)
	if (err == io.EOF || err == io.ErrUnexpectedEOF) && offset < f.logicalSize.Load() {
		return f.readRecord(f.file, offset)
	}

	return key, value, tombstone, n, err
}

// Compressor returns the compressor instance
func (f *File) Compressor() *Compressor {
	return f.compressor
}
