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

// NewFile creates or opens a storage file with the given configuration.
// It truncates the file to InitialSize if it's a new file, and sets up mmap.
// The allocated size tracks the actual file size on disk.
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
		if err := file.Truncate(cfg.InitialSize); err != nil {
			file.Close()
			return nil, err
		}
		file.allocatedSize.Store(cfg.InitialSize)
		file.logicalSize.Store(0)
	} else {
		file.allocatedSize.Store(fileSize)
		file.logicalSize.Store(fileSize)
	}

	_ = file.remap()

	file.wg.Add(1)
	go file.remapLoop()

	return file, nil
}

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

func (f *File) remapLoop() {
	defer f.wg.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
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

func (f *File) readRecord(reader io.ReaderAt, offset int64, dstBuf []byte) (key, value []byte, tombstone bool, n int64, err error) {
	// Read the fixed 9-byte header in a single call:
	//   [0:4]  keyLen  (uint32 big-endian)
	//   [4:8]  valLen  (uint32 big-endian)
	//   [8]    compressed flag (present only when valLen > 0)
	// We always read 9 bytes; for tombstones the flag byte is harmless padding.
	const headerSize = 9
	var hdr [headerSize]byte
	if _, err = reader.ReadAt(hdr[:4], offset); err != nil {
		return
	}
	keyLen := binary.BigEndian.Uint32(hdr[:4])
	n = 4

	// Zero keyLen: padding hole or an empty tombstone (keyLen==0, valLen==0).
	if keyLen == 0 {
		if _, err = reader.ReadAt(hdr[4:8], offset+4); err != nil {
			return nil, nil, false, n, err
		}
		valLen := binary.BigEndian.Uint32(hdr[4:8])
		if valLen == 0 {
			return nil, nil, true, n + 4, nil
		}
		return nil, nil, false, n, ErrCorrupt
	}

	// Read key + valLen + flag in one shot to avoid 3 separate syscalls.
	// Layout after keyLen: key[keyLen] | valLen[4] | flag[1]
	metaSize := int64(keyLen) + 4 + 1
	metaBuf := make([]byte, metaSize)
	if _, err = reader.ReadAt(metaBuf, offset+4); err != nil {
		return nil, nil, false, n, err
	}

	key = metaBuf[:keyLen]
	valLen := binary.BigEndian.Uint32(metaBuf[keyLen : keyLen+4])
	n += int64(keyLen) + 4

	if valLen == 0 {
		// Tombstone: key present, value absent.
		return key, nil, true, n, nil
	}

	compressed := metaBuf[keyLen+4] == 1
	n += 1

	// Read value — reuse dstBuf if it's large enough (zero-alloc hot path).
	if cap(dstBuf) >= int(valLen) {
		value = dstBuf[:valLen]
	} else {
		value = make([]byte, valLen)
	}
	valueOffset := offset + 4 + metaSize
	if _, err = reader.ReadAt(value, valueOffset); err != nil {
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

// ReadAt reads a value record at offset, utilizing a destination buffer to avoid allocations.
func (f *File) ReadAt(off int64, key []byte, dstBuf []byte) ([]byte, error) {
	if f.closed.Load() {
		return nil, ErrClosed
	}

	f.mmapMu.RLock()
	mm := f.mmap.Load()
	if mm != nil {
		rKey, rVal, tombstone, _, err := f.readRecord(mm, off, dstBuf)
		f.mmapMu.RUnlock()
		if err == nil && !tombstone && bytes.Equal(rKey, key) {
			return rVal, nil
		}
	} else {
		f.mmapMu.RUnlock()
	}

	rKey, rVal, tombstone, _, err := f.readRecord(f.file, off, dstBuf)
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

func (f *File) ReadRecordAt(offset int64) (key, value []byte, tombstone bool, n int64, err error) {
	if f.closed.Load() {
		return nil, nil, false, 0, ErrClosed
	}

	f.mmapMu.RLock()
	mm := f.mmap.Load()
	if mm != nil {
		k, v, ts, sz, e := f.readRecord(mm, offset, nil)
		f.mmapMu.RUnlock()
		if e == nil {
			return k, v, ts, sz, nil
		}
	} else {
		f.mmapMu.RUnlock()
	}

	return f.readRecord(f.file, offset, nil)
}

// WriteAtomic uses the Offset Reservation Pattern (lock-free in the common case).
func (f *File) WriteAtomic(p []byte) (int64, error) {
	if f.closed.Load() {
		return 0, ErrClosed
	}

	n := int64(len(p))
	off := f.logicalSize.Add(n) - n

	// Lock-free growth check + aggressive pre-allocation to avoid contention
	if off+n > f.allocatedSize.Load() {
		f.mu.Lock()
		if off+n > f.allocatedSize.Load() {
			// Grow 4× the current request so we rarely hit this path again
			newSize := off + n + (n * 4)
			if err := f.file.Truncate(newSize); err != nil {
				f.mu.Unlock()
				return off, err
			}
			f.allocatedSize.Store(newSize)
		}
		f.mu.Unlock()
	}

	_, err := f.file.WriteAt(p, off)
	if err != nil {
		return off, err
	}

	return off, nil
}

// WriteRecord serialises and appends a key-value record.
// Uses the offset-reservation pattern: atomically reserve space, build the
// buffer lock-free, then write. The mutex is only taken for the rare grow path.
func (f *File) WriteRecord(key, value []byte) (int64, error) {
	if f.closed.Load() {
		return 0, ErrClosed
	}

	keyLen := len(key)
	valLen := len(value)
	compressed := byte(0)

	// Compression is pure CPU work — do it before touching any shared state.
	if f.compressor != nil && valLen >= f.compressor.Threshold() {
		if enc, err := f.compressor.Encode(value); err == nil {
			value = enc
			valLen = len(value)
			compressed = 1
		}
	}

	// Serialise the record into a local buffer with no locks held.
	totalSize := int64(4 + keyLen + 4 + 1 + valLen)
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

	// Atomically reserve the offset. From this point every concurrent writer
	// has its own non-overlapping region — no mutex needed for the common path.
	off := f.logicalSize.Add(totalSize) - totalSize

	// Grow the file if the reservation exceeds the pre-allocated region.
	// This is the rare path; the file starts at InitialSize so it only triggers
	// after the initial allocation is exhausted.
	if off+totalSize > f.allocatedSize.Load() {
		f.mu.Lock()
		if off+totalSize > f.allocatedSize.Load() {
			newSize := off + totalSize + (totalSize * 4) // 4x over-allocation
			if err := f.file.Truncate(newSize); err != nil {
				f.mu.Unlock()
				return off, err
			}
			f.allocatedSize.Store(newSize)
		}
		f.mu.Unlock()
	}

	if _, err := f.file.WriteAt(buf, off); err != nil {
		return 0, err
	}
	return off, nil
}

// WriteTombstone writes a deletion marker for key.
func (f *File) WriteTombstone(key []byte) error {
	if f.closed.Load() {
		return ErrClosed
	}

	keyLen := len(key)
	totalSize := int64(4 + keyLen + 4)
	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint32(buf[0:], uint32(keyLen))
	copy(buf[4:], key)
	binary.BigEndian.PutUint32(buf[4+keyLen:], 0)

	off := f.logicalSize.Add(totalSize) - totalSize

	if off+totalSize > f.allocatedSize.Load() {
		f.mu.Lock()
		if off+totalSize > f.allocatedSize.Load() {
			newSize := off + totalSize + (totalSize * 4)
			if err := f.file.Truncate(newSize); err != nil {
				f.mu.Unlock()
				return err
			}
			f.allocatedSize.Store(newSize)
		}
		f.mu.Unlock()
	}

	_, err := f.file.WriteAt(buf, off)
	return err
}

func (f *File) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed.Load() {
		return ErrClosed
	}
	return f.file.Sync()
}

// Size returns the total allocated size of the file (the current file size on disk).
// It is safe for concurrent use.
func (f *File) Size() int64 {
	return f.allocatedSize.Load()
}

func (f *File) LogicalSize() int64 {
	return f.logicalSize.Load()
}

// SetLogicalSize updates the logical end of the file.
// This is heavily required by db.loadIndex() during startup to ignore pre-allocated zero-padding.
func (f *File) SetLogicalSize(size int64) {
	f.logicalSize.Store(size)
	// FIX: Must also update the write cursor tracking
	f.allocatedSize.Store(size)
}

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

// IsEmpty returns true if no valid records have been written.
func (f *File) IsEmpty() bool {
	if f.closed.Load() {
		return true
	}

	// Fast path check
	if f.logicalSize.Load() == 0 {
		return true
	}

	// Try to read the first record directly from the file.
	key, _, tombstone, _, err := f.readRecord(f.file, 0, nil)
	if err != nil {
		return true // EOF or Corrupt padding hole means it's practically empty
	}

	// If it successfully read a record but it's a padding hole (len(key) == 0)
	if tombstone && len(key) == 0 {
		return true
	}

	return false
}

func (f *File) Close() error {
	f.mu.Lock()
	if f.closed.Load() {
		f.mu.Unlock()
		return nil
	}
	f.closed.Store(true)
	f.mu.Unlock() // Release before Wait so remapLoop can finish without contending.

	close(f.stopRetry)
	f.wg.Wait()

	f.mmapMu.Lock()
	if mm := f.mmap.Load(); mm != nil {
		mm.Close()
	}
	f.mmapMu.Unlock()

	return f.file.Close()
}

func (f *File) Compressor() *Compressor {
	return f.compressor
}
