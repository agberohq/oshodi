package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
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
	ErrFileBusy  = errors.New("storage: database file is locked by another process")
)

// metaBufPool reuses buffers for reading record metadata (key + valLen + flag).
// Max key size is ~4KB for typical workloads; pool covers the common case.
var metaBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// cacheLinePad is the size of a CPU cache line on all supported architectures
// (64 bytes on x86/arm64). Fields on separate cache lines never cause
// false-sharing invalidations when written concurrently.
const cacheLinePad = 64

type File struct {
	file   *os.File
	mmap   atomic.Pointer[mmap.ReaderAt]
	mmapMu sync.RWMutex
	path   string

	// logicalSize is the write cursor: every WriteRecord/WriteTombstone call
	// does an atomic Add here to claim a non-overlapping file region.
	// It sits on its own cache line so that concurrent writers on different
	// cores do not invalidate allocatedSize readers.
	logicalSize atomic.Int64
	_           [cacheLinePad - 8]byte

	// allocatedSize is the physical extent of the file as set by Truncate.
	// It is read on every write (to decide whether to grow) but written
	// only on the rare grow path. Padding it away from logicalSize means
	// the grow-check read is not invalidated by every logicalSize.Add.
	allocatedSize atomic.Int64
	_             [cacheLinePad - 8]byte

	// mappedSize tracks the extent covered by the current mmap reader.
	// remapLoop uses it to skip remaps when the logical file has not
	// grown past what is already mapped.
	mappedSize atomic.Int64

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
		fsyncDir(cfg.Path)
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
	f.mappedSize.Store(int64(reader.Len()))
	return nil
}

// remapLoop periodically refreshes the mmap reader so newly appended data
// becomes visible to mmap-backed reads. Remap only when the logical write
// cursor has grown past the currently mapped extent — during idle or
// read-heavy periods this skips the exclusive-lock acquire and page
// invalidation entirely.
func (f *File) remapLoop() {
	defer f.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if f.closed.Load() {
				continue
			}
			// Truncate also refreshes the mapping explicitly, so only the
			// append-driven growth path needs the remap here.
			if f.logicalSize.Load() > f.mappedSize.Load() {
				_ = f.remap()
			}
		case <-f.stopRetry:
			return
		}
	}
}

func (f *File) readRecord(reader io.ReaderAt, offset int64, dstBuf []byte, verify []byte) (key, value []byte, tombstone bool, n int64, err error) {
	// The key length is kept in front of the key in one pooled buffer so the
	// CRC prefix [keyLen | key | valLen | flag] stays contiguous. Keeping it
	// contiguous avoids the per-read heap allocation that small local arrays
	// caused when their slices were passed through io.ReaderAt and crc32.
	bufPtr := metaBufPool.Get().(*[]byte)
	metaBuf := *bufPtr
	defer func() {
		*bufPtr = metaBuf[:0]
		metaBufPool.Put(bufPtr)
	}()

	if cap(metaBuf) < 4 {
		metaBuf = make([]byte, 4)
	}
	metaBuf = metaBuf[:4]
	if _, err = reader.ReadAt(metaBuf, offset); err != nil {
		return
	}
	keyLen := binary.BigEndian.Uint32(metaBuf)
	n = 4

	if keyLen > MaxLegalKeyLen {
		return nil, nil, false, n, ErrCorrupt
	}

	if keyLen == 0 {
		// Padding hole or empty tombstone: read valLen to decide. The key
		// length is zero, so the front of the buffer can be reused.
		if _, err = reader.ReadAt(metaBuf[:4], offset+4); err != nil {
			return nil, nil, false, n, err
		}
		if binary.BigEndian.Uint32(metaBuf[:4]) == 0 {
			return nil, nil, true, n + 4, nil
		}
		return nil, nil, false, n, ErrCorrupt
	}

	// Read key + valLen(4) + flag(1) in one call, directly behind the key
	// length prefix already in the buffer.
	metaSize := int64(keyLen) + 4 + 1
	prefixLen := 4 + metaSize
	if int64(cap(metaBuf)) < prefixLen {
		grown := make([]byte, prefixLen)
		copy(grown[:4], metaBuf[:4])
		metaBuf = grown
	} else {
		metaBuf = metaBuf[:prefixLen]
	}
	if _, err = reader.ReadAt(metaBuf[4:], offset+4); err != nil {
		return nil, nil, false, n, err
	}

	keyEnd := int64(4) + int64(keyLen)
	valLen := binary.BigEndian.Uint32(metaBuf[keyEnd : keyEnd+4])
	if valLen > MaxLegalValLen {
		return nil, nil, false, n, ErrCorrupt
	}
	flag := metaBuf[keyEnd+4]
	n += int64(keyLen) + 4 + 1 // key + valLen + flag

	// When verify is set the key never materializes: the comparison runs
	// against the pooled buffer before it is recycled. Otherwise the key is
	// copied out — the pooled slice may be overwritten by another goroutine
	// at any time.
	if verify != nil {
		if !bytes.Equal(metaBuf[4:keyEnd], verify) {
			return nil, nil, false, n, ErrCorrupt
		}
	} else {
		key = make([]byte, keyLen)
		copy(key, metaBuf[4:keyEnd])
	}

	crc := crc32.Checksum(metaBuf[:prefixLen], crc32cTable)

	if valLen == 0 {
		// Read the trailer directly behind the prefix in the same buffer.
		if int64(cap(metaBuf)) < prefixLen+RecordCRCSize {
			grown := make([]byte, prefixLen+RecordCRCSize)
			copy(grown, metaBuf[:prefixLen])
			metaBuf = grown
		} else {
			metaBuf = metaBuf[:prefixLen+RecordCRCSize]
		}
		if _, err = reader.ReadAt(metaBuf[prefixLen:], offset+prefixLen); err != nil {
			return nil, nil, false, n, err
		}
		if binary.BigEndian.Uint32(metaBuf[prefixLen:]) != crc {
			return nil, nil, false, n, ErrCorrupt
		}
		n += RecordCRCSize
		switch flag {
		case RecordFlagTombstone:
			return key, nil, true, n, nil
		case RecordFlagEmptyValue:
			return key, nil, false, n, nil
		default:
			return nil, nil, false, n, ErrCorrupt
		}
	}

	compressed := flag == 1

	// Value plus the trailing 4-byte CRC footer, read as one block.
	readSize := int64(valLen) + RecordCRCSize
	if cap(dstBuf) >= int(readSize) {
		value = dstBuf[:readSize]
	} else {
		value = make([]byte, readSize)
	}
	if _, err = reader.ReadAt(value, offset+prefixLen); err != nil {
		return nil, nil, false, n, err
	}
	n += readSize

	if crc32.Update(crc, crc32cTable, value[:valLen]) != binary.BigEndian.Uint32(value[valLen:]) {
		return nil, nil, false, n, ErrCorrupt
	}

	if compressed {
		if f.compressor == nil {
			return nil, nil, false, n, ErrCorrupt
		}
		value, err = f.compressor.Decode(value[:valLen])
		if err != nil {
			return nil, nil, false, n, err
		}
		return key, value, false, n, nil
	}
	return key, value[:valLen], false, n, nil
}

// readRecordShared reads through the current mmap when possible and falls
// back to the file descriptor otherwise. On a stale mapping (a read past the
// last remap after an append) it refreshes the mapping once and retries,
// instead of paying a failed mmap read, an allocated error and an fd read for
// every record in a scan.
func (f *File) readRecordShared(off int64, dstBuf, verify []byte) (key, value []byte, tombstone bool, n int64, err error) {
	f.mmapMu.RLock()
	mm := f.mmap.Load()
	if mm == nil {
		f.mmapMu.RUnlock()
		return f.readRecord(f.file, off, dstBuf, verify)
	}
	key, value, tombstone, n, err = f.readRecord(mm, off, dstBuf, verify)
	f.mmapMu.RUnlock()
	if err == nil || errors.Is(err, ErrCorrupt) {
		return
	}
	if f.refreshMmap() {
		f.mmapMu.RLock()
		if mm = f.mmap.Load(); mm != nil {
			key, value, tombstone, n, err = f.readRecord(mm, off, dstBuf, verify)
		}
		f.mmapMu.RUnlock()
		if err == nil || errors.Is(err, ErrCorrupt) {
			return
		}
	}
	return f.readRecord(f.file, off, dstBuf, verify)
}

// refreshMmap remaps only when the logical extent has grown past what is
// mapped. Callers must not hold mmapMu.
func (f *File) refreshMmap() bool {
	if f.closed.Load() || f.logicalSize.Load() <= f.mappedSize.Load() {
		return false
	}
	if err := f.remap(); err != nil {
		return false
	}
	if f.closed.Load() {
		// Close won the race after the remap; release the mapping we just
		// installed instead of leaking it on a closed File.
		f.mmapMu.Lock()
		if mm := f.mmap.Load(); mm != nil {
			_ = mm.Close()
			f.mmap.Store(nil)
		}
		f.mmapMu.Unlock()
		return false
	}
	return true
}

// ReadValueAt is the hot-path reader: verifies the stored key against the
// caller's key without materializing key bytes, so a cache-miss Get does
// not allocate a key copy.
func (f *File) ReadValueAt(off int64, key []byte, dstBuf []byte) ([]byte, error) {
	if f.closed.Load() {
		return nil, ErrClosed
	}
	_, rVal, tombstone, _, err := f.readRecordShared(off, dstBuf, key)
	if err != nil {
		return nil, err
	}
	if tombstone {
		return nil, ErrCorrupt
	}
	return rVal, nil
}

// ReadAt reads a value record at offset, utilizing a destination buffer to
// avoid allocations. The key is verified inside readRecord.
func (f *File) ReadAt(off int64, key []byte, dstBuf []byte) ([]byte, error) {
	if f.closed.Load() {
		return nil, ErrClosed
	}
	_, rVal, tombstone, _, err := f.readRecordShared(off, dstBuf, key)
	if err != nil {
		return nil, err
	}
	if tombstone {
		return nil, ErrCorrupt
	}
	return rVal, nil
}

func (f *File) ReadRecordAt(offset int64) (key, value []byte, tombstone bool, n int64, err error) {
	if f.closed.Load() {
		return nil, nil, false, 0, ErrClosed
	}
	return f.readRecordShared(offset, nil, nil)
}

// WriteAtomic uses the Offset Reservation Pattern (lock-free in the common case).
func (f *File) WriteAtomic(p []byte) (int64, error) {
	if f.closed.Load() {
		return 0, ErrClosed
	}

	n := int64(len(p))
	off := f.logicalSize.Add(n) - n

	if off+n > f.allocatedSize.Load() {
		f.mu.Lock()
		if off+n > f.allocatedSize.Load() {
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

	if valLen > int(MaxLegalValLen) {
		return 0, ErrCorrupt
	}
	if keyLen > int(MaxLegalKeyLen) {
		return 0, ErrCorrupt
	}

	if f.compressor != nil && valLen >= f.compressor.Threshold() {
		if enc, err := f.compressor.Encode(value); err == nil {
			value = enc
			valLen = len(value)
			compressed = 1
		}
	}

	// valLen == 0 records carry RecordFlagEmptyValue, never the raw/
	// compressed marker.
	flag := compressed
	if valLen == 0 {
		flag = RecordFlagEmptyValue
	}

	payloadSize := int64(4 + keyLen + 4 + 1 + valLen)
	totalSize := payloadSize + RecordCRCSize
	buf := make([]byte, totalSize)
	pos := 0
	binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
	pos += 4
	copy(buf[pos:], key)
	pos += keyLen
	binary.BigEndian.PutUint32(buf[pos:], uint32(valLen))
	pos += 4
	buf[pos] = flag
	pos++
	pos += copy(buf[pos:], value)
	binary.BigEndian.PutUint32(buf[pos:], crc32.Checksum(buf[:payloadSize], crc32cTable))

	// Atomically reserve the offset. From this point every concurrent writer
	// has its own non-overlapping region — no mutex needed for the common path.
	off := f.logicalSize.Add(totalSize) - totalSize

	// Grow the file if the reservation exceeds the pre-allocated region.
	if off+totalSize > f.allocatedSize.Load() {
		f.mu.Lock()
		if off+totalSize > f.allocatedSize.Load() {
			newSize := off + totalSize + (totalSize * 4)
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
// The marker carries an explicit 0x02 flag byte so it cannot be confused
// with a regular record holding an empty value.
func (f *File) WriteTombstone(key []byte) error {
	if f.closed.Load() {
		return ErrClosed
	}

	keyLen := len(key)
	if keyLen > int(MaxLegalKeyLen) {
		return ErrCorrupt
	}
	payloadSize := int64(4 + keyLen + 4 + 1)
	totalSize := payloadSize + RecordCRCSize
	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint32(buf[0:], uint32(keyLen))
	copy(buf[4:], key)
	binary.BigEndian.PutUint32(buf[4+keyLen:], 0)
	buf[4+keyLen+4] = RecordFlagTombstone
	binary.BigEndian.PutUint32(buf[payloadSize:], crc32.Checksum(buf[:payloadSize], crc32cTable))

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
func (f *File) Size() int64 {
	return f.allocatedSize.Load()
}

func (f *File) LogicalSize() int64 {
	return f.logicalSize.Load()
}

// SetLogicalSize updates the logical end of the file.
// Required by db.loadIndex() during startup to ignore pre-allocated zero-padding.
func (f *File) SetLogicalSize(size int64) {
	f.logicalSize.Store(size)
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

// IsEmpty returns true if no valid record exists in the logical region.
// A preallocated file whose first bytes are zero padding is not empty if a
// record was appended after the padding, so this scans rather than trusting
// offset 0.
func (f *File) IsEmpty() bool {
	if f.closed.Load() {
		return true
	}
	limit := f.logicalSize.Load()
	if limit == 0 {
		return true
	}
	_, ok := f.NextRecordStart(0, limit)
	return !ok
}

// NextRecordStart returns the offset of the first complete, CRC-valid record
// at or after off, skipping zero padding left by preallocation or interrupted
// writes. limit is the logical end of the file. ok is false when no record
// exists in [off, limit).
//
// A record begins with a non-zero big-endian key length, so scanning for the
// first non-zero byte narrows the record start to the four-byte window ending
// there. Each candidate is CRC-checked, and the value is streamed rather than
// materialised so a corrupt length prefix cannot force a large allocation.
func (f *File) NextRecordStart(off, limit int64) (int64, bool) {
	if off < 0 {
		off = 0
	}
	if limit > f.allocatedSize.Load() {
		limit = f.allocatedSize.Load()
	}
	p, ok := f.nextNonZero(off, limit)
	if !ok {
		return 0, false
	}
	start := p - 3
	if start < off {
		start = off
	}
	for cand := start; cand <= p; cand++ {
		if f.validRecordAt(cand, limit) {
			return cand, true
		}
	}
	return 0, false
}

// nextNonZero returns the offset of the first non-zero byte in [off, limit).
func (f *File) nextNonZero(off, limit int64) (int64, bool) {
	const window = 64 << 10
	buf := make([]byte, window)
	for off < limit {
		n := int64(len(buf))
		if limit-off < n {
			n = limit - off
		}
		m, err := f.file.ReadAt(buf[:n], off)
		for i := 0; i < m; i++ {
			if buf[i] != 0 {
				return off + int64(i), true
			}
		}
		off += int64(m)
		if err != nil || m == 0 {
			return 0, false
		}
	}
	return 0, false
}

// validRecordAt reports whether a complete record at off parses and matches
// its CRC trailer. The payload is checksummed as a stream, so no allocation
// is proportional to the declared value length.
func (f *File) validRecordAt(off, limit int64) bool {
	if off < 0 || off+4 > limit {
		return false
	}
	var hdr [4]byte
	if _, err := f.file.ReadAt(hdr[:], off); err != nil {
		return false
	}
	keyLen := int64(binary.BigEndian.Uint32(hdr[:]))
	if keyLen < 1 || keyLen > int64(MaxLegalKeyLen) {
		return false
	}
	meta := off + 4 + keyLen
	if meta+5 > limit {
		return false
	}
	var tail [5]byte
	if _, err := f.file.ReadAt(tail[:], meta); err != nil {
		return false
	}
	valLen := int64(binary.BigEndian.Uint32(tail[:4]))
	flag := tail[4]
	if valLen > int64(MaxLegalValLen) {
		return false
	}
	if valLen == 0 {
		if flag != RecordFlagEmptyValue && flag != RecordFlagTombstone {
			return false
		}
	} else if flag > 1 {
		return false
	}
	payload := 4 + keyLen + 4 + 1 + valLen
	if payload+RecordCRCSize > limit-off {
		return false
	}
	h := crc32.New(crc32cTable)
	if _, err := io.CopyN(h, io.NewSectionReader(f.file, off, payload), payload); err != nil {
		return false
	}
	var crcBuf [4]byte
	if _, err := f.file.ReadAt(crcBuf[:], off+payload); err != nil {
		return false
	}
	return h.Sum32() == binary.BigEndian.Uint32(crcBuf[:])
}

func (f *File) Close() error {
	f.mu.Lock()
	if f.closed.Load() {
		f.mu.Unlock()
		return nil
	}
	f.closed.Store(true)
	f.mu.Unlock()

	close(f.stopRetry)
	f.wg.Wait()

	f.mmapMu.Lock()
	if mm := f.mmap.Load(); mm != nil {
		mm.Close()
	}
	f.mmapMu.Unlock()

	return f.file.Close()
}

// fsyncDir flushes directory metadata after create/rename. Non-fatal on
// filesystems that forbid directory fsync.
func fsyncDir(path string) {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

func (f *File) Compressor() *Compressor {
	return f.compressor
}
