package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/agberohq/oshodi/internal/consts"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrWALClosed    = errors.New("wal: closed")
	ErrWALCorrupted = errors.New("wal: corrupted record")
)

type WALMode int

const (
	// AsyncMode is the zero value so an unset WLabMode selects asymptotic
	// throughput: writes are batched by the flusher with no per-write fsync.
	AsyncMode WALMode = iota
	SyncMode
)

type WALConfig struct {
	Path          string
	BufferSize    int
	FlushInterval time.Duration
	Mode          WALMode
	Sync          bool
}

func DefaultWALConfig(path string) *WALConfig {
	return &WALConfig{
		Path:          path,
		BufferSize:    DefaultWALBufferSize,
		FlushInterval: DefaultWALFlushInterval,
		Mode:          AsyncMode, // async by default for performance
		Sync:          false,
	}
}

// record is one unflushed WAL entry held in memory until the next flush batch.
type record struct {
	offset int64
	key    []byte
	value  []byte // nil for tombstones
	tomb   bool
}

type WAL struct {
	file *os.File
	path string

	// pending is swapped with pendingSpare on every flush; memory stays
	// bounded by one flush window.
	mu           sync.Mutex
	pending      []record
	pendingSpare []record

	flushCh chan struct{}
	closeCh chan struct{}
	wg      sync.WaitGroup

	filePos  atomic.Int64
	flushPos atomic.Int64

	mode          WALMode
	bufferSize    int
	flushInterval time.Duration
	sync          bool

	closed atomic.Bool
}

func NewWAL(cfg *WALConfig) (*WAL, error) {
	if cfg == nil {
		cfg = DefaultWALConfig(DefaultWALFilename)
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = consts.DefaultWALFlushInterval
	}

	f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_RDWR, consts.FileMode)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	wal := &WAL{
		file:          f,
		path:          cfg.Path,
		flushCh:       make(chan struct{}, 1),
		closeCh:       make(chan struct{}),
		mode:          cfg.Mode,
		bufferSize:    cfg.BufferSize,
		flushInterval: cfg.FlushInterval,
		sync:          cfg.Sync,
	}

	fsyncDir(cfg.Path)

	wal.filePos.Store(info.Size())
	wal.flushPos.Store(info.Size())

	if info.Size() > 0 {
		if err := wal.recover(); err != nil {
			f.Close()
			return nil, err
		}
	}

	if cfg.Mode == AsyncMode {
		wal.wg.Add(1)
		go wal.flusher()
	}

	return wal, nil
}

// walRecordCRC computes the CRC-Castagnoli over a WAL record frame:
// [keyLen(4LE) | key | valLen(4LE) | value].
func walRecordCRC(keyLen uint32, key []byte, valLen uint32, value []byte) uint32 {
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[:], keyLen)
	crc := crc32.Checksum(scratch[:], crc32cTable)
	crc = crc32.Update(crc, crc32cTable, key)
	binary.LittleEndian.PutUint32(scratch[:], valLen)
	crc = crc32.Update(crc, crc32cTable, scratch[:])
	if len(value) > 0 {
		crc = crc32.Update(crc, crc32cTable, value)
	}
	return crc
}

// recordLen is the serialised length of a record, including the CRC trailer.
// Tombstone sentinel lengths are just marker values in the valLen field —
// they contribute no physical payload bytes.
func (w *WAL) recordLen(key, value []byte, tomb bool) int {
	_ = tomb
	return 4 + len(key) + 4 + len(value) + RecordCRCSize
}

func (w *WAL) Write(ctx context.Context, key, value []byte) (int64, error) {
	if w.closed.Load() {
		return 0, ErrWALClosed
	}
	if len(key) == 0 {
		return 0, ErrWALCorrupted
	}

	// Caller slices are frequently reused; the WAL owns its copy until
	// the async flush serialises the record.
	rl := w.recordLen(key, value, false)
	buf := make([]byte, len(key)+len(value))
	copy(buf, key)
	copy(buf[len(key):], value)
	ownedKey := buf[:len(key)]
	ownedVal := buf[len(key):]

	w.mu.Lock()
	offset := w.filePos.Add(int64(rl)) - int64(rl)
	w.pending = append(w.pending, record{offset: offset, key: ownedKey, value: ownedVal})
	w.mu.Unlock()

	if w.mode == SyncMode {
		if err := w.flushToDisk(); err != nil {
			return offset, err
		}
	} else {
		select {
		case w.flushCh <- struct{}{}:
		default:
		}
	}

	return offset, nil
}

// WriteTombstone appends a delete marker for key. The marker carries a
// 0xFFFFFFFF value-length sentinel so it can never be replayed as a regular
// record with an empty value.
func (w *WAL) WriteTombstone(ctx context.Context, key []byte) (int64, error) {
	if w.closed.Load() {
		return 0, ErrWALClosed
	}
	if len(key) == 0 {
		return 0, ErrWALCorrupted
	}

	rl := w.recordLen(key, nil, true)
	w.mu.Lock()
	offset := w.filePos.Add(int64(rl)) - int64(rl)
	buf := make([]byte, len(key))
	copy(buf, key)
	w.pending = append(w.pending, record{offset: offset, key: buf, tomb: true})
	w.mu.Unlock()

	if w.mode == SyncMode {
		if err := w.flushToDisk(); err != nil {
			return offset, err
		}
	} else {
		select {
		case w.flushCh <- struct{}{}:
		default:
		}
	}

	return offset, nil
}

// BatchWriteTombstones appends deletion markers for every key under one mutex
// section. It is used by batched deletes: one owned copy of all keys replaces
// one allocation and lock acquisition per key.
func (w *WAL) BatchWriteTombstones(ctx context.Context, keys [][]byte) error {
	if w.closed.Load() {
		return ErrWALClosed
	}
	if len(keys) == 0 {
		return nil
	}
	total := 0
	for _, k := range keys {
		if len(k) == 0 {
			return ErrWALCorrupted
		}
		total += len(k)
	}

	buf := make([]byte, total)
	recs := make([]record, len(keys))
	pos := 0
	w.mu.Lock()
	for i, k := range keys {
		copy(buf[pos:], k)
		owned := buf[pos : pos+len(k)]
		pos += len(k)
		rl := w.recordLen(k, nil, true)
		offset := w.filePos.Add(int64(rl)) - int64(rl)
		recs[i] = record{offset: offset, key: owned, tomb: true}
	}
	w.pending = append(w.pending, recs...)
	w.mu.Unlock()

	if w.mode == SyncMode {
		return w.flushToDisk()
	}
	select {
	case w.flushCh <- struct{}{}:
	default:
	}
	return nil
}

func (w *WAL) flushToDisk() error {
	// mu is held for the whole flush so concurrent flush triggers cannot
	// interleave writes at the same file offset.
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.pending) == 0 {
		return nil
	}

	// Swap the pending queue out and reset the slot for future writes.
	batch := w.pending
	w.pending = w.pendingSpare[:0]
	w.pendingSpare = batch

	// Encode in file order: pending appends hold mu together with the
	// filePos advance, so order is exact.
	totalBufSize := 0
	for i := range batch {
		totalBufSize += w.recordLen(batch[i].key, batch[i].value, batch[i].tomb)
	}
	buf := make([]byte, totalBufSize)
	pos := 0
	encoded := 0
	for i := range batch {
		valLen := uint32(len(batch[i].value))
		if batch[i].tomb {
			valLen = WALTombstoneValLen
		}
		binary.LittleEndian.PutUint32(buf[pos:], uint32(len(batch[i].key)))
		pos += 4
		pos += copy(buf[pos:], batch[i].key)
		binary.LittleEndian.PutUint32(buf[pos:], valLen)
		pos += 4
		pos += copy(buf[pos:], batch[i].value)

		// CRC-Castagnoli over this record's frame.
		binary.LittleEndian.PutUint32(buf[pos:], walRecordCRC(
			uint32(len(batch[i].key)), batch[i].key, valLen, batch[i].value))
		pos += RecordCRCSize
		encoded = pos
	}

	flushPos := w.flushPos.Load()
	nextFlushPos := flushPos + int64(encoded)

	_, err := w.file.WriteAt(buf[:encoded], flushPos)
	if err != nil {
		return err
	}

	if w.sync {
		if err := w.file.Sync(); err != nil {
			w.flushPos.Store(nextFlushPos)
			return err
		}
	}

	w.flushPos.Store(nextFlushPos)

	// Release the batch's references so its memory is collectable.
	for i := range batch {
		batch[i] = record{}
	}
	return nil
}

func (w *WAL) Read(ctx context.Context, offset int64) (key, value []byte, err error) {
	if w.closed.Load() {
		return nil, nil, ErrWALClosed
	}

	w.mu.Lock()
	for i := range w.pending {
		if w.pending[i].offset == offset {
			k := make([]byte, len(w.pending[i].key))
			copy(k, w.pending[i].key)
			var v []byte
			if !w.pending[i].tomb {
				v = make([]byte, len(w.pending[i].value))
				copy(v, w.pending[i].value)
			}
			w.mu.Unlock()
			return k, v, nil
		}
	}
	w.mu.Unlock()

	return w.readFromDisk(offset)
}

func (w *WAL) readRecordAt(offset int64) (key, value []byte, tomb bool, next int64, err error) {
	var keyLenBuf [4]byte
	if _, err = w.file.ReadAt(keyLenBuf[:], offset); err != nil {
		return
	}
	keyLen := binary.LittleEndian.Uint32(keyLenBuf[:])
	if keyLen == 0 || keyLen > MaxLegalKeyLen {
		return nil, nil, false, offset, ErrWALCorrupted
	}

	key = make([]byte, keyLen)
	if _, err = w.file.ReadAt(key, offset+4); err != nil {
		return
	}

	var valLenBuf [4]byte
	valOffset := offset + 4 + int64(keyLen)
	if _, err = w.file.ReadAt(valLenBuf[:], valOffset); err != nil {
		return
	}
	valLen := binary.LittleEndian.Uint32(valLenBuf[:])

	if valLen == WALTombstoneValLen {
		// Tombstones carry the same CRC trailer as value records; advancing
		// past only the length field would land inside the trailer and make
		// recovery truncate the WAL at the first tombstone.
		var crcBuf [RecordCRCSize]byte
		if _, err = w.file.ReadAt(crcBuf[:], valOffset+4); err != nil {
			return
		}
		if binary.LittleEndian.Uint32(crcBuf[:]) != walRecordCRC(keyLen, key, valLen, nil) {
			return nil, nil, false, offset, ErrWALCorrupted
		}
		return key, nil, true, valOffset + 4 + RecordCRCSize, nil
	}
	if valLen > MaxLegalValLen {
		return nil, nil, false, offset, ErrWALCorrupted
	}

	value = make([]byte, valLen)
	if valLen > 0 {
		if _, err = w.file.ReadAt(value, valOffset+4); err != nil {
			return
		}
	}

	// CRC trailer over the frame.
	var crcBuf [RecordCRCSize]byte
	if _, err = w.file.ReadAt(crcBuf[:], valOffset+4+int64(valLen)); err != nil {
		return
	}
	valueLen := uint32(len(value))
	if binary.LittleEndian.Uint32(crcBuf[:]) != walRecordCRC(keyLen, key, valueLen, value) {
		err = ErrWALCorrupted
		return
	}
	return key, value, false, valOffset + 4 + int64(valLen) + RecordCRCSize, nil
}

func (w *WAL) readFromDisk(offset int64) (key, value []byte, err error) {
	k, v, _, _, err := w.readRecordAt(offset)
	return k, v, err
}

// recover validates the on-disk WAL contents and positions the append and
// flush cursors at the last intact record boundary. A torn tail — a crash
// in the middle of a partially flushed record — is truncated instead of
// failing the open, so a crash never makes the database unopenable.
func (w *WAL) recover() error {
	offset := int64(0)
	size := w.filePos.Load()

	for offset < size {
		_, _, _, next, err := w.readRecordAt(offset)
		if err != nil {
			// Torn tail: drop the incomplete record.
			break
		}
		offset = next
	}

	if offset < size {
		if err := w.file.Truncate(offset); err != nil {
			return err
		}
		w.filePos.Store(offset)
		w.flushPos.Store(offset)
	}

	return nil
}

func (w *WAL) flusher() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.closeCh:
			_ = w.flushToDisk()
			return
		case <-ticker.C:
			_ = w.flushToDisk()
		case <-w.flushCh:
			_ = w.flushToDisk()
		}
	}
}

func (w *WAL) Flush(ctx context.Context) error {
	if w.closed.Load() {
		return ErrWALClosed
	}
	// Flush synchronously. Signalling the background flusher and waiting for
	// a mutex acquisition did not guarantee that the write had happened, so
	// Flush could return with records still only in memory.
	return w.flushToDisk()
}

func (w *WAL) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return ErrWALClosed
	}

	if w.mode == AsyncMode {
		close(w.closeCh)
		w.wg.Wait()
	}

	_ = w.flushToDisk()

	return w.file.Close()
}

func (w *WAL) Size() int64 {
	return w.filePos.Load()
}

// FileSize returns the current byte size of the WAL file.
// It is an alias for Size() to satisfy the db.go replay interface.
func (w *WAL) FileSize() int64 {
	return w.filePos.Load()
}

// WALReader is a sequential reader for WAL replay on startup.
// It holds a read cursor and advances record-by-record through the file.
type WALReader struct {
	wal    *WAL
	offset int64
}

// NewReader creates a WALReader starting at the given byte offset.
// Pass 0 to replay from the beginning of the file.
func (w *WAL) NewReader(startOffset int64) *WALReader {
	return &WALReader{wal: w, offset: startOffset}
}

// ReadNext reads the next record and returns the raw serialised payload
// (keyLen[4LE] + key + valueLen[4LE] + value). A deleted record carries the
// WALTombstoneValLen sentinel as the value length; such records are
// deletions. A value length of 0 is a regular empty-value record.
// Returns (nil, io.EOF) when the end of the written region is reached.
// The dst parameter is reserved for future zero-copy support and is ignored.
func (r *WALReader) ReadNext(dst []byte) ([]byte, error) {
	w := r.wal
	size := w.filePos.Load()
	if r.offset >= size {
		return nil, io.EOF
	}

	key, _, tomb, next, err := w.readRecordAt(r.offset)
	if err != nil {
		return nil, err
	}

	totalLen := int(next - r.offset)
	buf := make([]byte, totalLen)
	pos := 0
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(key)))
	pos += 4
	copy(buf[pos:], key)
	pos += len(key)
	if tomb {
		binary.LittleEndian.PutUint32(buf[pos:], WALTombstoneValLen)
		pos += 4
	} else {
		// The value occupies every remaining byte of the frame.
		valueLen := totalLen - pos - 4
		binary.LittleEndian.PutUint32(buf[pos:], uint32(valueLen))
		pos += 4
		if _, err := w.file.ReadAt(buf[pos:], r.offset+int64(pos)); err != nil {
			return nil, err
		}
	}

	r.offset = next
	return buf, nil
}
