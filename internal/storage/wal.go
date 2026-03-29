package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olekukonko/mappo"
)

var (
	ErrWALClosed     = errors.New("wal: closed")
	ErrWALCorrupted  = errors.New("wal: corrupted record")
	ErrWALBufferFull = errors.New("wal: buffer full")
)

type WALMode int

const (
	SyncMode WALMode = iota
	AsyncMode
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
		BufferSize:    1024,
		FlushInterval: 1 * time.Millisecond,
		Mode:          SyncMode,
		Sync:          false,
	}
}

type record struct {
	offset int64
	key    []byte
	value  []byte
}

type WAL struct {
	file *os.File
	path string

	records *mappo.Slicer[record]

	flushCh chan struct{}
	closeCh chan struct{}
	wg      sync.WaitGroup

	mu sync.Mutex

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
		cfg = DefaultWALConfig("wal.log")
	}

	f, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_RDWR, 0600)
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
		records:       mappo.NewSlicer[record](),
		flushCh:       make(chan struct{}, 1),
		closeCh:       make(chan struct{}),
		mode:          cfg.Mode,
		bufferSize:    cfg.BufferSize,
		flushInterval: cfg.FlushInterval,
		sync:          cfg.Sync,
	}

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

func (w *WAL) Write(ctx context.Context, key, value []byte) (int64, error) {
	if w.closed.Load() {
		return 0, ErrWALClosed
	}

	recordLen := 4 + len(key) + 4 + len(value)
	offset := w.filePos.Add(int64(recordLen)) - int64(recordLen)

	keyCopy := make([]byte, len(key))
	valueCopy := make([]byte, len(value))
	copy(keyCopy, key)
	copy(valueCopy, value)

	w.records.Append(record{
		offset: offset,
		key:    keyCopy,
		value:  valueCopy,
	})

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

func (w *WAL) flushToDisk() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	flushPos := w.flushPos.Load()
	currentFilePos := w.filePos.Load()

	if currentFilePos <= flushPos {
		return nil
	}

	var recordsToFlush []record

	w.records.Range(func(idx uint64, r record) bool {
		if r.offset >= flushPos && r.offset < currentFilePos {
			recordsToFlush = append(recordsToFlush, r)
		}
		return true
	})

	if len(recordsToFlush) == 0 {
		return nil
	}

	sort.Slice(recordsToFlush, func(i, j int) bool {
		return recordsToFlush[i].offset < recordsToFlush[j].offset
	})

	buf := make([]byte, 0)

	for _, r := range recordsToFlush {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(r.key)))
		buf = append(buf, r.key...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(r.value)))
		buf = append(buf, r.value...)
	}

	_, err := w.file.WriteAt(buf, flushPos)
	if err != nil {
		return err
	}

	if w.sync {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	w.flushPos.Store(currentFilePos)

	return nil
}

func (w *WAL) Read(ctx context.Context, offset int64) (key, value []byte, err error) {
	if w.closed.Load() {
		return nil, nil, ErrWALClosed
	}

	var found *record

	w.records.Range(func(idx uint64, r record) bool {
		if r.offset == offset {
			found = &r
			return false
		}
		return true
	})

	if found != nil {
		key = make([]byte, len(found.key))
		value = make([]byte, len(found.value))
		copy(key, found.key)
		copy(value, found.value)
		return key, value, nil
	}

	return w.readFromDisk(offset)
}

func (w *WAL) readFromDisk(offset int64) (key, value []byte, err error) {
	var keyLenBuf [4]byte
	_, err = w.file.ReadAt(keyLenBuf[:], offset)
	if err != nil {
		return nil, nil, err
	}
	keyLen := binary.LittleEndian.Uint32(keyLenBuf[:])

	key = make([]byte, keyLen)
	_, err = w.file.ReadAt(key, offset+4)
	if err != nil {
		return nil, nil, err
	}

	var valLenBuf [4]byte
	valOffset := offset + 4 + int64(keyLen)

	_, err = w.file.ReadAt(valLenBuf[:], valOffset)
	if err != nil {
		return nil, nil, err
	}
	valLen := binary.LittleEndian.Uint32(valLenBuf[:])

	value = make([]byte, valLen)
	_, err = w.file.ReadAt(value, valOffset+4)
	if err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

func (w *WAL) recover() error {
	offset := int64(0)
	size := w.filePos.Load()

	for offset < size {
		var keyLenBuf [4]byte
		_, err := w.file.ReadAt(keyLenBuf[:], offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		keyLen := binary.LittleEndian.Uint32(keyLenBuf[:])

		key := make([]byte, keyLen)
		_, err = w.file.ReadAt(key, offset+4)
		if err != nil {
			return err
		}

		var valLenBuf [4]byte
		valOffset := offset + 4 + int64(keyLen)

		_, err = w.file.ReadAt(valLenBuf[:], valOffset)
		if err != nil {
			return err
		}

		valLen := binary.LittleEndian.Uint32(valLenBuf[:])

		value := make([]byte, valLen)
		_, err = w.file.ReadAt(value, valOffset+4)
		if err != nil {
			return err
		}

		w.records.Append(record{
			offset: offset,
			key:    key,
			value:  value,
		})

		offset += 4 + int64(keyLen) + 4 + int64(valLen)
	}

	w.filePos.Store(offset)
	w.flushPos.Store(offset)

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

	if w.mode == SyncMode {
		return w.flushToDisk()
	}

	done := make(chan struct{}, 1)

	select {
	case w.flushCh <- struct{}{}:
	default:
	}

	go func() {
		w.mu.Lock()
		w.mu.Unlock()
		done <- struct{}{}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
