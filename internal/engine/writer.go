package engine

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/agberohq/oshodi/internal/storage"
	"github.com/olekukonko/jack"
	"github.com/zeebo/xxh3"
)

// ShardedWriter parallelizes CPU-bound encoding (compression, formatting) across shards.
// It relies on the lock-free Offset Reservation Pattern in storage.File for actual disk writes.
type ShardedWriter struct {
	shards      []*writeShard
	mask        uint32
	store       atomic.Pointer[storage.File]
	encoderPool sync.Pool // Pool for large encoding buffers used in Batching
}

type writeShard struct {
	bufferPool sync.Pool
}

type ShardedWriterConfig struct {
	ShardCount int
	BufferSize int
	Pool       *jack.Pool
	Store      *storage.File
}

// NewShardedWriter creates a highly optimized ShardedWriter.
func NewShardedWriter(cfg ShardedWriterConfig) (*ShardedWriter, error) {
	shardCount := cfg.ShardCount
	if shardCount <= 0 {
		shardCount = 64
	}

	// Round up to power of 2 for fast bitwise masking
	shardCount--
	shardCount |= shardCount >> 1
	shardCount |= shardCount >> 2
	shardCount |= shardCount >> 4
	shardCount |= shardCount >> 8
	shardCount |= shardCount >> 16
	shardCount++

	sw := &ShardedWriter{
		shards: make([]*writeShard, shardCount),
		mask:   uint32(shardCount - 1),
		encoderPool: sync.Pool{
			New: func() interface{} {
				// 32KB pre-allocation for Batch writes to eliminate mallocs
				b := make([]byte, 0, 32768)
				return &b
			},
		},
	}
	sw.store.Store(cfg.Store)

	for i := range sw.shards {
		sw.shards[i] = &writeShard{
			bufferPool: sync.Pool{
				New: func() interface{} {
					// 1KB pre-allocation for single record writes
					b := make([]byte, 0, 1024)
					return &b
				},
			},
		}
	}
	return sw, nil
}

// WriteRecord performs zero-allocation encoding and lock-free offset reservation.
func (sw *ShardedWriter) WriteRecord(key, value []byte) (int64, error) {
	h := xxh3.Hash(key)
	idx := uint32(h) & sw.mask
	shard := sw.shards[idx]

	bufPtr := shard.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	valLen := len(value)
	compressed := byte(0)
	comp := sw.store.Load().Compressor()

	// 1. Handle Compression (CPU bound, parallelized by shards)
	if comp != nil && valLen >= comp.Threshold() {
		if enc, err := comp.Encode(value); err == nil {
			value = enc
			valLen = len(value)
			compressed = 1
		}
	}

	keyLen := len(key)
	totalSize := 4 + keyLen + 4 + 1 + valLen

	// 2. Expand buffer if the record is larger than 1KB
	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	// 3. Binary Formatting
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

	// 4. Lock-free write via Offset Reservation Pattern
	offset, err := sw.store.Load().WriteAtomic(buf)

	// 5. Zero-allocation recycle
	*bufPtr = buf[:0]
	shard.bufferPool.Put(bufPtr)

	return offset, err
}

// BatchWriteRecord heavily optimizes throughput by merging multiple records into a single WriteAtomic OS Syscall.
func (sw *ShardedWriter) BatchWriteRecord(entries []KV) ([]int64, error) {
	bufPtr := sw.encoderPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	comp := sw.store.Load().Compressor()
	var totalSize int

	// Pre-calculate total size and compress in-place
	for i := range entries {
		valLen := len(entries[i].Value)
		if comp != nil && valLen >= comp.Threshold() {
			if enc, err := comp.Encode(entries[i].Value); err == nil {
				entries[i].Value = enc
			}
		}
		totalSize += 4 + len(entries[i].Key) + 4 + 1 + len(entries[i].Value)
	}

	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	pos := 0
	offsets := make([]int64, len(entries))

	for _, entry := range entries {
		keyLen := len(entry.Key)
		valLen := len(entry.Value)

		binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
		pos += 4
		copy(buf[pos:], entry.Key)
		pos += keyLen
		binary.BigEndian.PutUint32(buf[pos:], uint32(valLen))
		pos += 4
		buf[pos] = 0 // Compression already applied to entry.Value if needed
		pos += 1
		copy(buf[pos:], entry.Value)
		pos += valLen
	}

	// Single lock-free syscall for the entire batch!
	baseOffset, err := sw.store.Load().WriteAtomic(buf)
	if err != nil {
		return nil, err
	}

	// Calculate individual logical offsets for the index
	currentOffset := baseOffset
	for i, entry := range entries {
		offsets[i] = currentOffset
		currentOffset += int64(4 + len(entry.Key) + 4 + 1 + len(entry.Value))
	}

	*bufPtr = buf[:0]
	sw.encoderPool.Put(bufPtr)

	return offsets, nil
}

// WriteTombstone creates a deletion marker lock-free and appends it to storage.
func (sw *ShardedWriter) WriteTombstone(key []byte) error {
	h := xxh3.Hash(key)
	idx := uint32(h) & sw.mask
	shard := sw.shards[idx]

	bufPtr := shard.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	keyLen := len(key)
	totalSize := 4 + keyLen + 4

	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	pos := 0
	binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
	pos += 4
	copy(buf[pos:], key)
	pos += keyLen
	binary.BigEndian.PutUint32(buf[pos:], 0) // zero value length marks tombstone

	_, err := sw.store.Load().WriteAtomic(buf)

	*bufPtr = buf[:0]
	shard.bufferPool.Put(bufPtr)

	return err
}

// Flush ensures the OS page cache is synced to the underlying disk.
func (sw *ShardedWriter) Flush() error {
	return sw.store.Load().Sync()
}

// Sync is an alias for Flush to satisfy API requirements.
func (sw *ShardedWriter) Sync() error {
	return sw.Flush()
}

// Close flushes data and closes the writer.
func (sw *ShardedWriter) Close() error {
	return sw.Flush()
}

// Offset returns the exact current logical file offset from the storage file.
func (sw *ShardedWriter) Offset() int64 {
	return sw.store.Load().LogicalSize()
}
