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

	// Handle Compression (CPU bound, parallelized by shards)
	if comp != nil && valLen >= comp.Threshold() {
		if enc, err := comp.Encode(value); err == nil {
			value = enc
			valLen = len(value)
			compressed = 1
		}
	}

	keyLen := len(key)
	payloadSize := 4 + keyLen + 4 + 1 + valLen
	totalSize := payloadSize + storage.RecordCRCSize

	// Zero-length values carry RecordFlagEmptyValue.
	flag := compressed
	if valLen == 0 {
		flag = storage.RecordFlagEmptyValue
	}

	// Expand buffer if the record is larger than 1KB
	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	// Binary Formatting
	pos := 0
	binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
	pos += 4
	copy(buf[pos:], key)
	pos += keyLen
	binary.BigEndian.PutUint32(buf[pos:], uint32(valLen))
	pos += 4
	buf[pos] = flag
	pos += 1
	pos += copy(buf[pos:], value)
	binary.BigEndian.PutUint32(buf[pos:], storage.RecordChecksum(buf[:payloadSize]))

	// Lock-free write via Offset Reservation Pattern
	offset, err := sw.store.Load().WriteAtomic(buf)

	// Zero-allocation recycle
	*bufPtr = buf[:0]
	shard.bufferPool.Put(bufPtr)

	return offset, err
}

// BatchWriteRecord heavily optimizes throughput by merging multiple records into a single WriteAtomic OS Syscall.
// Compressed values are staged locally — the caller's entries are never mutated.
func (sw *ShardedWriter) BatchWriteRecord(entries []KV) ([]int64, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	bufPtr := sw.encoderPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	comp := sw.store.Load().Compressor()
	var totalSize int

	// Stage compressed values in a local slice (nil = not compressed).
	// Writing into entries[i].Value would hand the caller back compressed
	// bytes in place of its own data — never mutate caller-owned slices.
	compressedVals := make([][]byte, len(entries))
	flags := make([]byte, len(entries))

	for i := range entries {
		keyLen := len(entries[i].Key)
		valLen := len(entries[i].Value)

		flag := byte(0)
		if comp != nil && valLen >= comp.Threshold() {
			if enc, err := comp.Encode(entries[i].Value); err == nil {
				compressedVals[i] = enc
				valLen = len(enc)
				flag = 1
			}
		}
		if valLen == 0 {
			// Zero-length values carry RecordFlagEmptyValue.
			flag = storage.RecordFlagEmptyValue
		}
		flags[i] = flag
		totalSize += 4 + keyLen + 4 + 1 + valLen + storage.RecordCRCSize
	}

	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	pos := 0
	offsets := make([]int64, len(entries))

	for i := range entries {
		keyLen := len(entries[i].Key)
		staged := compressedVals[i] != nil // compressed; empty-value marker (0x01) has nothing staged
		valLen := len(entries[i].Value)
		if staged {
			valLen = len(compressedVals[i])
		}

		binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
		pos += 4
		copy(buf[pos:], entries[i].Key)
		pos += keyLen
		binary.BigEndian.PutUint32(buf[pos:], uint32(valLen))
		pos += 4
		buf[pos] = flags[i]
		pos += 1

		if staged {
			copy(buf[pos:], compressedVals[i])
		} else {
			copy(buf[pos:], entries[i].Value)
		}
		pos += valLen

		// Per-record CRC trailer over this record's serialised payload.
		recordStart := pos - valLen - 1 - 4 - keyLen - 4
		binary.BigEndian.PutUint32(buf[pos:], storage.RecordChecksum(buf[recordStart:pos]))
		pos += storage.RecordCRCSize
	}

	// Single lock-free syscall for the entire batch!
	baseOffset, err := sw.store.Load().WriteAtomic(buf)
	if err != nil {
		return nil, err
	}

	// Calculate individual logical offsets for the index
	currentOffset := baseOffset
	for i := range entries {
		keyLen := len(entries[i].Key)
		staged := compressedVals[i] != nil
		valLen := len(entries[i].Value)
		if staged {
			valLen = len(compressedVals[i])
		}
		offsets[i] = currentOffset
		currentOffset += int64(4 + keyLen + 4 + 1 + valLen + storage.RecordCRCSize)
	}

	*bufPtr = buf[:0]
	sw.encoderPool.Put(bufPtr)

	return offsets, nil
}

// WriteTombstone creates a deletion marker lock-free and appends it to storage.
// It returns the tombstone record's file offset so callers can retire the
// index entry in offset order (set-after-delete races).
func (sw *ShardedWriter) WriteTombstone(key []byte) (int64, error) {
	h := xxh3.Hash(key)
	idx := uint32(h) & sw.mask
	shard := sw.shards[idx]

	bufPtr := shard.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	keyLen := len(key)
	payloadSize := 4 + keyLen + 4 + 1
	totalSize := payloadSize + storage.RecordCRCSize

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
	binary.BigEndian.PutUint32(buf[pos:], 0) // zero value length
	pos += 4
	buf[pos] = storage.RecordFlagTombstone
	pos += 1
	binary.BigEndian.PutUint32(buf[pos:], storage.RecordChecksum(buf[:payloadSize]))

	offset, err := sw.store.Load().WriteAtomic(buf)

	*bufPtr = buf[:0]
	shard.bufferPool.Put(bufPtr)

	return offset, err
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

// BatchWriteTombstones appends deletion markers for every key in one atomic
// write and returns each marker's file offset, so callers can retire index
// entries in offset order.
func (sw *ShardedWriter) BatchWriteTombstones(keys [][]byte) ([]int64, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	bufPtr := sw.encoderPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	totalSize := 0
	for _, k := range keys {
		totalSize += 4 + len(k) + 4 + 1 + storage.RecordCRCSize
	}
	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	pos := 0
	for _, k := range keys {
		keyLen := len(k)
		payloadSize := 4 + keyLen + 4 + 1
		binary.BigEndian.PutUint32(buf[pos:], uint32(keyLen))
		pos += 4
		copy(buf[pos:], k)
		pos += keyLen
		binary.BigEndian.PutUint32(buf[pos:], 0) // zero value length
		pos += 4
		buf[pos] = storage.RecordFlagTombstone
		pos++
		binary.BigEndian.PutUint32(buf[pos:], storage.RecordChecksum(buf[pos-payloadSize:pos]))
		pos += storage.RecordCRCSize
	}

	baseOffset, err := sw.store.Load().WriteAtomic(buf)
	if err != nil {
		*bufPtr = buf[:0]
		sw.encoderPool.Put(bufPtr)
		return nil, err
	}

	offsets := make([]int64, len(keys))
	cur := baseOffset
	for i, k := range keys {
		offsets[i] = cur
		cur += int64(4 + len(k) + 4 + 1 + storage.RecordCRCSize)
	}

	*bufPtr = buf[:0]
	sw.encoderPool.Put(bufPtr)
	return offsets, nil
}
