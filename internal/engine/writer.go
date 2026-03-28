package engine

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/agberohq/oshodi/internal/storage"
	"github.com/olekukonko/jack"
	"github.com/zeebo/xxh3"
)

// ShardedWriter manages writes to storage with per-shard buffer pooling.
// Note: Currently uses a single write lock for offset atomicity. Future iteration
// will use offset reservation for true lock-free concurrent writes.
type ShardedWriter struct {
	shards      []*writeShard
	mask        uint32
	store       atomic.Pointer[storage.File]
	offset      atomic.Int64
	writeMu     sync.Mutex // Protects offset allocation and store writes
	encoderPool sync.Pool  // Pool for encoding buffers
}

type writeShard struct {
	bufferPool sync.Pool
}

// ShardedWriterConfig configures the sharded writer.
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
	// Round up to power of 2 for fast modulo via bitmask
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
				b := make([]byte, 0, 4096)
				return &b
			},
		},
	}
	sw.store.Store(cfg.Store)
	sw.offset.Store(cfg.Store.LogicalSize())

	for i := range sw.shards {
		sw.shards[i] = &writeShard{
			bufferPool: sync.Pool{
				New: func() interface{} {
					b := make([]byte, 0, 1024)
					return &b
				},
			},
		}
	}
	return sw, nil
}

// WriteRecord performs zero-allocation encoding and writes to storage.
func (sw *ShardedWriter) WriteRecord(key, value []byte) (int64, error) {
	h := xxh3.Hash(key)
	idx := uint32(h) & sw.mask
	shard := sw.shards[idx]

	bufPtr := shard.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	valLen := len(value)
	compressed := byte(0)
	comp := sw.store.Load().Compressor()

	if comp != nil && valLen >= comp.Threshold() {
		if enc, err := comp.Encode(value); err == nil {
			value = enc
			valLen = len(value)
			compressed = 1
		}
	}

	keyLen := len(key)
	totalSize := 4 + keyLen + 4 + 1 + valLen

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
	binary.BigEndian.PutUint32(buf[pos:], uint32(valLen))
	pos += 4
	buf[pos] = compressed
	pos += 1
	copy(buf[pos:], value)

	sw.writeMu.Lock()
	offset := sw.offset.Load()
	n, err := sw.store.Load().Write(buf)
	if err == nil {
		sw.offset.Add(int64(n))
	}
	sw.writeMu.Unlock()

	*bufPtr = buf[:0] // Reset but keep capacity
	shard.bufferPool.Put(bufPtr)

	return offset, err
}

// WriteTombstone creates a deletion marker.
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
	binary.BigEndian.PutUint32(buf[pos:], 0)

	sw.writeMu.Lock()
	n, err := sw.store.Load().Write(buf)
	if err == nil {
		sw.offset.Add(int64(n))
	}
	sw.writeMu.Unlock()

	*bufPtr = buf[:0]
	shard.bufferPool.Put(bufPtr)

	return err
}

// Flush ensures the OS page cache is synced.
func (sw *ShardedWriter) Flush() error {
	return sw.store.Load().Sync()
}

// Sync is an alias for Flush.
func (sw *ShardedWriter) Sync() error {
	return sw.Flush()
}

// Close flushes data.
func (sw *ShardedWriter) Close() error {
	return sw.Flush()
}

// Offset returns the current logical file offset.
func (sw *ShardedWriter) Offset() int64 {
	return sw.offset.Load()
}
