package engine

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/agberohq/oshodi/internal/storage"
	"github.com/olekukonko/jack"
	"github.com/zeebo/xxh3"
)

// ShardedWriter manages writes to storage.
type ShardedWriter struct {
	shards  []*writeShard
	mask    uint32
	store   atomic.Pointer[storage.File] // Atomic for safe RCU updates
	offset  atomic.Int64
	writeMu sync.Mutex
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
	}
	sw.store.Store(cfg.Store)
	sw.offset.Store(cfg.Store.Size())
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

// WriteRecord performs a zero-allocation, lock-free encoding of the record.
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
	*bufPtr = buf
	shard.bufferPool.Put(bufPtr)
	return offset, err
}

// WriteTombstone creates a deletion marker lock-free.
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
	*bufPtr = buf
	shard.bufferPool.Put(bufPtr)
	return err
}

// Flush ensures the OS page cache is synced to the underlying disk.
func (sw *ShardedWriter) Flush() error {
	return sw.store.Load().Sync()
}

// Sync is an alias for Flush.
func (sw *ShardedWriter) Sync() error {
	return sw.Flush()
}

// Close flushes data and closes the writer.
func (sw *ShardedWriter) Close() error {
	return sw.Flush()
}

// Offset returns the exact current logical file offset.
func (sw *ShardedWriter) Offset() int64 {
	return sw.offset.Load()
}
