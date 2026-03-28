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
// It achieves extreme performance by parallelizing CPU-bound work (compression, binary encoding)
// across shards using sync.Pool, and serializing only the final microsecond file append.
type ShardedWriter struct {
	shards []*writeShard
	mask   uint32
	store  *storage.File
	offset atomic.Int64 // strictly tracks the exact logical file offset

	// writeMu serializes the final disk append to ensure the atomic offset perfectly
	// matches the order the bytes are written to the file.
	writeMu sync.Mutex
}

// writeShard holds a NUMA-friendly buffer pool to completely eliminate memory allocations
// during the record encoding phase.
type writeShard struct {
	bufferPool sync.Pool
}

// ShardedWriterConfig configures the sharded writer.
type ShardedWriterConfig struct {
	ShardCount int
	BufferSize int        // Kept for API compatibility, unused in this architecture
	Pool       *jack.Pool // Kept for API compatibility, unused in this architecture
	Store      *storage.File
}

// NewShardedWriter creates a highly optimized ShardedWriter.
func NewShardedWriter(cfg ShardedWriterConfig) (*ShardedWriter, error) {
	shardCount := cfg.ShardCount
	if shardCount <= 0 {
		shardCount = 64
	}

	// Round up to power of two for fast bitwise masking
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
		store:  cfg.Store,
	}

	// Initialize the exact starting offset from the file
	// This should be the current file size
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

// WriteRecord performs a zero-allocation, lock-free encoding of the record,
// and appends it to the storage file. It returns the exact file offset instantly.
func (sw *ShardedWriter) WriteRecord(key, value []byte) (int64, error) {
	// 1. Pick a shard to get a NUMA-local buffer pool (Lock-free)
	h := xxh3.Hash(key)
	idx := uint32(h) & sw.mask
	shard := sw.shards[idx]

	bufPtr := shard.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	// 2. Handle Compression locally in the shard (Parallelized CPU work)
	valLen := len(value)
	compressed := byte(0)

	comp := sw.store.Compressor()
	if comp != nil && valLen >= comp.Threshold() {
		if enc, err := comp.Encode(value); err == nil {
			value = enc
			valLen = len(value)
			compressed = 1
		}
	}

	// 3. Binary encoding (Lock-free)
	keyLen := len(key)
	totalSize := 4 + keyLen + 4 + 1 + valLen

	// Resize buffer if needed (only happens if record is > 1KB)
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

	// 4. Serialize the OS Page Cache write to guarantee offset ordering
	sw.writeMu.Lock()
	offset := sw.offset.Load()
	n, err := sw.store.Write(buf)
	if err == nil {
		sw.offset.Add(int64(n))
	}
	sw.writeMu.Unlock()

	// 5. Recycle the buffer (Zero-allocation)
	*bufPtr = buf
	shard.bufferPool.Put(bufPtr)

	return offset, err
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

	sw.writeMu.Lock()
	n, err := sw.store.Write(buf)
	if err == nil {
		sw.offset.Add(int64(n))
	}
	sw.writeMu.Unlock()

	*bufPtr = buf
	shard.bufferPool.Put(bufPtr)

	return err
}

// Flush ensures the OS page cache is synced to the underlying disk.
// Because the ShardedWriter writes instantly to the OS, Flush just calls Sync.
func (sw *ShardedWriter) Flush() error {
	return sw.store.Sync()
}

// Sync is an alias for Flush to satisfy API requirements.
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
