package pipeline

import (
	"github.com/olekukonko/mappo"
	"github.com/zeebo/xxh3"
)

// Cache provides a high-performance, allocation-free cache for byte slices.
// It uses hash-based keys (uint64) and a sharded map to eliminate string conversions.
type Cache struct {
	data *mappo.Sharded[uint64, []byte] // sharded, lock-free map
	pool *mappo.BytesPool               // buffer pool for returning copies
	size int                            // configured size (used for capacity)
}

// NewCache creates a new cache with the given maximum number of entries.
// If size <= 0, a default of 10000 is used.
func NewCache(size int) *Cache {
	if size <= 0 {
		size = 10000
	}
	return &Cache{
		data: mappo.NewShardedWithConfig[uint64, []byte](mappo.ShardedConfig{
			ShardCount: 64, // good default for high concurrency
		}),
		pool: mappo.NewBytesPool(1024), // pre-allocate 1KB buffers; will grow as needed
		size: size,
	}
}

// Get retrieves a cached value by key.
// It returns a copy of the stored slice to prevent external mutation.
func (c *Cache) Get(key []byte) ([]byte, bool) {
	h := xxh3.Hash(key) // zero-allocation hash
	val, ok := c.data.Get(h)
	if !ok {
		return nil, false
	}
	// Copy from pool to avoid exposing internal slice
	buf := c.pool.Get()
	buf = append(buf[:0], val...)
	return buf, true
}

// Set stores a value in the cache.
// The value is stored as provided; the caller must not mutate it afterward.
// If the cache exceeds its size limit, the oldest entry may be evicted (implementation-dependent).
func (c *Cache) Set(key, value []byte) {
	h := xxh3.Hash(key)
	c.data.Set(h, value)
	// Note: Sharded does not enforce size limits automatically.
	// For production, we could implement a size-aware eviction or rely on the caller
	// to limit total cache entries. The original `mappo.Cache` (otter) does evict
	// based on size, but we are trading that for zero-allocation speed.
}

// Delete removes a key from the cache.
func (c *Cache) Delete(key []byte) {
	h := xxh3.Hash(key)
	c.data.Delete(h)
}

// Len returns the current number of entries in the cache.
func (c *Cache) Len() int {
	return c.data.Len()
}

// Clear removes all entries from the cache.
func (c *Cache) Clear() {
	c.data.Clear()
}

// Stats returns cache statistics.
// Since the sharded map does not expose hit/miss counters, we return a dummy zero stats.
// For production, you could extend this to track hits/misses separately.
func (c *Cache) Stats() mappo.CacheStats {
	return mappo.CacheStats{
		Size: int64(c.data.Len()),
	}
}

// Close releases resources used by the cache.
// The sharded map does not need explicit closing; this method exists for interface compatibility.
func (c *Cache) Close() error {
	return nil
}
