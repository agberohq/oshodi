package pipeline

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/olekukonko/mappo"
	"github.com/zeebo/xxh3"
)

// cacheEntry holds a cached value, the original key for collision verification,
// and an access-timestamp for eviction.
type cacheEntry struct {
	key    []byte // original key for hash collision verification
	val    []byte
	access uint64 // monotonic counter for approximate LRU
}

// Cache provides a high-performance cache for byte slices with size-bounded
// approximate-LRU eviction. It uses hash-based keys (uint64) and a sharded
// map to eliminate string conversions.
type Cache struct {
	data    *mappo.Sharded[uint64, cacheEntry] // sharded map
	Pool    *mappo.BytesPool                   // buffer pool for returning copies
	size    int                                // configured max entries
	access  atomic.Uint64                      // monotonic access counter
	sets    atomic.Uint64                      // set counter for periodic eviction
	evictMu sync.Mutex                         // serializes eviction sweeps
}

// NewCache creates a new cache with the given maximum number of entries.
// If size <= 0, a default of 10000 is used.
func NewCache(size int) *Cache {
	if size <= 0 {
		size = 10000
	}
	return &Cache{
		data: mappo.NewShardedWithConfig[uint64, cacheEntry](mappo.ShardedConfig{
			ShardCount: 128,
		}),
		Pool: mappo.NewBytesPool(1024),
		size: size,
	}
}

// Get retrieves a cached value by key.
// It verifies the stored key matches to guard against hash collisions,
// then returns a copy of the stored slice to prevent external mutation.
// A hit refreshes the entry's access stamp — eviction is LRU, not FIFO.
func (c *Cache) Get(key []byte) ([]byte, bool) {
	h := xxh3.Hash(key)

	var val []byte
	var hit bool
	c.data.Update(h, func(entry cacheEntry, exists bool) cacheEntry {
		if !exists || !bytes.Equal(entry.key, key) {
			return entry
		}
		entry.access = c.access.Add(1)
		val = entry.val
		hit = true
		return entry
	})
	if !hit {
		return nil, false
	}

	// Copy from pool to avoid exposing internal slice.
	buf := c.Pool.Get()
	buf = append(buf[:0], val...)
	return buf, true
}

// Set stores a value in the cache.
// The value is copied to protect against external mutation of the caller's slice.
// Periodically checks capacity and evicts if over limit.
func (c *Cache) Set(key, value []byte) {
	h := xxh3.Hash(key)
	// Copy the key and value to establish clear ownership.
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	c.data.Set(h, cacheEntry{key: keyCopy, val: valCopy, access: c.access.Add(1)})

	// Check eviction every 1024 sets to avoid per-operation overhead.
	if c.sets.Add(1)&1023 == 0 && c.data.Len() > c.size {
		c.evict()
	}
}

// evict removes the ~25% least-recently-accessed entries when over capacity.
// It uses a simple sweep: collect entries below the median access timestamp.
func (c *Cache) evict() {
	c.evictMu.Lock()
	defer c.evictMu.Unlock()

	current := c.data.Len()
	if current <= c.size {
		return
	}

	// Find a cutoff: scan to find the median access value.
	var minAccess, maxAccess uint64
	first := true
	c.data.Range(func(_ uint64, e cacheEntry) bool {
		if first {
			minAccess = e.access
			maxAccess = e.access
			first = false
		} else {
			if e.access < minAccess {
				minAccess = e.access
			}
			if e.access > maxAccess {
				maxAccess = e.access
			}
		}
		return true
	})

	cutoff := minAccess + (maxAccess-minAccess)/2
	target := c.size * 3 / 4 // keep at least 75% of capacity

	// Delete entries below the cutoff until we're under target.
	c.data.Range(func(h uint64, e cacheEntry) bool {
		if c.data.Len() <= target {
			return false
		}
		if e.access < cutoff {
			c.data.Delete(h)
		}
		return true
	})
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
func (c *Cache) Stats() mappo.CacheStats {
	return mappo.CacheStats{
		Size: int64(c.data.Len()),
	}
}

// Close releases resources used by the cache.
func (c *Cache) Close() error {
	return nil
}
