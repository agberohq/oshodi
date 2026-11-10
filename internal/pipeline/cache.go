package pipeline

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/agberohq/oshodi/internal/consts"
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
		size = consts.DefaultCacheSize
	}
	return &Cache{
		data: mappo.NewShardedWithConfig[uint64, cacheEntry](mappo.ShardedConfig{
			ShardCount: consts.CacheShards,
		}),
		Pool: mappo.NewBytesPool(consts.CacheBufferSize),
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

	// Copy to avoid exposing the internal slice. The caller owns this copy,
	// so it cannot come from a pool: nothing would return it. Sizing it to
	// the value also avoids handing out a full 1 KiB pool buffer per hit.
	out := make([]byte, len(val))
	copy(out, val)
	return out, true
}

// View retrieves a cached value and passes it to fn without copying. The
// slice is the cache's own immutable buffer; fn must not retain or mutate it.
// fn runs after the shard lock is released, so it may call back into the cache
// or the database. ok reports whether the key was present.
func (c *Cache) View(key []byte, fn func(value []byte) error) (ok bool, err error) {
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
		return false, nil
	}
	return true, fn(val)
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
	if c.sets.Add(1)&consts.CacheEvictCheckMask == 0 && c.data.Len() > c.size {
		c.evict()
	}
}

// evict removes the least-recently-accessed entries until the cache is at
// 75% of capacity. It samples a bounded number of entries per pass instead of
// sorting the whole cache: full-cache sorting dominated churn profiles.
// Deleting from successive samples approximately preserves LRU while keeping
// each eviction O(sample log sample) plus the number of victims.
func (c *Cache) evict() {
	c.evictMu.Lock()
	defer c.evictMu.Unlock()

	current := c.data.Len()
	if current <= c.size {
		return
	}
	target := c.size * consts.CacheEvictTargetNum / consts.CacheEvictTargetDen
	need := current - target

	type victim struct {
		hash   uint64
		access uint64
	}
	// Scale the sample with the cache: a fixed 512 is fine for ~10k
	// entries but would need thousands of passes on a large cache, while a
	// huge sample reintroduces full sorts on small ones.
	sampleSize := c.size / consts.CacheEvictSampleDivisor
	if sampleSize < consts.CacheEvictSampleMin {
		sampleSize = consts.CacheEvictSampleMin
	}
	if sampleSize > consts.CacheEvictSampleMax {
		sampleSize = consts.CacheEvictSampleMax
	}
	victims := make([]victim, 0, sampleSize)
	for need > 0 {
		victims = victims[:0]
		c.data.Range(func(h uint64, e cacheEntry) bool {
			victims = append(victims, victim{hash: h, access: e.access})
			return len(victims) < sampleSize
		})
		if len(victims) == 0 {
			return
		}
		sort.Slice(victims, func(i, j int) bool {
			return victims[i].access < victims[j].access
		})
		take := need
		if take > len(victims) {
			take = len(victims)
		}
		for i := 0; i < take; i++ {
			c.data.Delete(victims[i].hash)
		}
		need -= take
		if len(victims) < sampleSize {
			// The whole cache fit in one sample; capacity is satisfied.
			return
		}
	}
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
