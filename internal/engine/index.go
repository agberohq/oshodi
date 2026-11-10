package engine

import (
	"github.com/agberohq/oshodi/internal/consts"
	"github.com/agberohq/oshodi/internal/metrics"
	"github.com/olekukonko/mappo"
)

// Index manages key to offset mapping using a lock-free sharded map.
// It provides O(1) lookups with no mutex contention.
type Index struct {
	shards *mappo.Sharded[string, int64] // lock-free map
	bloom  *metrics.BloomFilter          // fast negative checks
}

// NewIndex creates a new index with the given Bloom filter parameters.
func NewIndex(expectedItems uint, falsePositiveRate float64) *Index {
	return &Index{
		shards: mappo.NewShardedWithConfig[string, int64](mappo.ShardedConfig{
			ShardCount: consts.IndexShards,
		}),
		bloom: metrics.NewBloomFilter(expectedItems, falsePositiveRate),
	}
}

// Set inserts or updates a key with its storage offset.
func (idx *Index) Set(key []byte, offset int64) {
	idx.shards.Set(string(key), offset)
	idx.bloom.Add(key)
}

// SetIfNewer stores offset only when the key is absent or the incoming
// offset is newer — offsets come from one atomic counter, so a higher
// offset is always a later write. This keeps last-writer-wins true when
// concurrent writers commit index updates outside write order.
func (idx *Index) SetIfNewer(key []byte, offset int64) {
	idx.shards.Update(string(key), func(current int64, exists bool) int64 {
		if exists && current > offset {
			return current
		}
		return offset
	})
	idx.bloom.Add(key)
}

// DeleteIfStale removes the key's index entry unless a newer write
// (offset > tombstoneOffset) is already recorded.
func (idx *Index) DeleteIfStale(key []byte, tombstoneOffset int64) {
	idx.shards.Compute(string(key), func(current int64, exists bool) (int64, bool) {
		if exists && current > tombstoneOffset {
			return current, true // keep: a newer Set beats this tombstone
		}
		return 0, false
	})
}

// Get returns the offset for a key. The boolean indicates existence.
func (idx *Index) Get(key []byte) (int64, bool) {
	return idx.shards.Get(string(key))
}

// Delete removes a key from the index.
func (idx *Index) Delete(key []byte) {
	idx.shards.Delete(string(key))
}

// MaybeHas checks the Bloom filter for potential existence.
func (idx *Index) MaybeHas(key []byte) bool {
	return idx.bloom.MaybeHas(key)
}

// Len returns the total number of keys.
func (idx *Index) Len() int {
	return idx.shards.Len()
}

// Has reports whether a key currently has an index entry.
func (idx *Index) Has(key []byte) bool {
	_, ok := idx.shards.Get(string(key))
	return ok
}

// BloomInsertions returns the number of insertions into the Bloom filter.
func (idx *Index) BloomInsertions() uint64 {
	return idx.bloom.Insertions()
}

// BloomFPR returns the current false positive rate of the Bloom filter.
func (idx *Index) BloomFPR() float64 {
	return idx.bloom.EstimatedFPR()
}

// Range iterates over all keys in arbitrary order.
// The function receives a copy of the key and its offset.
// Return false from the callback to stop iteration.
func (idx *Index) Range(fn func(key []byte, offset int64) bool) {
	idx.shards.Range(func(k string, v int64) bool {
		return fn([]byte(k), v)
	})
}

// Scan iterates over all keys in arbitrary order, supporting error propagation.
// The function receives a copy of the key and its offset.
// Return an error from the callback to abort the scan.
func (idx *Index) Scan(fn func(key []byte, offset int64) error) error {
	var result error
	idx.shards.Range(func(k string, v int64) bool {
		if result != nil {
			return false
		}
		if err := fn([]byte(k), v); err != nil {
			result = err
			return false
		}
		return true
	})
	return result
}

// RangeString iterates like Range but passes the map's own string key. It
// avoids materialising a []byte copy per entry, which is the dominant cost of
// a large scan. The string is immutable and safe to retain.
func (idx *Index) RangeString(fn func(key string, offset int64) bool) {
	idx.shards.Range(fn)
}
