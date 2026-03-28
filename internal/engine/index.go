package engine

import (
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
		shards: mappo.NewSharded[string, int64](),
		bloom:  metrics.NewBloomFilter(expectedItems, falsePositiveRate),
	}
}

// Set inserts or updates a key with its storage offset.
func (idx *Index) Set(key []byte, offset int64) {
	idx.shards.Set(string(key), offset)
	idx.bloom.Add(key)
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
