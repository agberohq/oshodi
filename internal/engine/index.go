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

// Tombstone watermarks: a negative stored value records the tombstone of a
// deleted key as -(offset+1), so an in-flight Set whose index update has not
// landed yet cannot resurrect the key once the delete has committed. Live
// offsets are always >= 0, so the sign is unambiguous.

func tombstoneMark(tombstoneOffset int64) int64 { return -(tombstoneOffset + 1) }

func markToTombstone(mark int64) int64 { return -mark - 1 }

// newerThan reports whether the stored value represents a write newer than
// offset. Offsets come from one monotonic counter, so a higher offset is
// always a later write.
func newerThan(stored, offset int64) bool {
	if stored < 0 {
		return markToTombstone(stored) > offset
	}
	return stored > offset
}

// SetIfNewer stores offset only when the key is absent or the incoming
// offset is newer. This keeps last-writer-wins true when concurrent writers
// commit index updates outside write order, and refuses offsets that predate
// a tombstone watermark recorded by DeleteIfStale.
func (idx *Index) SetIfNewer(key []byte, offset int64) {
	idx.shards.Update(string(key), func(current int64, exists bool) int64 {
		if exists && newerThan(current, offset) {
			return current
		}
		return offset
	})
	idx.bloom.Add(key)
}

// noOffset is returned by the publish helpers when a key had no previous
// version. Offset 0 is a valid record offset, so a zero value cannot be used as
// the sentinel.
const noOffset int64 = -1

// PublishOffset installs offset as the key's head if no newer offset is
// recorded, and returns the offset it displaced. The displaced offset is the
// exact previous version, so a snapshot can still reach it. A false return
// means a newer write already won and the caller's record is dead space.
func (idx *Index) PublishOffset(key []byte, offset int64) (prev int64, won bool) {
	k := string(key)
	for {
		cur, exists := idx.shards.Get(k)
		if exists && newerThan(cur, offset) {
			return noOffset, false
		}
		if !exists {
			if _, loaded := idx.shards.SetIfAbsent(k, offset); loaded {
				continue
			}
			idx.bloom.Add(key)
			return noOffset, true
		}
		// A tombstone watermark is stored negated; the history records the
		// tombstone's own offset so the reader can see the delete.
		prevOffset := cur
		if cur < 0 {
			prevOffset = markToTombstone(cur)
		}
		if idx.shards.CompareAndSwap(k, cur, offset) {
			idx.bloom.Add(key)
			return prevOffset, true
		}
	}
}

// PublishDelete installs a tombstone watermark for key, returning the live
// offset it displaced so a snapshot taken before the delete still reads the
// deleted value. A false return means a newer write already won.
func (idx *Index) PublishDelete(key []byte, tombstoneOffset int64) (prev int64, won bool) {
	k := string(key)
	mark := tombstoneMark(tombstoneOffset)
	for {
		cur, exists := idx.shards.Get(k)
		if exists && newerThan(cur, tombstoneOffset) {
			return noOffset, false
		}
		if !exists {
			if _, loaded := idx.shards.SetIfAbsent(k, mark); loaded {
				continue
			}
			return noOffset, true
		}
		prevOffset := cur
		if cur < 0 {
			prevOffset = markToTombstone(cur)
		}
		if idx.shards.CompareAndSwap(k, cur, mark) {
			return prevOffset, true
		}
	}
}

// DeleteIfStale retires the key's index entry unless a newer write
// (offset > tombstoneOffset) is already recorded. The tombstone is recorded
// as a negative watermark either way, so a later stale SetIfNewer cannot
// resurrect the key.
func (idx *Index) DeleteIfStale(key []byte, tombstoneOffset int64) {
	idx.shards.Update(string(key), func(current int64, exists bool) int64 {
		if exists && newerThan(current, tombstoneOffset) {
			return current // a newer write beats this tombstone
		}
		return tombstoneMark(tombstoneOffset)
	})
}

// Get returns the offset for a key. The boolean indicates existence;
// tombstone watermarks read as absent.
func (idx *Index) Get(key []byte) (int64, bool) {
	return idx.get(string(key))
}

// GetString is Get for callers that already hold the map's string key.
func (idx *Index) GetString(key string) (int64, bool) {
	return idx.get(key)
}

// GetRaw returns the stored index value as-is, including the negative
// tombstone watermark. Snapshot reads need it: a tombstone is a version, and
// the version behind it is still reachable through history.
func (idx *Index) GetRaw(key string) (int64, bool) {
	return idx.shards.Get(key)
}

func (idx *Index) get(key string) (int64, bool) {
	v, ok := idx.shards.Get(key)
	if ok && v < 0 {
		return 0, false
	}
	return v, ok
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

// Has reports whether a key currently has a live index entry.
func (idx *Index) Has(key []byte) bool {
	_, ok := idx.get(string(key))
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

// Range iterates over all live keys in arbitrary order. The function
// receives a copy of the key and its offset. Return false from the callback
// to stop iteration. Tombstone watermarks are skipped.
func (idx *Index) Range(fn func(key []byte, offset int64) bool) {
	idx.shards.Range(func(k string, v int64) bool {
		if v < 0 {
			return true
		}
		return fn([]byte(k), v)
	})
}

// Scan iterates over all live keys in arbitrary order, supporting error
// propagation. The function receives a copy of the key and its offset.
// Return an error from the callback to abort the scan. Tombstone watermarks
// are skipped.
func (idx *Index) Scan(fn func(key []byte, offset int64) error) error {
	var result error
	idx.shards.Range(func(k string, v int64) bool {
		if result != nil {
			return false
		}
		if v < 0 {
			return true
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
// a large scan. The string is immutable and safe to retain. Tombstone
// watermarks are skipped.
// RangeRaw visits every key with its raw index value, including negative
// tombstone watermarks. Snapshot scans need those: a key deleted after the
// snapshot's watermark still holds a value at that watermark.
func (idx *Index) RangeRaw(fn func(key string, mark int64) bool) {
	idx.shards.Range(fn)
}

func (idx *Index) RangeString(fn func(key string, offset int64) bool) {
	idx.shards.Range(func(k string, v int64) bool {
		if v < 0 {
			return true
		}
		return fn(k, v)
	})
}
