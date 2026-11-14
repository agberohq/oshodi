package engine

import (
	"bytes"
	"sort"
	"strings"
)

// orderedSnapshot is a sorted view of one bucket's index keys at a generation.
// It does not copy key bytes: the strings are owned by the index map and remain
// valid while the snapshot references them.
type orderedSnapshot struct {
	gen  uint64
	keys []string
}

// bucketPrefixOf derives the bucket prefix ("name\x00") from a full key. Root
// keys without a separator use the empty prefix.
func bucketPrefixOf(key []byte) string {
	if i := bytes.IndexByte(key, 0); i >= 0 {
		return string(key[:i+1])
	}
	return ""
}

// invalidateOrderedKeys marks the affected bucket snapshots stale, plus the
// root (all-keys) view. Keys from different buckets only invalidate their own
// snapshot, so a write to "stats" does not force a rebuild for "songs".
func (db *DB) invalidateOrderedKeys(keys ...[]byte) {
	db.orderedMu.Lock()
	defer db.orderedMu.Unlock()
	if db.orderedGens == nil {
		db.orderedGens = make(map[string]uint64)
	}
	seen := make(map[string]struct{}, 2)
	for _, key := range keys {
		p := bucketPrefixOf(key)
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		db.orderedGens[p]++
	}
	if _, ok := seen[""]; !ok {
		db.orderedGens[""]++
	}
}

// invalidateAllOrderedKeys drops every snapshot; compaction swaps all offsets
// and replaces the index.
func (db *DB) invalidateAllOrderedKeys() {
	db.orderedMu.Lock()
	db.orderedSnaps = nil
	db.orderedGens = nil
	db.orderedMu.Unlock()
}

// orderedKeys returns one bucket's index keys in bytewise order, rebuilding
// only that bucket's snapshot when writes have occurred. The build is O(B log
// B) for B keys in the bucket; readers of other buckets are unaffected.
func (db *DB) orderedKeys(prefix string) []string {
	db.orderedMu.Lock()
	gen := db.orderedGens[prefix]
	if snap := db.orderedSnaps[prefix]; snap != nil && snap.gen == gen {
		keys := snap.keys
		db.orderedMu.Unlock()
		return keys
	}
	db.orderedMu.Unlock()

	idx := db.index.Load()
	keys := make([]string, 0, idx.Len())
	if prefix == "" {
		idx.RangeString(func(k string, _ int64) bool {
			keys = append(keys, k)
			return true
		})
	} else {
		idx.RangeString(func(k string, _ int64) bool {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
			return true
		})
	}
	sort.Strings(keys)

	db.orderedMu.Lock()
	if db.orderedSnaps == nil {
		db.orderedSnaps = make(map[string]*orderedSnapshot)
	}
	// If writes raced the build, keep the snapshot tagged with the generation
	// it started from; it will not be considered fresh, but this query can use
	// the keys it already has (weakly consistent, like other scans).
	db.orderedSnaps[prefix] = &orderedSnapshot{gen: gen, keys: keys}
	db.orderedMu.Unlock()
	return keys
}
