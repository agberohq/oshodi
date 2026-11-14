package engine

import (
	"errors"
	"sort"
	"strings"

	"github.com/agberohq/oshodi/internal/storage"
)

// ScanRange iterates over keys in [start, end) in bytewise key order within the
// bucket. A nil start means the beginning of the bucket; a nil end means the
// end of the bucket. key and value are only valid until fn returns.
func (b *Bucket) ScanRange(start, end []byte, fn func(key, value []byte) bool) error {
	return b.scanOrdered(start, end, false, fn)
}

// ScanRangeDesc is ScanRange in reverse key order. start and end keep the same
// half-open meaning, so the last key yielded is >= start.
func (b *Bucket) ScanRangeDesc(start, end []byte, fn func(key, value []byte) bool) error {
	return b.scanOrdered(start, end, true, fn)
}

// ScanPrefix iterates over keys beginning with prefix in bytewise order.
func (b *Bucket) ScanPrefix(prefix []byte, fn func(key, value []byte) bool) error {
	return b.scanOrdered(prefix, prefixUpperBound(prefix), false, fn)
}

// ScanPrefixDesc is ScanPrefix in reverse key order.
func (b *Bucket) ScanPrefixDesc(prefix []byte, fn func(key, value []byte) bool) error {
	return b.scanOrdered(prefix, prefixUpperBound(prefix), true, fn)
}

// ScanRangeKeys is ScanRange without reading values.
func (b *Bucket) ScanRangeKeys(start, end []byte, fn func(key []byte) bool) error {
	return b.scanOrderedKeys(start, end, false, func(fullKey []byte, _ string) bool {
		return fn(fullKey[len(b.prefix):])
	})
}

// ScanRangeKeysDesc is ScanRangeKeys in reverse key order.
func (b *Bucket) ScanRangeKeysDesc(start, end []byte, fn func(key []byte) bool) error {
	return b.scanOrderedKeys(start, end, true, func(fullKey []byte, _ string) bool {
		return fn(fullKey[len(b.prefix):])
	})
}

// ScanPrefixKeys is ScanPrefix without reading values.
func (b *Bucket) ScanPrefixKeys(prefix []byte, fn func(key []byte) bool) error {
	return b.scanOrderedKeys(prefix, prefixUpperBound(prefix), false, func(fullKey []byte, _ string) bool {
		return fn(fullKey[len(b.prefix):])
	})
}

// ScanPrefixKeysDesc is ScanPrefixKeys in reverse key order.
func (b *Bucket) ScanPrefixKeysDesc(prefix []byte, fn func(key []byte) bool) error {
	return b.scanOrderedKeys(prefix, prefixUpperBound(prefix), true, func(fullKey []byte, _ string) bool {
		return fn(fullKey[len(b.prefix):])
	})
}

func (b *Bucket) scanOrdered(userStart, userEnd []byte, desc bool, fn func(key, value []byte) bool) error {
	var valBuf []byte
	var readErr error
	err := b.scanOrderedKeys(userStart, userEnd, desc, func(fullKey []byte, k string) bool {
		val, found, err := b.readOrderedKey(k, fullKey, valBuf)
		if err != nil {
			readErr = err
			return false
		}
		if !found {
			return true
		}
		if cap(val) > cap(valBuf) {
			valBuf = val[:cap(val)]
		}
		return fn(fullKey[len(b.prefix):], val)
	})
	if err != nil {
		return err
	}
	return readErr
}

// scanOrderedKeys walks the sorted key snapshot within [start,end), invoking fn
// with the reusable full-key buffer and the snapshot's key string.
func (b *Bucket) scanOrderedKeys(userStart, userEnd []byte, desc bool, fn func(fullKey []byte, key string) bool) error {
	if b.db.closed.Load() {
		return ErrDBClosed
	}

	startFull := append(append([]byte(nil), b.prefix...), userStart...)
	var endFull []byte
	if userEnd != nil {
		endFull = append(append([]byte(nil), b.prefix...), userEnd...)
	} else if len(b.prefix) > 0 {
		endFull = prefixUpperBound(b.prefix)
	}
	startS := string(startFull)
	endS := ""
	if endFull != nil {
		endS = string(endFull)
	}
	bucketPrefix := string(b.prefix)

	keys := b.db.orderedKeys(string(b.prefix))
	var fullKey []byte
	visit := func(k string) bool {
		fullKey = append(fullKey[:0], k...)
		return fn(fullKey, k)
	}

	if desc {
		hi := len(keys)
		if endS != "" {
			hi = sort.Search(len(keys), func(i int) bool { return keys[i] >= endS })
		}
		for i := hi - 1; i >= 0; i-- {
			k := keys[i]
			if len(bucketPrefix) > 0 && !strings.HasPrefix(k, bucketPrefix) {
				if k > bucketPrefix {
					continue
				}
				break
			}
			if k < startS {
				break
			}
			if !visit(k) {
				return nil
			}
		}
		return nil
	}

	i := sort.Search(len(keys), func(i int) bool { return keys[i] >= startS })
	for ; i < len(keys); i++ {
		k := keys[i]
		if endS != "" && k >= endS {
			break
		}
		if len(bucketPrefix) > 0 && !strings.HasPrefix(k, bucketPrefix) {
			break
		}
		if !visit(k) {
			return nil
		}
	}
	return nil
}

// readOrderedKey resolves the current offset for a snapshot key and reads its
// value, retrying across a compaction's index/storage swap.
func (b *Bucket) readOrderedKey(k string, fullKey, valBuf []byte) (value []byte, found bool, err error) {
	for attempt := 0; attempt < 3; attempt++ {
		idx := b.db.index.Load()
		off, ok := idx.GetString(k)
		if !ok {
			return nil, false, nil // deleted since the snapshot was built
		}
		store := b.db.storage.Load()
		val, readErr := store.ReadValueAt(off, fullKey, valBuf)
		if readErr == nil {
			return val, true, nil
		}
		if !errors.Is(readErr, storage.ErrCorrupt) && !errors.Is(readErr, storage.ErrClosed) {
			return nil, false, readErr
		}
		err = readErr
	}
	return nil, false, err
}

// prefixUpperBound returns the smallest key that sorts after every key with
// prefix, or nil when the prefix is empty or all 0xFF bytes (unbounded).
func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
