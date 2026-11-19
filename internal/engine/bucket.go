package engine

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/agberohq/oshodi/internal/consts"
	"github.com/agberohq/oshodi/internal/messaging"
	"github.com/agberohq/oshodi/internal/storage"
)

// bucketKeySeparator joins the bucket name and the user key/channel/handler
// name to build the fully-qualified key.
const bucketKeySeparator = consts.KeySeparator

// Bucket represents a namespaced collection
type Bucket struct {
	name   string
	prefix []byte
	db     *DB
}

// makeKey constructs the full key with bucket prefix
func (b *Bucket) makeKey(userKey []byte) []byte {
	key := make([]byte, 0, len(b.prefix)+len(userKey))
	key = append(key, b.prefix...)
	key = append(key, userKey...)
	return key
}

// MakeKey exposes the bucket-prefixed key form for external batch helpers.
func (b *Bucket) MakeKey(userKey []byte) []byte {
	return b.makeKey(userKey)
}

// ParseKey exposes the bucket-prefix stripping used by batch readers.
func (b *Bucket) ParseKey(fullKey []byte) []byte {
	return b.parseKey(fullKey)
}

// parseKey extracts user key from full key
func (b *Bucket) parseKey(fullKey []byte) []byte {
	if len(b.prefix) == 0 {
		return fullKey
	}
	if len(fullKey) < len(b.prefix) || !bytes.Equal(fullKey[:len(b.prefix)], b.prefix) {
		return nil
	}
	return fullKey[len(b.prefix):]
}

// Set stores a key-value pair. An empty key is rejected.
func (b *Bucket) Set(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	fullKey := b.makeKey(key)
	return b.db.Set(fullKey, value)
}

// BatchSet stores multiple key-value pairs efficiently in a single lock-free syscall.
func (b *Bucket) BatchSet(entries []KV) error {
	for i := range entries {
		if len(entries[i].Key) == 0 {
			return ErrKeyEmpty
		}
	}
	namespacedEntries := make([]KV, len(entries))
	for i, entry := range entries {
		namespacedEntries[i] = KV{
			Key:   b.makeKey(entry.Key),
			Value: entry.Value,
		}
	}
	return b.db.BatchSet(namespacedEntries)
}

// Get retrieves a value
func (b *Bucket) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyNotFound
	}
	fullKey := b.makeKey(key)
	return b.db.Get(fullKey)
}

// Exists reports whether key has an index entry, without reading or
// validating the stored value. Use Get when the value is needed.
func (b *Bucket) Exists(key []byte) bool {
	if b.db.closed.Load() || len(key) == 0 {
		return false
	}
	fullKey := b.makeKey(key)
	idx := b.db.index.Load()
	if !idx.MaybeHas(fullKey) {
		return false
	}
	_, ok := idx.Get(fullKey)
	return ok
}

// BatchDelete removes multiple keys through one batched tombstone write.
func (b *Bucket) BatchDelete(keys [][]byte) error {
	if b.db.closed.Load() {
		return ErrDBClosed
	}
	if len(keys) == 0 {
		return nil
	}
	namespaced := make([][]byte, len(keys))
	for i, k := range keys {
		if len(k) == 0 {
			return ErrKeyEmpty
		}
		namespaced[i] = b.makeKey(k)
	}
	return b.db.BatchDelete(namespaced)
}

// GetterMany resolves each key in order and passes present values to fn
// without an owned copy. Missing keys are skipped; values are callback-scoped.
func (b *Bucket) GetterMany(keys [][]byte, fn func(key, value []byte) error) error {
	if b.db.closed.Load() {
		return ErrDBClosed
	}
	if len(keys) == 0 {
		return nil
	}
	var fullKey []byte
	for _, k := range keys {
		if len(k) == 0 {
			continue
		}
		fullKey = append(fullKey[:0], b.prefix...)
		fullKey = append(fullKey, k...)
		err := b.db.Getter(fullKey, func(v []byte) error { return fn(k, v) })
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Getter is Get without an owned value copy: value is only valid until fn
// returns, and must not be retained or mutated. Hooks, when configured, still
// receive a private copy.
func (b *Bucket) Getter(key []byte, fn func(value []byte) error) error {
	if b.db.closed.Load() {
		return ErrDBClosed
	}
	if len(key) == 0 {
		return ErrKeyNotFound
	}
	return b.db.Getter(b.makeKey(key), fn)
}

// Delete removes a key. An empty key is rejected.
func (b *Bucket) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	fullKey := b.makeKey(key)
	return b.db.Delete(fullKey)
}

// Snapshot returns a point-in-time read view of this bucket. Creating one is
// O(1); Close must be called, and an open view defers compaction.
func (b *Bucket) Snapshot() (*BucketSnapshot, error) {
	snap, err := b.db.Snapshot()
	if err != nil {
		return nil, err
	}
	return &BucketSnapshot{bucket: b, snap: snap}, nil
}

// BucketSnapshot is a point-in-time read view of one bucket.
type BucketSnapshot struct {
	bucket *Bucket
	snap   *Snapshot
}

// Watermark returns the offset boundary this view reads at.
func (bs *BucketSnapshot) Watermark() int64 { return bs.snap.Watermark() }

// Close releases the view.
func (bs *BucketSnapshot) Close() error { return bs.snap.Close() }

// Get returns the value of key as of the view.
func (bs *BucketSnapshot) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyNotFound
	}
	val, ok, err := bs.snap.getFull(string(bs.bucket.MakeKey(key)))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrKeyNotFound
	}
	return val, nil
}

// GetMany returns the values of keys present as of the view, in input order.
// Missing keys are omitted; values are owned copies.
func (bs *BucketSnapshot) GetMany(keys [][]byte) ([]KV, error) {
	out := make([]KV, 0, len(keys))
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		val, ok, err := bs.snap.getFull(string(bs.bucket.MakeKey(key)))
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, KV{Key: append([]byte(nil), key...), Value: val})
	}
	return out, nil
}

// ScanRange iterates the keys in [start, end) as of the view, in key order.
//
// Candidates come from the ordered index plus the displaced-version history,
// which still holds keys overwritten or deleted since the view was taken. Each
// candidate is resolved through the snapshot, so a key only reaches the
// callback if it existed at the watermark.
func (bs *BucketSnapshot) ScanRange(start, end []byte, fn func(key, value []byte) bool) error {
	prefix := string(bs.bucket.prefix)
	lo := prefix + string(start)
	hi := ""
	if len(end) > 0 {
		hi = prefix + string(end)
	}

	seen := make(map[string]struct{}, 64)
	var keys []string
	add := func(k string) {
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}

	if err := bs.bucket.scanOrderedKeys(start, end, false, func(_ []byte, k string) bool {
		add(k)
		return true
	}); err != nil {
		return err
	}
	for _, k := range bs.snap.db.history.keysInRange(prefix, lo, hi) {
		add(k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		val, ok, err := bs.snap.getFull(k)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if !fn(bs.bucket.ParseKey([]byte(k)), val) {
			return nil
		}
	}
	return nil
}

// Has reports whether key exists as of the view.
func (bs *BucketSnapshot) Has(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	_, ok, err := bs.snap.getFull(string(bs.bucket.MakeKey(key)))
	return err == nil && ok
}

// Has reports whether the key exists in this bucket. It is an alias for Exists.
func (b *Bucket) Has(key []byte) bool {
	return b.Exists(key)
}

// GetMany reads several keys under one consistent view: every returned value
// comes from the same generation, and a concurrent BatchSet or BatchDelete is
// either entirely visible or entirely invisible. Keys that do not exist are
// omitted. Values are owned copies.
func (b *Bucket) GetMany(keys [][]byte) ([]KV, error) {
	namespaced := make([][]byte, len(keys))
	for i, k := range keys {
		namespaced[i] = b.MakeKey(k)
	}
	got, err := b.db.GetMany(namespaced)
	if err != nil {
		return nil, err
	}
	out := make([]KV, len(got))
	for i := range got {
		out[i] = KV{Key: b.ParseKey(got[i].Key), Value: got[i].Value}
	}
	return out, nil
}

// GetAll returns all key-value pairs in the bucket. Read errors surface
// instead of silently skipping records.
func (b *Bucket) GetAll() ([]KV, error) {
	if b.db.closed.Load() {
		return nil, ErrDBClosed
	}
	var result []KV
	var firstErr error
	idx := b.db.index.Load()
	idx.Range(func(fullKey []byte, offset int64) bool {
		if !bytes.HasPrefix(fullKey, b.prefix) {
			return true
		}
		userKey := b.parseKey(fullKey)
		if userKey == nil {
			return true
		}
		store := b.db.storage.Load()

		val, err := store.ReadValueAt(offset, fullKey, nil)
		if err != nil {
			// A stale index/storage pairing mid-compaction is transient;
			// anything else is a real read error.
			if errors.Is(err, storage.ErrCorrupt) || errors.Is(err, storage.ErrClosed) {
				return true
			}
			firstErr = err
			return false
		}
		// fullKey and val are already freshly allocated per entry, so they
		// can be retained without another copy.
		result = append(result, KV{Key: userKey, Value: val})
		return true
	})
	return result, firstErr
}

// Scan iterates over all key-value pairs without copying. Read errors
// surface instead of silently skipping records.
func (b *Bucket) Scan(fn func(key, value []byte) bool) error {
	if b.db.closed.Load() {
		return ErrDBClosed
	}
	var firstErr error
	idx := b.db.index.Load()
	idx.Range(func(fullKey []byte, offset int64) bool {
		if !bytes.HasPrefix(fullKey, b.prefix) {
			return true
		}
		userKey := b.parseKey(fullKey)
		if userKey == nil {
			return true
		}
		store := b.db.storage.Load()

		val, err := store.ReadValueAt(offset, fullKey, nil)
		if err != nil {
			if errors.Is(err, storage.ErrCorrupt) || errors.Is(err, storage.ErrClosed) {
				return true
			}
			firstErr = err
			return false
		}
		return fn(userKey, val)
	})
	return firstErr
}

// Scanner iterates over all key-value pairs in the bucket while reusing its
// read buffers instead of allocating per record. The key and value slices are backed by buffers that
// are reused for the next record: both are only valid until fn returns. Copy
// anything that must outlive the callback. Use Scan when the callback needs
// to retain the slices.
func (b *Bucket) Scanner(fn func(key, value []byte) bool) error {
	if b.db.closed.Load() {
		return ErrDBClosed
	}
	idx := b.db.index.Load()
	store := b.db.storage.Load()
	prefix := string(b.prefix)
	var fullKey, valBuf []byte
	idx.RangeString(func(k string, offset int64) bool {
		if !strings.HasPrefix(k, prefix) {
			return true
		}
		fullKey = append(fullKey[:0], k...)
		val, err := store.ReadValueAt(offset, fullKey, valBuf)
		if err != nil {
			return true
		}
		if cap(val) > cap(valBuf) {
			valBuf = val[:cap(val)]
		}
		return fn(fullKey[len(b.prefix):], val)
	})
	return nil
}

// ScannerE iterates over all key-value pairs while reusing its read buffers,
// and propagates context, storage and callback errors. key and value are only
// valid until fn returns. It is the error-propagating counterpart of Scanner,
// intended for rebuild-style passes that process and copy each record
// immediately.
func (b *Bucket) ScannerE(ctx context.Context, fn func(key, value []byte) error) error {
	if b.db.closed.Load() {
		return ErrDBClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	idx := b.db.index.Load()
	store := b.db.storage.Load()
	prefix := string(b.prefix)
	var fullKey, valBuf []byte
	var scanErr error
	idx.RangeString(func(k string, offset int64) bool {
		if !strings.HasPrefix(k, prefix) {
			return true
		}
		select {
		case <-ctx.Done():
			scanErr = ctx.Err()
			return false
		default:
		}
		fullKey = append(fullKey[:0], k...)
		val, err := store.ReadValueAt(offset, fullKey, valBuf)
		if err != nil {
			scanErr = err
			return false
		}
		if cap(val) > cap(valBuf) {
			valBuf = val[:cap(val)]
		}
		if err := fn(fullKey[len(b.prefix):], val); err != nil {
			scanErr = err
			return false
		}
		return true
	})
	return scanErr
}

// ScanE is the error-propagating scan: storage read errors, context
// cancellation and callback errors all terminate the scan and are returned
// (Scan skips unreadable records instead). Iteration order is unspecified
// and concurrent writes may be missed or observed — no snapshot isolation.
func (b *Bucket) ScanE(ctx context.Context, fn func(key, value []byte) error) error {
	if b.db.closed.Load() {
		return ErrDBClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	idx := b.db.index.Load()
	store := b.db.storage.Load()
	var scanErr error
	idx.Range(func(fullKey []byte, offset int64) bool {
		userKey := b.parseKey(fullKey)
		if userKey == nil {
			return true
		}
		select {
		case <-ctx.Done():
			scanErr = ctx.Err()
			return false
		default:
		}
		val, err := store.ReadValueAt(offset, fullKey, nil)
		if err != nil {
			scanErr = err
			return false
		}
		if err := fn(userKey, val); err != nil {
			scanErr = err
			return false
		}
		return true
	})
	return scanErr
}

// EstimatedKeyCount returns estimated unique keys in this bucket
func (b *Bucket) EstimatedKeyCount() uint64 {
	return b.db.EstimatedKeyCount()
}

// GetKeyFrequency returns estimated frequency of a key
func (b *Bucket) GetKeyFrequency(key []byte) uint64 {
	fullKey := b.makeKey(key)
	return b.db.metrics.EstimateFrequency(fullKey)
}

// Publish sends a message to a channel
func (b *Bucket) Publish(channel string, payload []byte) (int, error) {
	return b.db.Publish(b.name+bucketKeySeparator+channel, payload)
}

// Subscribe registers a callback for a channel
func (b *Bucket) Subscribe(channel string, fn func([]byte)) func() {
	return b.db.Subscribe(b.name+bucketKeySeparator+channel, fn)
}

// SubscribeWithID registers a callback and returns subscription ID
func (b *Bucket) SubscribeWithID(channel string, fn func([]byte)) (int64, func()) {
	return b.db.SubscribeWithID(b.name+bucketKeySeparator+channel, fn)
}

// RegisterHandler registers a bucket-scoped handler
func (b *Bucket) RegisterHandler(name string, fn messaging.HandlerFunc) {
	b.db.RegisterHandler(b.name+bucketKeySeparator+name, fn)
}

// UnregisterHandler removes a bucket-scoped handler
func (b *Bucket) UnregisterHandler(name string) {
	b.db.UnregisterHandler(b.name + bucketKeySeparator + name)
}

// Call invokes a bucket-scoped handler
func (b *Bucket) Call(ctx context.Context, name string, payload []byte) ([]byte, error) {
	return b.db.Call(ctx, b.name+bucketKeySeparator+name, payload)
}

// CallDirect invokes a bucket-scoped handler directly
func (b *Bucket) CallDirect(ctx context.Context, name string, payload []byte) ([]byte, error) {
	return b.db.CallDirect(ctx, b.name+bucketKeySeparator+name, payload)
}

// UnsubscribeAll removes all subscribers from a channel
func (b *Bucket) UnsubscribeAll(channel string) {
	b.db.UnsubscribeAll(b.name + bucketKeySeparator + channel)
}

// GetSubscriptionCount returns number of subscribers on a channel
func (b *Bucket) GetSubscriptionCount(channel string) int {
	return b.db.GetSubscriptionCount(b.name + bucketKeySeparator + channel)
}

// GetAllSubscriptionIDs returns all subscription IDs on a channel
func (b *Bucket) GetAllSubscriptionIDs(channel string) []int64 {
	return b.db.GetAllSubscriptionIDs(b.name + bucketKeySeparator + channel)
}

// KV represents a key-value pair
type KV struct {
	Key   []byte
	Value []byte
}
