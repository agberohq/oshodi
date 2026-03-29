package engine

import (
	"bytes"
)

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

// Set stores a key-value pair
func (b *Bucket) Set(key, value []byte) error {
	fullKey := b.makeKey(key)
	return b.db.Set(fullKey, value)
}

// BatchSet stores multiple key-value pairs efficiently in a single lock-free syscall.
func (b *Bucket) BatchSet(entries []KV) error {
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

// Delete removes a key
func (b *Bucket) Delete(key []byte) error {
	fullKey := b.makeKey(key)
	return b.db.Delete(fullKey)
}

// GetAll returns all key-value pairs in the bucket
func (b *Bucket) GetAll() ([]KV, error) {
	var result []KV
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

		// Fixed: pass nil as 3rd param for dynamic allocation
		val, err := store.ReadAt(offset, fullKey, nil)
		if err != nil {
			return true
		}
		result = append(result, KV{
			Key:   append([]byte(nil), userKey...),
			Value: append([]byte(nil), val...),
		})
		return true
	})
	return result, nil
}

// Scan iterates over all key-value pairs without copying
func (b *Bucket) Scan(fn func(key, value []byte) bool) error {
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

		// Fixed: pass nil as 3rd param for dynamic allocation
		val, err := store.ReadAt(offset, fullKey, nil)
		if err != nil {
			return true
		}
		return fn(userKey, val)
	})
	return nil
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
	return b.db.Publish(b.name+"\x00"+channel, payload)
}

// Subscribe registers a callback for a channel
func (b *Bucket) Subscribe(channel string, fn func([]byte)) func() {
	return b.db.Subscribe(b.name+"\x00"+channel, fn)
}

// SubscribeWithID registers a callback and returns subscription ID
func (b *Bucket) SubscribeWithID(channel string, fn func([]byte)) (int64, func()) {
	return b.db.SubscribeWithID(b.name+"\x00"+channel, fn)
}

// RegisterHandler registers a bucket-scoped handler
func (b *Bucket) RegisterHandler(name string, fn func([]byte) ([]byte, error)) {
	b.db.RegisterHandler(b.name+"\x00"+name, fn)
}

// UnregisterHandler removes a bucket-scoped handler
func (b *Bucket) UnregisterHandler(name string) {
	b.db.UnregisterHandler(b.name + "\x00" + name)
}

// Call invokes a bucket-scoped handler
func (b *Bucket) Call(name string, payload []byte) ([]byte, error) {
	return b.db.Call(b.name+"\x00"+name, payload)
}

// CallDirect invokes a bucket-scoped handler directly
func (b *Bucket) CallDirect(name string, payload []byte) ([]byte, error) {
	return b.db.CallDirect(b.name+"\x00"+name, payload)
}

// UnsubscribeAll removes all subscribers from a channel
func (b *Bucket) UnsubscribeAll(channel string) {
	b.db.UnsubscribeAll(b.name + "\x00" + channel)
}

// GetSubscriptionCount returns number of subscribers on a channel
func (b *Bucket) GetSubscriptionCount(channel string) int {
	return b.db.GetSubscriptionCount(b.name + "\x00" + channel)
}

// GetAllSubscriptionIDs returns all subscription IDs on a channel
func (b *Bucket) GetAllSubscriptionIDs(channel string) []int64 {
	return b.db.GetAllSubscriptionIDs(b.name + "\x00" + channel)
}

// KV represents a key-value pair
type KV struct {
	Key   []byte
	Value []byte
}
