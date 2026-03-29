package oshodi

import (
	"errors"
	"sync/atomic"

	"github.com/agberohq/oshodi/internal/engine"
)

var (
	ErrClosed   = errors.New("oshodi: database closed")
	ErrKeyEmpty = errors.New("oshodi: key cannot be empty")
)

// DB is the main database handle
type DB struct {
	inner  *engine.DB
	cfg    *Config
	closed atomic.Bool
}

// Open creates or opens a database
func Open(path string, opts ...Option) (*DB, error) {
	cfg := DefaultConfig()
	cfg.Path = path

	for _, opt := range opts {
		opt(cfg)
	}

	// Convert to engine config
	engineCfg := &engine.Config{
		FilePath:                path,
		BloomExpectedItems:      cfg.BloomExpectedItems,
		BloomFalsePositiveRate:  cfg.BloomFalsePositiveRate,
		Logger:                  cfg.Logger,
		BatchMinSize:            cfg.BatchMinSize,
		BatchMaxSize:            cfg.BatchMaxSize,
		BatchTimeout:            cfg.BatchTimeout,
		FlushTimeout:            cfg.FlushTimeout,
		SetQueueCapacity:        cfg.QueueCapacity,
		SplitMutexShards:        cfg.ShardCount,
		WriterBufferSize:        cfg.WriterBufferSize,
		InitialFileSize:         cfg.InitialFileSize,
		CompactionMinItems:      cfg.CompactionMinItems,
		CompactionFragmentation: cfg.CompactionFragmentation,
		CompactionInterval:      cfg.CompactionInterval,
		PoolSize:                cfg.PoolSize,
		EnableCompression:       cfg.EnableCompression,
		CompressionThreshold:    cfg.CompressionThreshold,
		CompressionLevel:        cfg.CompressionLevel,
		CacheSize:               cfg.CacheSize,
		EnableCardinality:       cfg.EnableCardinality,
		CardinalityPrecision:    cfg.CardinalityPrecision,
		EnableFrequency:         cfg.EnableFrequency,
		JackPool:                cfg.JackPool,
		JackDoctor:              cfg.JackDoctor,
		JackLifetime:            cfg.JackLifetime,
		WALMaxBufSize:           cfg.WALMaxBufSize,
		DisableWAL:              cfg.DisableWAL,
	}

	inner, err := engine.NewDB(engineCfg)
	if err != nil {
		return nil, err
	}

	return &DB{
		inner: inner,
		cfg:   cfg,
	}, nil
}

// Close gracefully shuts down the database
func (db *DB) Close() error {
	if db.closed.Load() {
		return ErrClosed
	}
	defer db.closed.Store(true)
	return db.inner.Close()
}

// Bucket returns a handle to a named bucket
func (db *DB) Bucket(name string) *Bucket {
	return &Bucket{
		inner: db.inner.GetBucket(name),
		db:    db,
	}
}

// Flush forces all pending writes to disk
func (db *DB) Flush() error {
	return db.inner.Flush()
}

// Compact triggers manual compaction
func (db *DB) Compact() error {
	return db.inner.Compact()
}

// CompactIfNeeded checks and compacts if thresholds are met
func (db *DB) CompactIfNeeded() error {
	return db.inner.CompactIfNeeded()
}

// Stats returns database statistics
func (db *DB) Stats() map[string]interface{} {
	return db.inner.Info()
}

// Size returns the current logical size of the database
func (db *DB) Size() int64 {
	return db.inner.Size()
}

// EstimatedKeyCount returns estimated unique keys in the database
func (db *DB) EstimatedKeyCount() uint64 {
	return db.inner.EstimatedKeyCount()
}

// RegisterHandler registers a request-response handler
func (db *DB) RegisterHandler(name string, fn func([]byte) ([]byte, error)) {
	db.inner.RegisterHandler(name, fn)
}

// UnregisterHandler removes a request-response handler
func (db *DB) UnregisterHandler(name string) {
	db.inner.UnregisterHandler(name)
}

// Call invokes a handler with timeout protection
func (db *DB) Call(name string, payload []byte) ([]byte, error) {
	return db.inner.Call(name, payload)
}

// CallDirect invokes a handler in current goroutine
func (db *DB) CallDirect(name string, payload []byte) ([]byte, error) {
	return db.inner.CallDirect(name, payload)
}

// Subscribe registers a callback for a channel
func (db *DB) Subscribe(channel string, fn func([]byte)) func() {
	return db.inner.Subscribe(channel, fn)
}

// SubscribeWithID registers a callback and returns subscription ID
func (db *DB) SubscribeWithID(channel string, fn func([]byte)) (int64, func()) {
	return db.inner.SubscribeWithID(channel, fn)
}

// Publish sends a message to a channel
func (db *DB) Publish(channel string, payload []byte) (int, error) {
	return db.inner.Publish(channel, payload)
}

// UnsubscribeAll removes all subscribers from a channel
func (db *DB) UnsubscribeAll(channel string) {
	db.inner.UnsubscribeAll(channel)
}

// GetSubscriptionCount returns number of subscribers on a channel
func (db *DB) GetSubscriptionCount(channel string) int {
	return db.inner.GetSubscriptionCount(channel)
}

// GetAllSubscriptionIDs returns all subscription IDs on a channel
func (db *DB) GetAllSubscriptionIDs(channel string) []int64 {
	return db.inner.GetAllSubscriptionIDs(channel)
}

// Bucket represents a namespaced collection
type Bucket struct {
	inner *engine.Bucket
	db    *DB
}

// Set stores a key-value pair
func (b *Bucket) Set(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	return b.inner.Set(key, value)
}

// Get retrieves a value by key
func (b *Bucket) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	return b.inner.Get(key)
}

// Delete removes a key
func (b *Bucket) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	return b.inner.Delete(key)
}

// Has checks if a key exists
func (b *Bucket) Has(key []byte) bool {
	_, err := b.Get(key)
	return err == nil
}

// GetAll returns all key-value pairs in the bucket
func (b *Bucket) GetAll() ([]KV, error) {
	kvs, err := b.inner.GetAll()
	if err != nil {
		return nil, err
	}

	result := make([]KV, len(kvs))
	for i, kv := range kvs {
		result[i] = KV{
			Key:   kv.Key,
			Value: kv.Value,
		}
	}
	return result, nil
}

// Scan iterates over all key-value pairs without copying
func (b *Bucket) Scan(fn func(key, value []byte) bool) error {
	return b.inner.Scan(fn)
}

// EstimatedKeyCount returns estimated unique keys in this bucket
func (b *Bucket) EstimatedKeyCount() uint64 {
	return b.inner.EstimatedKeyCount()
}

// GetKeyFrequency returns estimated frequency of a key
func (b *Bucket) GetKeyFrequency(key []byte) uint64 {
	return b.inner.GetKeyFrequency(key)
}

// Publish sends a message to a channel
func (b *Bucket) Publish(channel string, payload []byte) (int, error) {
	return b.inner.Publish(channel, payload)
}

// Subscribe registers a callback for a channel
func (b *Bucket) Subscribe(channel string, fn func([]byte)) func() {
	return b.inner.Subscribe(channel, fn)
}

// SubscribeWithID registers a callback and returns subscription ID
func (b *Bucket) SubscribeWithID(channel string, fn func([]byte)) (int64, func()) {
	return b.inner.SubscribeWithID(channel, fn)
}

// RegisterHandler registers a bucket-scoped handler
func (b *Bucket) RegisterHandler(name string, fn func([]byte) ([]byte, error)) {
	b.inner.RegisterHandler(name, fn)
}

// UnregisterHandler removes a bucket-scoped handler
func (b *Bucket) UnregisterHandler(name string) {
	b.inner.UnregisterHandler(name)
}

// Call invokes a bucket-scoped handler
func (b *Bucket) Call(name string, payload []byte) ([]byte, error) {
	return b.inner.Call(name, payload)
}

// CallDirect invokes a bucket-scoped handler in current goroutine
func (b *Bucket) CallDirect(name string, payload []byte) ([]byte, error) {
	return b.inner.CallDirect(name, payload)
}

// UnsubscribeAll removes all subscribers from a channel
func (b *Bucket) UnsubscribeAll(channel string) {
	b.inner.UnsubscribeAll(channel)
}

// GetSubscriptionCount returns number of subscribers on a channel
func (b *Bucket) GetSubscriptionCount(channel string) int {
	return b.inner.GetSubscriptionCount(channel)
}

// GetAllSubscriptionIDs returns all subscription IDs on a channel
func (b *Bucket) GetAllSubscriptionIDs(channel string) []int64 {
	return b.inner.GetAllSubscriptionIDs(channel)
}

// KV represents a key-value pair
type KV struct {
	Key   []byte
	Value []byte
}
