package oshodi

import (
	"context"
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
		WALSync:                 cfg.WALSync,
		WALFlushInterval:        cfg.WALFlushInterval,
		MaxKeySize:              cfg.MaxKeySize,
		MaxValueSize:            cfg.MaxValueSize,
		EnableWriteCoalescing:   cfg.EnableWriteCoalescing,
		PubSubPayloadCopy:       cfg.PubSubPayloadCopy,
		Hooks:                   cfg.Hooks,
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

// ScanOptions configures the behavior of Scan and MapReduce operations.
type ScanOptions struct {
	// Concurrency controls the number of parallel workers.
	// 0 or 1 means sequential scan. Values > 1 enable parallel scanning.
	Concurrency int

	// BatchSize controls how many keys are collected before dispatching
	// to workers in parallel mode. Default is 1024.
	BatchSize int
}

// Scan iterates over all key-value pairs in the database.
// It uses the index to enumerate keys and reads values from storage.
//
// Consistency: Scan does NOT provide snapshot isolation. It observes the
// current state of the database as it iterates. Concurrent writes may cause
// some records to be seen, missed, or seen twice. For analytics workloads
// this is acceptable; for exact consistency, use a snapshot-based approach.
//
// The callback receives copies of the key and value bytes. Modifying them
// does not affect the database. Return an error to abort the scan.
func (db *DB) Scan(ctx context.Context, opts *ScanOptions, fn func(key, value []byte) error) error {
	if db.closed.Load() {
		return ErrClosed
	}
	engineOpts := &engine.ScanOptions{
		Concurrency: opts.Concurrency,
		BatchSize:   opts.BatchSize,
	}
	return db.inner.Scan(ctx, engineOpts, fn)
}

// MapFunc transforms a key-value pair into an intermediate key-value pair.
// The intermediate key is used for grouping in the reduce phase.
type MapFunc func(key, value []byte) (ik, iv []byte, err error)

// ReduceFunc combines intermediate values for a given intermediate key.
// It receives all intermediate values for one intermediate key and produces
// a single output value.
type ReduceFunc func(ik []byte, values [][]byte) ([]byte, error)

// CombinerFunc is a worker-local aggregation function that can partially
// combine intermediate results before the final reduce phase.
// It has the same signature as ReduceFunc but operates on a subset of values.
// The combiner must be compatible with the reducer (i.e., combining partial
// results must produce the same final result as combining all values at once).
type CombinerFunc func(ik []byte, values [][]byte) ([]byte, error)

// MapReduceOptions configures the behavior of MapReduce operations.
type MapReduceOptions struct {
	// Concurrency controls the number of parallel map workers.
	// 0 or 1 means sequential processing. Values > 1 enable parallel maps.
	Concurrency int

	// BatchSize controls how many keys are collected before dispatching
	// to workers in parallel mode. Default is 1024.
	BatchSize int

	// EnableCombiner enables worker-local aggregation when a combiner
	// is provided. This reduces the amount of data shuffled to the
	// final reduce phase.
	EnableCombiner bool
}

// MapResult holds the results of a Map operation.
type MapResult struct {
	// Key is the intermediate key produced by the mapper.
	Key []byte
	// Value is the intermediate value produced by the mapper.
	Value []byte
}

// Map applies a transformation function to all key-value pairs in the database.
// It returns a slice of intermediate key-value pairs.
//
// For large datasets, consider using MapReduce with a combiner to avoid
// materializing all intermediate results in memory.
func (db *DB) Map(ctx context.Context, opts *MapReduceOptions, fn MapFunc) ([]MapResult, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
	engineOpts := &engine.MapReduceOptions{
		Concurrency:    opts.Concurrency,
		BatchSize:      opts.BatchSize,
		EnableCombiner: opts.EnableCombiner,
	}
	engineResults, err := db.inner.Map(ctx, engineOpts, engine.MapFunc(fn))
	if err != nil {
		return nil, err
	}
	results := make([]MapResult, len(engineResults))
	for i, r := range engineResults {
		results[i] = MapResult{Key: r.Key, Value: r.Value}
	}
	return results, nil
}

// Reduce combines intermediate values for each intermediate key.
// It groups all intermediate values by their key and applies the reduce
// function to each group.
func (db *DB) Reduce(ctx context.Context, results []MapResult, fn ReduceFunc) (map[string][]byte, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
	engineResults := make([]engine.MapResult, len(results))
	for i, r := range results {
		engineResults[i] = engine.MapResult{Key: r.Key, Value: r.Value}
	}
	return db.inner.Reduce(ctx, engineResults, engine.ReduceFunc(fn))
}

// MapReduce executes a complete MapReduce pipeline: scan → map → [combiner] → reduce.
// This is the primary API for data processing in Oshodi.
//
// The pipeline works as follows:
// 1. Scan: Enumerate all keys from the index and read values from storage.
// 2. Map: Transform each record into an intermediate key-value pair.
// 3. Combiner (optional): Worker-local aggregation to reduce intermediate data.
// 4. Reduce: Combine all intermediate results into final output.
//
// Consistency: MapReduce does NOT provide snapshot isolation. It observes the
// current state of the database as it iterates. For analytics workloads this
// is acceptable; for exact consistency, use a snapshot-based approach.
//
// Memory usage: The pipeline avoids materializing all intermediate results
// when a combiner is provided. Each worker maintains local partial results
// that are combined at the end.
func (db *DB) MapReduce(
	ctx context.Context,
	opts *MapReduceOptions,
	mapper MapFunc,
	reducer ReduceFunc,
	combiner CombinerFunc,
) (map[string][]byte, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
	engineOpts := &engine.MapReduceOptions{
		Concurrency:    opts.Concurrency,
		BatchSize:      opts.BatchSize,
		EnableCombiner: opts.EnableCombiner,
	}
	return db.inner.MapReduce(
		ctx,
		engineOpts,
		engine.MapFunc(mapper),
		engine.ReduceFunc(reducer),
		engine.CombinerFunc(combiner),
	)
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
	if b.db.closed.Load() {
		return ErrClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	return b.inner.Set(key, value)
}

// Get retrieves a value by key
func (b *Bucket) Get(key []byte) ([]byte, error) {
	if b.db.closed.Load() {
		return nil, ErrClosed
	}
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	return b.inner.Get(key)
}

// Delete removes a key
func (b *Bucket) Delete(key []byte) error {
	if b.db.closed.Load() {
		return ErrClosed
	}
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
