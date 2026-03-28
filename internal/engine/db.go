package engine

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agberohq/oshodi/internal/messaging"
	"github.com/agberohq/oshodi/internal/metrics"
	"github.com/agberohq/oshodi/internal/pipeline"
	"github.com/agberohq/oshodi/internal/storage"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
)

var (
	ErrKeyNotFound = errors.New("oshodi: key not found")
	ErrBufferFull  = errors.New("oshodi: write buffer full")
)

// DBStats holds database statistics.
type DBStats struct {
	NumKeys         int
	FileSizeBytes   int64
	LogicalOffset   int64
	BloomInsertions uint64
	BloomFPR        float64
	CacheSize       int
	EstimatedKeys   uint64
}

// DB is the core database implementation.
type DB struct {
	// RCU (Read-Copy-Update) Pointers for zero-lock reads
	storage atomic.Pointer[storage.File]
	index   atomic.Pointer[Index]

	// Sharded Writer handles all concurrent multi-producer writes lock-free
	writer *ShardedWriter

	cache    *pipeline.Cache
	pool     *jack.Pool
	pubsub   *messaging.PubSub
	handlers *messaging.ReqResp
	metrics  *metrics.Manager
	config   *Config

	stop      chan struct{}
	compactCh chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	wg        sync.WaitGroup
	logger    *ll.Logger

	compactMu sync.Mutex

	// External Dependencies (Dependency Injection)
	lifetime *jack.Lifetime
	doctor   *jack.Doctor
	ownPool  bool
}

// Config for core DB
type Config struct {
	FilePath                string
	BloomExpectedItems      uint
	BloomFalsePositiveRate  float64
	Logger                  *ll.Logger
	BatchMinSize            int
	BatchMaxSize            int
	BatchTimeout            time.Duration
	FlushTimeout            time.Duration
	SetQueueCapacity        int
	WriterBufferSize        int
	InitialFileSize         int64
	CompactionMinItems      int
	CompactionFragmentation float64
	CompactionInterval      time.Duration
	PoolSize                int
	EnableCompression       bool
	CompressionThreshold    int
	CompressionLevel        int
	CacheSize               int
	EnableCardinality       bool
	CardinalityPrecision    uint8
	EnableFrequency         bool
	SplitMutexShards        int

	// External resources injected by the load balancer
	JackPool     *jack.Pool
	JackDoctor   *jack.Doctor
	JackLifetime *jack.Lifetime
}

// NewDB creates a new database.
func NewDB(config *Config) (*DB, error) {
	// Apply defaults
	if config == nil {
		config = &Config{}
	}
	if config.WriterBufferSize <= 0 {
		config.WriterBufferSize = 1024
	}
	if config.FlushTimeout <= 0 {
		config.FlushTimeout = 10 * time.Second
	}
	if config.CompactionMinItems <= 0 {
		config.CompactionMinItems = 1000
	}
	if config.CompactionFragmentation <= 0 {
		config.CompactionFragmentation = 0.3
	}
	if config.CompactionInterval <= 0 {
		config.CompactionInterval = 30 * time.Minute
	}
	if config.PoolSize <= 0 {
		config.PoolSize = 10
	}
	if config.CacheSize <= 0 {
		config.CacheSize = 10000
	}
	if config.SplitMutexShards <= 0 {
		config.SplitMutexShards = 64
	}
	if config.Logger == nil {
		config.Logger = ll.New("oshodi").Disable()
	}
	if config.CardinalityPrecision == 0 {
		config.CardinalityPrecision = 14
	}
	if config.BloomExpectedItems == 0 {
		config.BloomExpectedItems = 1_000_000
	}
	if config.BloomFalsePositiveRate == 0 {
		config.BloomFalsePositiveRate = 0.01
	}

	// Setup Dependency Injection Pool
	var pool *jack.Pool
	var ownPool bool
	if config.JackPool != nil {
		pool = config.JackPool
		ownPool = false
	} else {
		pool = jack.NewPool(config.PoolSize)
		ownPool = true
	}

	// Setup compressor
	var compressor *storage.Compressor
	if config.EnableCompression {
		var err error
		compressor, err = storage.NewCompressor(config.CompressionThreshold, config.CompressionLevel)
		if err != nil {
			return nil, err
		}
	}

	// Open storage
	store, err := storage.NewFile(config.FilePath, config.InitialFileSize, config.WriterBufferSize, compressor)
	if err != nil {
		return nil, err
	}

	// Create index
	idx := NewIndex(config.BloomExpectedItems, config.BloomFalsePositiveRate)

	// Create sharded writer directly tied to the storage
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: config.SplitMutexShards,
		BufferSize: config.WriterBufferSize,
		Pool:       pool,
		Store:      store,
	})
	if err != nil {
		store.Close()
		return nil, err
	}

	// Create Zero-Allocation Cache
	cache := pipeline.NewCache(config.CacheSize)

	// Create metrics
	metricsMgr := metrics.NewManager(config.EnableCardinality, config.CardinalityPrecision, config.EnableFrequency)
	// Bind Bloom filter to metrics manager to track all additions seamlessly
	metricsMgr.SetBloomFilter(idx.bloom)

	db := &DB{
		writer:    writer,
		cache:     cache,
		pool:      pool,
		metrics:   metricsMgr,
		config:    config,
		stop:      make(chan struct{}),
		compactCh: make(chan struct{}, 1),
		logger:    config.Logger,
		lifetime:  config.JackLifetime,
		doctor:    config.JackDoctor,
		ownPool:   ownPool,
	}

	db.storage.Store(store)
	db.index.Store(idx)

	// Initialize messaging
	db.pubsub = messaging.NewPubSub(pool, config.Logger, nil)
	db.handlers = messaging.NewReqResp(5 * time.Second)

	// Load existing data
	if store.Size() > 0 && !store.IsEmpty() {
		if err := db.loadIndex(); err != nil {
			db.Close()
			return nil, err
		}
	}

	// Register with external Doctor if provided
	if db.doctor != nil {
		db.doctor.Add(jack.NewPatient(jack.PatientConfig{
			ID:       "oshodi.storage",
			Interval: 30 * time.Second,
			Check:    jack.FuncCtx(db.healthCheck),
		}))
	}

	// Route background tasks to Lifetime if provided, otherwise spawn local goroutines
	if db.lifetime != nil {
		// Self-rescheduling wrapper for periodic compaction
		var scheduleCompaction func()
		scheduleCompaction = func() {
			if db.closed.Load() {
				return
			}
			db.lifetime.ScheduleTimed(context.Background(), "oshodi.compaction", jack.CallbackCtx(func(ctx context.Context, id string) {
				if err := db.CompactIfNeeded(); err != nil {
					db.logger.Fields("error", err).Error("compaction check failed")
				}
				scheduleCompaction() // Schedule the next run
			}), db.config.CompactionInterval)
		}
		scheduleCompaction()

		// Self-rescheduling wrapper for periodic metrics
		var scheduleMetrics func()
		scheduleMetrics = func() {
			if db.closed.Load() {
				return
			}
			db.lifetime.ScheduleTimed(context.Background(), "oshodi.metrics", jack.CallbackCtx(func(ctx context.Context, id string) {
				stats := db.Stats()
				db.logger.Fields(
					"keys", stats.NumKeys,
					"size", stats.FileSizeBytes,
					"bloom_fpr", stats.BloomFPR,
					"cache_size", stats.CacheSize,
					"estimated_keys", stats.EstimatedKeys,
				).Debug("oshodi metrics")
				scheduleMetrics() // Schedule the next run
			}), 1*time.Minute)
		}
		scheduleMetrics()

	} else {
		db.wg.Add(2)
		go db.processCompaction()
		go db.metricsReporter()
	}

	return db, nil
}

// healthCheck is called by jack.Doctor.
func (db *DB) healthCheck(ctx context.Context) error {
	store := db.storage.Load()
	if store == nil {
		return errors.New("storage is nil")
	}
	// A simple size check verifies the storage pointer is valid and the file handle is responsive.
	_ = store.Size()
	return nil
}

// loadIndex rebuilds the index from the file.
func (db *DB) loadIndex() error {
	store := db.storage.Load()
	idx := db.index.Load()
	var offset int64
	for offset < store.Size() {
		key, _, tombstone, n, err := store.ReadRecordAt(offset)
		if err != nil {
			break
		}
		if !tombstone && len(key) > 0 {
			idx.Set(key, offset)
			db.metrics.Add(key)
		} else if tombstone {
			idx.Delete(key)
		}
		offset += n
	}
	return nil
}

// Set stores a key-value pair, routing it directly to the ShardedWriter without double-buffering.
func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("oshodi: key cannot be empty")
	}
	db.metrics.Add(key)

	// Lock-free write to the sharded writer.
	// The sharded writer handles the async flushing automatically via jack.Pool!
	offset, err := db.writer.WriteRecord(key, value)
	if err != nil {
		return err
	}

	// Optimistically update the RCU index (readers will find the new data after flush).
	db.index.Load().Set(key, offset)
	db.cache.Delete(key)

	return nil
}

// Get retrieves a value (100% Zero-Lock Hot Path).
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyNotFound
	}

	// Check zero-allocation cache first
	if val, ok := db.cache.Get(key); ok {
		return val, nil
	}

	// Atomic load index (Lock-Free)
	idx := db.index.Load()

	// Atomic Bloom Filter check
	if !idx.MaybeHas(key) {
		return nil, ErrKeyNotFound
	}

	offset, ok := idx.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Atomic load storage (Lock-Free)
	store := db.storage.Load()
	val, err := store.ReadAt(offset, key)
	if err != nil {
		return nil, err
	}

	db.cache.Set(key, val)
	return val, nil
}

// Delete queues a tombstone directly to the ShardedWriter.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("oshodi: key cannot be empty")
	}

	if err := db.writer.WriteTombstone(key); err != nil {
		return err
	}

	db.index.Load().Delete(key)
	db.cache.Delete(key)
	return nil
}

// Flush forces all pending writes to disk via the ShardedWriter.
func (db *DB) Flush() error {
	return db.writer.Flush()
}

// Compact runs full compaction utilizing the RCU pattern.
func (db *DB) Compact() error {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	// Flush all pending writes so we don't miss anything
	if err := db.writer.Flush(); err != nil {
		return err
	}

	// Build new storage and index
	tmpPath := db.config.FilePath + ".compact"

	// Clean up any old crashed compact files
	_ = os.Remove(tmpPath)

	tmpStore, err := storage.NewFile(tmpPath, db.config.InitialFileSize, db.config.WriterBufferSize, db.storage.Load().Compressor())
	if err != nil {
		return err
	}

	newIndex := NewIndex(db.config.BloomExpectedItems, db.config.BloomFalsePositiveRate)
	oldIndex := db.index.Load()
	var newOffset int64

	// Copy live data
	oldIndex.Range(func(key []byte, offset int64) bool {
		store := db.storage.Load()
		_, value, tombstone, _, err := store.ReadRecordAt(offset)
		if err != nil || tombstone {
			return true
		}
		newOff, err := tmpStore.WriteRecord(key, value)
		if err != nil {
			return false
		}
		newIndex.Set(key, newOff)
		newOffset += newOff
		return true
	})

	if err := tmpStore.Sync(); err != nil {
		_ = tmpStore.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// Atomic Swap (RCU) - Readers instantly switch to new index and storage
	oldStorage := db.storage.Load()

	db.storage.Store(tmpStore)
	db.index.Store(newIndex)

	// Update the writer to point to the new store so future writes go to the compacted file
	db.writer.store = tmpStore

	// RCU Grace Period cleanup
	db.pool.Do(func() {
		// Wait for existing lock-free readers to finish reading from old mmap
		time.Sleep(3 * time.Second)

		_ = oldStorage.Close()

		// Cross-platform safety: we remove the original and rename the compacted file.
		_ = os.Remove(db.config.FilePath)
		if err := os.Rename(tmpPath, db.config.FilePath); err != nil {
			db.logger.Fields("error", err).Error("compaction file rename failed")
		} else {
			db.logger.Fields("new_size", newOffset, "live_keys", newIndex.Len()).Info("compaction completed gracefully")
		}
	})

	return nil
}

// CompactIfNeeded checks and triggers compaction if fragmentation exceeds threshold.
func (db *DB) CompactIfNeeded() error {
	idx := db.index.Load()
	if idx.Len() < db.config.CompactionMinItems {
		return nil
	}
	store := db.storage.Load()
	avgRecordSize := store.Size() / int64(idx.Len())
	estimatedLive := int64(idx.Len()) * avgRecordSize
	fragmentation := float64(store.Size()-estimatedLive) / float64(store.Size())

	if fragmentation > db.config.CompactionFragmentation {
		db.logger.Fields(
			"fragmentation", fragmentation,
			"threshold", db.config.CompactionFragmentation,
			"live_bytes", estimatedLive,
			"total_bytes", store.Size(),
		).Info("compaction triggered")
		return db.Compact()
	}
	return nil
}

// processCompaction handles background compaction when jack.Lifetime is not available.
func (db *DB) processCompaction() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.config.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-db.compactCh:
			if err := db.Compact(); err != nil {
				db.logger.Fields("error", err).Error("compaction failed")
			}
		case <-ticker.C:
			if err := db.CompactIfNeeded(); err != nil {
				db.logger.Fields("error", err).Error("compaction check failed")
			}
		case <-db.stop:
			return
		}
	}
}

// metricsReporter logs periodic metrics when jack.Lifetime is not available.
func (db *DB) metricsReporter() {
	defer db.wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stats := db.Stats()
			db.logger.Fields(
				"keys", stats.NumKeys,
				"size", stats.FileSizeBytes,
				"bloom_fpr", stats.BloomFPR,
				"cache_size", stats.CacheSize,
				"estimated_keys", stats.EstimatedKeys,
			).Debug("oshodi metrics")
		case <-db.stop:
			return
		}
	}
}

// Stats returns database statistics as a struct.
func (db *DB) Stats() DBStats {
	store := db.storage.Load()
	idx := db.index.Load()
	return DBStats{
		NumKeys:         idx.Len(),
		FileSizeBytes:   store.Size(),
		LogicalOffset:   db.writer.Offset(),
		BloomInsertions: idx.BloomInsertions(),
		BloomFPR:        idx.BloomFPR(),
		CacheSize:       db.cache.Len(),
		EstimatedKeys:   db.metrics.EstimatedKeys(),
	}
}

// Info returns database statistics as a map (for API compatibility).
func (db *DB) Info() map[string]interface{} {
	stats := db.Stats()
	return map[string]interface{}{
		"num_keys":         stats.NumKeys,
		"file_size_bytes":  stats.FileSizeBytes,
		"logical_offset":   stats.LogicalOffset,
		"bloom_insertions": stats.BloomInsertions,
		"bloom_fpr":        stats.BloomFPR,
		"cache_size":       stats.CacheSize,
		"estimated_keys":   stats.EstimatedKeys,
	}
}

// Size returns the current logical size.
func (db *DB) Size() int64 {
	return db.writer.Offset()
}

// EstimatedKeyCount returns estimated unique keys.
func (db *DB) EstimatedKeyCount() uint64 {
	return db.metrics.EstimatedKeys()
}

// RegisterHandler registers a request-response handler.
func (db *DB) RegisterHandler(name string, fn func([]byte) ([]byte, error)) {
	db.handlers.Register(name, messaging.HandlerFunc(fn))
}

// UnregisterHandler removes a request-response handler.
func (db *DB) UnregisterHandler(name string) {
	db.handlers.Unregister(name)
}

// Call invokes a handler.
func (db *DB) Call(name string, payload []byte) ([]byte, error) {
	return db.handlers.Call(name, payload)
}

// CallDirect invokes a handler directly.
func (db *DB) CallDirect(name string, payload []byte) ([]byte, error) {
	return db.handlers.CallDirect(name, payload)
}

// Subscribe registers a callback.
func (db *DB) Subscribe(channel string, fn func([]byte)) func() {
	return db.pubsub.SubscribeSimple("", channel, fn)
}

// SubscribeWithID registers with ID.
func (db *DB) SubscribeWithID(channel string, fn func([]byte)) (int64, func()) {
	return db.pubsub.Subscribe("", channel, fn)
}

// Publish sends a message.
func (db *DB) Publish(channel string, payload []byte) (int, error) {
	return db.pubsub.Publish("", channel, payload)
}

// UnsubscribeAll removes all subscribers.
func (db *DB) UnsubscribeAll(channel string) {
	db.pubsub.UnsubscribeAll("", channel)
}

// GetSubscriptionCount returns subscriber count.
func (db *DB) GetSubscriptionCount(channel string) int {
	return db.pubsub.Count("", channel)
}

// GetAllSubscriptionIDs returns all subscription IDs.
func (db *DB) GetAllSubscriptionIDs(channel string) []int64 {
	return db.pubsub.IDs("", channel)
}

// GetBucket returns a bucket handle.
func (db *DB) GetBucket(name string) *Bucket {
	prefix := make([]byte, 0, len(name)+1)
	if name != "" {
		prefix = append(prefix, name...)
		prefix = append(prefix, 0x00)
	}
	return &Bucket{
		name:   name,
		prefix: prefix,
		db:     db,
	}
}

// Close gracefully shuts down the database.
func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		db.closed.Store(true)
		close(db.stop)

		// Wait for workers to finish
		db.wg.Wait()

		// Final flush
		if flushErr := db.Flush(); flushErr != nil {
			err = flushErr
		}

		// Close storage
		if store := db.storage.Load(); store != nil {
			if closeErr := store.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}

		// Shutdown pool if we own it
		if db.ownPool && db.pool != nil {
			db.pool.Shutdown(30 * time.Second)
		}
	})
	return err
}
