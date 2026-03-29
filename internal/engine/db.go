package engine

import (
	"context"
	"encoding/binary"
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
	ErrKeyEmpty    = errors.New("oshodi: key cannot be empty")
)

type DBStats struct {
	NumKeys         int
	FileSizeBytes   int64
	LogicalOffset   int64
	BloomInsertions uint64
	BloomFPR        float64
	CacheSize       int
	EstimatedKeys   uint64
}

type DB struct {
	storage  atomic.Pointer[storage.File]
	index    atomic.Pointer[Index]
	writer   *ShardedWriter
	cache    *pipeline.Cache
	pool     *jack.Pool
	pubsub   *messaging.PubSub
	handlers *messaging.ReqResp
	metrics  *metrics.Manager
	config   *Config

	// Locks
	writeCommitMu sync.RWMutex
	compactMu     sync.Mutex
	wal           atomic.Pointer[storage.WAL]

	stop      chan struct{}
	compactCh chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	wg        sync.WaitGroup
	logger    *ll.Logger
	lifetime  *jack.Lifetime
	doctor    *jack.Doctor
	ownPool   bool
}

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
	JackPool                *jack.Pool
	JackDoctor              *jack.Doctor
	JackLifetime            *jack.Lifetime
	WALMaxBufSize           int
	DisableWAL              bool
}

// NewDB creates a database instance with the given configuration.
// It initialises storage, index, writer, cache, metrics, and WAL.
// If the WAL is enabled and contains data, it replays unflushed writes.
func NewDB(config *Config) (*DB, error) {
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

	var pool *jack.Pool
	var ownPool bool
	if config.JackPool != nil {
		pool = config.JackPool
		ownPool = false
	} else {
		pool = jack.NewPool(config.PoolSize)
		ownPool = true
	}

	var compressor *storage.Compressor
	if config.EnableCompression {
		var err error
		compressor, err = storage.NewCompressor(config.CompressionThreshold, config.CompressionLevel)
		if err != nil {
			return nil, err
		}
	}

	storeCfg := storage.DefaultFileConfig(config.FilePath)
	storeCfg.InitialSize = config.InitialFileSize
	storeCfg.BufferSize = config.WriterBufferSize
	storeCfg.Compressor = compressor
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		return nil, err
	}
	idx := NewIndex(config.BloomExpectedItems, config.BloomFalsePositiveRate)
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

	cache := pipeline.NewCache(config.CacheSize)
	metricsMgr := metrics.NewManager(config.EnableCardinality, config.CardinalityPrecision, config.EnableFrequency)
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
	db.pubsub = messaging.NewPubSub(pool, config.Logger, nil)
	db.handlers = messaging.NewReqResp(5 * time.Second)

	// Initialise WAL only if explicitly enabled (disabled by default for performance)
	if !config.DisableWAL {
		if config.WALMaxBufSize <= 0 {
			config.WALMaxBufSize = 4 * 1024 * 1024
		}
		walPath := config.FilePath + ".wal"
		wal, err := storage.NewWAL(&storage.WALConfig{Path: walPath, BufferSize: config.WALMaxBufSize})
		if err != nil {
			store.Close()
			return nil, err
		}
		db.wal.Store(wal)
	}

	if store.Size() > 0 && !store.IsEmpty() {
		if err := db.loadIndex(); err != nil {
			db.Close()
			return nil, err
		}
	}

	// Replay WAL entries to recover writes that were not flushed to main storage.
	// ReadNext returns raw bytes: keyLen[4LE] | key | valueLen[4LE] | value.
	if wal := db.wal.Load(); wal != nil && wal.FileSize() > 0 {
		reader := wal.NewReader(0)
		replayCount := 0
		for {
			data, err := reader.ReadNext(nil)
			if err != nil {
				break
			}
			if len(data) < 8 {
				continue
			}
			// WAL uses LittleEndian — must match the encoding in wal.Write.
			keyLen := binary.LittleEndian.Uint32(data[0:4])
			if uint32(len(data)) < 8+keyLen {
				continue
			}
			key := data[4 : 4+keyLen]
			valueLen := binary.LittleEndian.Uint32(data[4+keyLen : 4+keyLen+4])
			if uint32(len(data)) < 8+keyLen+valueLen {
				continue
			}

			var value []byte
			if valueLen > 0 {
				value = data[4+keyLen+4 : 4+keyLen+4+valueLen]
			}

			if valueLen == 0 {
				db.Delete(key)
			} else {
				db.Set(key, value)
			}
			replayCount++
		}
		if replayCount > 0 {
			db.logger.Fields("replayed", replayCount).Info("WAL recovery successful")
			db.Flush()
		}
	}

	if db.doctor != nil {
		db.doctor.Add(jack.NewPatient(jack.PatientConfig{
			ID:       "oshodi.storage",
			Interval: 30 * time.Second,
			Check:    db.healthCheck,
		}))
	}

	if db.lifetime != nil {
		var scheduleCompaction func()
		scheduleCompaction = func() {
			if db.closed.Load() {
				return
			}
			db.lifetime.ScheduleTimed(context.Background(), "oshodi.compaction", jack.CallbackCtx(func(ctx context.Context, id string) {
				if err := db.CompactIfNeeded(); err != nil {
					db.logger.Fields("error", err).Error("compaction check failed")
				}
				scheduleCompaction()
			}), db.config.CompactionInterval)
		}
		scheduleCompaction()
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
				scheduleMetrics()
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

// healthCheck returns nil if the storage is accessible.
// Used by jack.Doctor to monitor database health.
func (db *DB) healthCheck(ctx context.Context) error {
	store := db.storage.Load()
	if store == nil {
		return errors.New("storage is nil")
	}
	_ = store.Size()
	return nil
}

// loadIndex rebuilds the in-memory index from the storage file.
// It scans all records sequentially, updating the index and advancing
// past tombstones and padding holes. The logical size is set to the
// last valid record boundary so pre-allocated zero padding is ignored.
func (db *DB) loadIndex() error {
	store := db.storage.Load()
	idx := db.index.Load()
	var offset int64
	limit := store.LogicalSize()
	var lastValid int64

	for offset < limit {
		key, _, tombstone, n, err := store.ReadRecordAt(offset)
		if err != nil {
			// Unreadable record: stop and trim to the last good boundary.
			break
		}

		if tombstone && len(key) == 0 {
			// Padding hole from pre-allocation. Skip the 8-byte sentinel
			// and continue — do not treat this as end-of-file.
			offset += n
			continue
		}

		if !tombstone && len(key) > 0 {
			idx.Set(key, offset)
			db.metrics.Add(key)
		} else if tombstone && len(key) > 0 {
			idx.Delete(key)
		}
		offset += n
		lastValid = offset
	}

	store.SetLogicalSize(lastValid)
	return nil
}

// Set stores a key-value pair. It writes to the WAL first if enabled,
// then to the main storage under a read-lock that blocks during compaction swaps,
// and finally updates the index and cache.
func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("oshodi: key cannot be empty")
	}

	if wal := db.wal.Load(); wal != nil {
		_, err := wal.Write(context.Background(), key, value)
		if err != nil && errors.Is(err, storage.ErrWALClosed) {
			if newWal := db.wal.Load(); newWal != nil && newWal != wal {
				_, err = newWal.Write(context.Background(), key, value)
			}
		}
		if err != nil {
			return err
		}
	}

	db.metrics.Add(key)

	// Hold RLock so Compact's write-lock waits for in-flight writes to drain
	// before swapping the storage pointer. Without this a Set can write to
	// the old storage after the compaction has already atomically replaced it,
	// silently losing the write.
	db.writeCommitMu.RLock()
	offset, err := db.writer.WriteRecord(key, value)
	if err != nil {
		db.writeCommitMu.RUnlock()
		return err
	}
	db.index.Load().Set(key, offset)
	db.writeCommitMu.RUnlock()

	db.cache.Delete(key)
	return nil
}

// Get retrieves the value for a key. It checks the cache first,
// then the Bloom filter, the index, and finally reads from storage.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyNotFound
	}
	if val, ok := db.cache.Get(key); ok {
		return val, nil
	}
	idx := db.index.Load()
	if !idx.MaybeHas(key) {
		return nil, ErrKeyNotFound
	}
	offset, ok := idx.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}
	store := db.storage.Load()

	buf := db.cache.Pool.Get()
	val, err := store.ReadAt(offset, key, buf)
	if err != nil {
		db.cache.Pool.Put(buf)
		return nil, err
	}

	db.cache.Set(key, val)
	return val, nil
}

// Delete removes a key. It writes a tombstone to the WAL and main storage,
// then removes the entry from the index and cache.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	if wal := db.wal.Load(); wal != nil {
		_, err := wal.Write(context.Background(), key, nil)
		if err != nil && errors.Is(err, storage.ErrWALClosed) {
			if newWal := db.wal.Load(); newWal != nil && newWal != wal {
				_, err = newWal.Write(context.Background(), key, nil)
			}
		}
		if err != nil {
			return err
		}
	}

	db.writeCommitMu.RLock()
	defer db.writeCommitMu.RUnlock()

	if err := db.writer.WriteTombstone(key); err != nil {
		return err
	}
	db.index.Load().Delete(key)
	db.cache.Delete(key)
	return nil
}

// BatchSet stores multiple key-value pairs efficiently.
// It writes all entries to the WAL (if enabled), then performs a single batch
// write to main storage under a read-lock, updating the index and cache.
func (db *DB) BatchSet(entries []KV) error {
	if len(entries) == 0 {
		return nil
	}

	if wal := db.wal.Load(); wal != nil {
		for _, entry := range entries {
			_, err := wal.Write(context.Background(), entry.Key, entry.Value)
			if err != nil && errors.Is(err, storage.ErrWALClosed) {
				if newWal := db.wal.Load(); newWal != nil && newWal != wal {
					_, err = newWal.Write(context.Background(), entry.Key, entry.Value)
				}
			}
			if err != nil {
				return err
			}
		}
	}

	db.writeCommitMu.RLock()
	offsets, err := db.writer.BatchWriteRecord(entries)
	if err != nil {
		db.writeCommitMu.RUnlock()
		return err
	}
	idx := db.index.Load()
	for i, entry := range entries {
		db.metrics.Add(entry.Key)
		idx.Set(entry.Key, offsets[i])
	}
	db.writeCommitMu.RUnlock()

	for _, entry := range entries {
		db.cache.Delete(entry.Key)
	}
	return nil
}

// Flush forces all pending writes to disk and rotates the WAL.
// It syncs main storage first, then closes the old WAL, removes the file,
// and creates a fresh WAL for future writes. The sync happens before WAL
// deletion so a crash between the two leaves a recoverable WAL on disk.
func (db *DB) Flush() error {
	if err := db.writer.Flush(); err != nil {
		return err
	}

	if wal := db.wal.Load(); wal != nil {
		wal.Close()
		walPath := db.config.FilePath + ".wal"
		os.Remove(walPath)
		newWal, err := storage.NewWAL(&storage.WALConfig{
			Path:       walPath,
			BufferSize: db.config.WALMaxBufSize,
		})
		if err != nil {
			return err
		}
		db.wal.Store(newWal)
	}
	return nil
}

// Compact rewrites the storage file, removing deleted records and reducing fragmentation.
// It creates a temporary file, copies all live records without holding any locks,
// then takes the write-commit lock to drain in-flight writers, copies any records
// written during the first pass, atomically swaps the storage and index pointers,
// and renames the temp file synchronously.
func (db *DB) Compact() error {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	idx := db.index.Load()
	if idx.Len() == 0 {
		return nil
	}

	tmpPath := db.config.FilePath + ".compact"
	_ = os.Remove(tmpPath)

	oldStorage := db.storage.Load()
	tmpStoreCfg := storage.DefaultFileConfig(tmpPath)
	tmpStoreCfg.InitialSize = db.config.InitialFileSize
	tmpStoreCfg.BufferSize = db.config.WriterBufferSize
	tmpStoreCfg.Compressor = oldStorage.Compressor()

	tmpStore, err := storage.NewFile(tmpStoreCfg)
	if err != nil {
		return err
	}

	newIndex := NewIndex(db.config.BloomExpectedItems, db.config.BloomFalsePositiveRate)
	oldIndex := db.index.Load()
	snapshotOffset := oldStorage.LogicalSize()

	// Phase 1: copy records written before the snapshot offset without any lock.
	oldIndex.Range(func(key []byte, offset int64) bool {
		if offset >= snapshotOffset {
			return true
		}
		_, value, tombstone, _, err := oldStorage.ReadRecordAt(offset)
		if err != nil || tombstone {
			return true
		}
		newOff, err := tmpStore.WriteRecord(key, value)
		if err != nil {
			return false
		}
		newIndex.Set(key, newOff)
		return true
	})

	if err := tmpStore.Sync(); err != nil {
		_ = tmpStore.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// Phase 2: take the write lock to drain in-flight writers, then copy
	// any records appended after the snapshot (written during phase 1).
	db.writeCommitMu.Lock()

	oldIndex.Range(func(key []byte, offset int64) bool {
		if offset < snapshotOffset {
			return true
		}
		_, value, tombstone, _, err := oldStorage.ReadRecordAt(offset)
		if err != nil || tombstone {
			return true
		}
		newOff, err := tmpStore.WriteRecord(key, value)
		if err != nil {
			return false
		}
		newIndex.Set(key, newOff)
		return true
	})

	if err := tmpStore.Sync(); err != nil {
		db.writeCommitMu.Unlock()
		_ = tmpStore.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// Set the logical size so new writes start at the right cursor position.
	tmpStore.SetLogicalSize(tmpStore.LogicalSize())

	// Atomically swap — all writers now blocked by writeCommitMu.Lock.
	db.writer.store.Store(tmpStore)
	db.storage.Store(tmpStore)
	db.index.Store(newIndex)
	db.cache.Clear()

	db.writeCommitMu.Unlock()

	// Close and replace the old file synchronously so errors are surfaced.
	if err := oldStorage.Close(); err != nil {
		db.logger.Fields("error", err).Error("compaction: close old storage failed")
	}
	if err := os.Remove(db.config.FilePath); err != nil {
		db.logger.Fields("error", err).Error("compaction: remove old file failed")
	}
	if err := os.Rename(tmpPath, db.config.FilePath); err != nil {
		db.logger.Fields("error", err).Error("compaction: rename failed")
		return err
	}

	db.logger.Fields("live_keys", newIndex.Len()).Info("compaction completed")
	return nil
}

// CompactIfNeeded checks whether fragmentation exceeds the configured threshold.
// If compaction is required, it triggers a full compaction.
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

// processCompaction runs the compaction loop when no lifetime manager is provided.
// It listens for manual compaction signals and periodic tickers.
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

// metricsReporter periodically logs database statistics.
// It runs every minute when no lifetime manager is provided.
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

// Stats returns a snapshot of database statistics.
// It includes key count, file size, Bloom filter metrics, cache size, and estimated keys.
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

// Info returns a map of database statistics for external consumption.
// It is a convenient wrapper around Stats.
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

// Size returns the current logical size of the database file.
func (db *DB) Size() int64 {
	return db.writer.Offset()
}

// EstimatedKeyCount returns the approximate number of unique keys.
// It uses HyperLogLog if cardinality tracking is enabled.
func (db *DB) EstimatedKeyCount() uint64 {
	return db.metrics.EstimatedKeys()
}

// RegisterHandler binds a name to a request‑response handler.
// The handler will be invoked when Call is used with the same name.
func (db *DB) RegisterHandler(name string, fn func([]byte) ([]byte, error)) {
	db.handlers.Register(name, messaging.HandlerFunc(fn))
}

// UnregisterHandler removes a previously registered handler.
func (db *DB) UnregisterHandler(name string) {
	db.handlers.Unregister(name)
}

// Call invokes a registered handler with a timeout.
// It returns the handler's response or an error on timeout.
func (db *DB) Call(name string, payload []byte) ([]byte, error) {
	return db.handlers.Call(name, payload)
}

// CallDirect invokes a registered handler without timeout protection.
// It runs the handler in the caller's goroutine.
func (db *DB) CallDirect(name string, payload []byte) ([]byte, error) {
	return db.handlers.CallDirect(name, payload)
}

// Subscribe registers a callback for a channel.
// It returns an unsubscribe function to remove the subscription.
func (db *DB) Subscribe(channel string, fn func([]byte)) func() {
	return db.pubsub.SubscribeSimple("", channel, fn)
}

// SubscribeWithID registers a callback and returns a subscription ID.
// It returns both the ID and an unsubscribe function.
func (db *DB) SubscribeWithID(channel string, fn func([]byte)) (int64, func()) {
	return db.pubsub.Subscribe("", channel, fn)
}

// Publish sends a payload to all subscribers of a channel.
// It returns the number of subscribers that received the message.
func (db *DB) Publish(channel string, payload []byte) (int, error) {
	return db.pubsub.Publish("", channel, payload)
}

// UnsubscribeAll removes all subscribers from a channel.
func (db *DB) UnsubscribeAll(channel string) {
	db.pubsub.UnsubscribeAll("", channel)
}

// GetSubscriptionCount returns the number of subscribers on a channel.
func (db *DB) GetSubscriptionCount(channel string) int {
	return db.pubsub.Count("", channel)
}

// GetAllSubscriptionIDs returns all subscription IDs on a channel.
func (db *DB) GetAllSubscriptionIDs(channel string) []int64 {
	return db.pubsub.IDs("", channel)
}

// GetBucket returns a namespaced bucket handle.
// Buckets isolate keys and provide their own set of methods.
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

// Close shuts down the database gracefully.
// It flushes pending writes, closes storage, and waits for background tasks.
func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		db.closed.Store(true)
		close(db.stop)
		db.wg.Wait()
		if flushErr := db.Flush(); flushErr != nil {
			err = flushErr
		}

		if wal := db.wal.Load(); wal != nil {
			wal.Close()
		}

		if store := db.storage.Load(); store != nil {
			if closeErr := store.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
		if db.ownPool && db.pool != nil {
			db.pool.Shutdown(30 * time.Second)
		}
	})
	return err
}
