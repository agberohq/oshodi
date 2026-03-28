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
	storage   atomic.Pointer[storage.File]
	index     atomic.Pointer[Index]
	writer    *ShardedWriter
	cache     *pipeline.Cache
	pool      *jack.Pool
	pubsub    *messaging.PubSub
	handlers  *messaging.ReqResp
	metrics   *metrics.Manager
	config    *Config
	stop      chan struct{}
	compactCh chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	wg        sync.WaitGroup
	logger    *ll.Logger
	compactMu sync.Mutex
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
}

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
	if store.Size() > 0 && !store.IsEmpty() {
		if err := db.loadIndex(); err != nil {
			db.Close()
			return nil, err
		}
	}
	if db.doctor != nil {
		db.doctor.Add(jack.NewPatient(jack.PatientConfig{
			ID:       "oshodi.storage",
			Interval: 30 * time.Second,
			Check:    jack.FuncCtx(db.healthCheck),
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

func (db *DB) healthCheck(ctx context.Context) error {
	store := db.storage.Load()
	if store == nil {
		return errors.New("storage is nil")
	}
	_ = store.Size()
	return nil
}

func (db *DB) loadIndex() error {
	store := db.storage.Load()
	idx := db.index.Load()
	var offset int64
	// Use LogicalSize (actual written bytes), not Size (allocated/pre-allocated).
	limit := store.LogicalSize()
	for offset < limit {
		key, _, tombstone, n, err := store.ReadRecordAt(offset)
		if err != nil {
			break
		}
		if !tombstone && len(key) > 0 {
			idx.Set(key, offset)
			db.metrics.Add(key)
		} else if tombstone && len(key) > 0 {
			idx.Delete(key)
		}
		offset += n
	}
	return nil
}

func (db *DB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("oshodi: key cannot be empty")
	}
	db.metrics.Add(key)
	offset, err := db.writer.WriteRecord(key, value)
	if err != nil {
		return err
	}
	db.index.Load().Set(key, offset)
	db.cache.Delete(key)
	return nil
}

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
	val, err := store.ReadAt(offset, key)
	if err != nil {
		return nil, err
	}
	db.cache.Set(key, val)
	return val, nil
}

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

func (db *DB) Flush() error {
	return db.writer.Flush()
}

func (db *DB) Compact() error {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	if err := db.writer.Flush(); err != nil {
		return err
	}

	// Hold write lock to ensure atomic swap
	db.writer.writeMu.Lock()

	tmpPath := db.config.FilePath + ".compact"
	_ = os.Remove(tmpPath)
	tmpStoreCfg := storage.DefaultFileConfig(tmpPath)
	tmpStoreCfg.InitialSize = db.config.InitialFileSize
	tmpStoreCfg.BufferSize = db.config.WriterBufferSize
	tmpStoreCfg.Compressor = db.storage.Load().Compressor()
	tmpStore, err := storage.NewFile(tmpStoreCfg)
	if err != nil {
		db.writer.writeMu.Unlock()
		return err
	}

	newIndex := NewIndex(db.config.BloomExpectedItems, db.config.BloomFalsePositiveRate)
	oldIndex := db.index.Load()
	oldStorage := db.storage.Load()

	var lastOffset int64
	oldIndex.Range(func(key []byte, offset int64) bool {
		_, value, tombstone, _, err := oldStorage.ReadRecordAt(offset)
		if err != nil || tombstone {
			return true
		}
		newOff, err := tmpStore.WriteRecord(key, value)
		if err != nil {
			return false
		}
		newIndex.Set(key, newOff)
		lastOffset = newOff
		return true
	})

	if err := tmpStore.Sync(); err != nil {
		db.writer.writeMu.Unlock()
		_ = tmpStore.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// Atomic swap: update writer, storage pointer, and index together.
	db.writer.store.Store(tmpStore)
	db.writer.offset.Store(tmpStore.LogicalSize())
	db.storage.Store(tmpStore)
	db.index.Store(newIndex)
	db.writer.writeMu.Unlock()

	// Clear cache — offsets in the new store differ from the old one.
	db.cache.Clear()

	// Close old storage; new readers already see tmpStore via the atomic pointer.
	_ = oldStorage.Close()

	// Rename synchronously so callers see current data immediately.
	_ = os.Remove(db.config.FilePath)
	if err := os.Rename(tmpPath, db.config.FilePath); err != nil {
		db.logger.Fields("error", err).Error("compaction rename failed")
		return err
	}

	db.logger.Fields("new_size", lastOffset, "live_keys", newIndex.Len()).Info("compaction completed")
	return nil
}

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

func (db *DB) Size() int64 {
	return db.writer.Offset()
}

func (db *DB) EstimatedKeyCount() uint64 {
	return db.metrics.EstimatedKeys()
}

func (db *DB) RegisterHandler(name string, fn func([]byte) ([]byte, error)) {
	db.handlers.Register(name, messaging.HandlerFunc(fn))
}

func (db *DB) UnregisterHandler(name string) {
	db.handlers.Unregister(name)
}

func (db *DB) Call(name string, payload []byte) ([]byte, error) {
	return db.handlers.Call(name, payload)
}

func (db *DB) CallDirect(name string, payload []byte) ([]byte, error) {
	return db.handlers.CallDirect(name, payload)
}

func (db *DB) Subscribe(channel string, fn func([]byte)) func() {
	return db.pubsub.SubscribeSimple("", channel, fn)
}

func (db *DB) SubscribeWithID(channel string, fn func([]byte)) (int64, func()) {
	return db.pubsub.Subscribe("", channel, fn)
}

func (db *DB) Publish(channel string, payload []byte) (int, error) {
	return db.pubsub.Publish("", channel, payload)
}

func (db *DB) UnsubscribeAll(channel string) {
	db.pubsub.UnsubscribeAll("", channel)
}

func (db *DB) GetSubscriptionCount(channel string) int {
	return db.pubsub.Count("", channel)
}

func (db *DB) GetAllSubscriptionIDs(channel string) []int64 {
	return db.pubsub.IDs("", channel)
}

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

func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		db.closed.Store(true)
		close(db.stop)
		db.wg.Wait()
		if flushErr := db.Flush(); flushErr != nil {
			err = flushErr
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
