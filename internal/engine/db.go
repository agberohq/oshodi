package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"io"

	"github.com/agberohq/oshodi/internal/consts"
	"github.com/agberohq/oshodi/internal/messaging"
	"github.com/agberohq/oshodi/internal/metrics"
	"github.com/agberohq/oshodi/internal/pipeline"
	"github.com/agberohq/oshodi/internal/storage"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
)

var (
	ErrKeyNotFound = errors.New("oshodi: key not found")
	ErrKeyEmpty    = errors.New("oshodi: key cannot be empty")
	ErrBufferFull  = errors.New("oshodi: write buffer full")
	ErrDBClosed    = errors.New("oshodi: database closed")

	// ErrInvalidBucketName rejects bucket names containing NUL bytes, which
	// would collide with other buckets' keys.
	ErrInvalidBucketName = errors.New("oshodi: bucket name must not contain NUL bytes")
	ErrKeyTooLarge       = errors.New("oshodi: key exceeds configured maximum size")
	ErrValueTooLarge     = errors.New("oshodi: value exceeds configured maximum size")
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
	hooks    *pipeline.Hooks
	config   *Config

	// Locks
	writeCommitMu sync.RWMutex
	compactMu     sync.Mutex
	coalescer     *writeCoalescer
	flushMu       sync.Mutex
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
	lockFile  io.Closer

	// Ordered key snapshots for range/prefix/ordered scans, one per bucket.
	// Writes bump that bucket's generation; the next ordered scan rebuilds
	// only that bucket's sorted snapshot.
	orderedMu    sync.Mutex
	orderedSnaps map[string]*orderedSnapshot
	orderedGens  map[string]uint64
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

	// WALSync enables per-write fsync durability on the WAL (SyncMode with
	// sync). When false the WAL uses AsyncMode + periodic flush, which is
	// the throughput default. DisableWAL takes precedence.
	WALSync bool

	// WALFlushInterval is the async flusher period. 0 selects 10ms.
	WALFlushInterval time.Duration

	// MaxKeySize caps key length on writes. 0 selects DefaultMaxKeySize.
	MaxKeySize int

	// MaxValueSize caps value length on writes (decode rejects values past
	// MaxLegalValLen regardless). 0 selects MaxLegalValLen.
	MaxValueSize int

	// EnableWriteCoalescing switches Set() to leader-based group commit.
	// Default off.
	EnableWriteCoalescing bool

	// PubSubPayloadCopy gives each subscriber its own payload copy instead
	// of the shared slice (subscriber mutation safety).
	PubSubPayloadCopy bool

	// Hooks, when set, intercept Set/Get/Delete. OnWrite may transform the
	// value or reject the write; OnRead may transform the returned value;
	// OnDelete may veto.
	Hooks *pipeline.Hooks
}

// newWALConfig resolves the user's WAL settings onto a storage.WALConfig.
func newWALConfig(config *Config, walPath string) *storage.WALConfig {
	walCfg := storage.DefaultWALConfig(walPath)
	if config.WALMaxBufSize > 0 {
		walCfg.BufferSize = config.WALMaxBufSize
	}
	if config.WALSync {
		walCfg.Mode = storage.SyncMode
		walCfg.Sync = true
	} else {
		walCfg.Mode = storage.AsyncMode
	}
	if config.WALFlushInterval > 0 {
		walCfg.FlushInterval = config.WALFlushInterval
	}
	return walCfg
}

// NewDB creates a database instance with the given configuration.
// It initialises storage, index, writer, cache, metrics, and WAL.
// If the WAL is enabled and contains data, it replays unflushed writes.
func NewDB(config *Config) (*DB, error) {
	if config == nil {
		config = &Config{}
	}
	if config.InitialFileSize <= 0 {
		config.InitialFileSize = DefaultInitialFileSize
	}
	if config.CompactionMinItems <= 0 {
		config.CompactionMinItems = DefaultCompactionMinItems
	}
	if config.CompactionFragmentation <= 0 {
		config.CompactionFragmentation = DefaultCompactionFragmentation
	}
	if config.CompactionInterval <= 0 {
		config.CompactionInterval = DefaultCompactionInterval
	}
	if config.PoolSize <= 0 {
		config.PoolSize = DefaultPoolSize
	}
	if config.CacheSize <= 0 {
		config.CacheSize = DefaultCacheSize
	}
	if config.SplitMutexShards <= 0 {
		config.SplitMutexShards = DefaultShardCount
	}
	if config.Logger == nil {
		config.Logger = ll.New(consts.AppName).Disable()
	}
	if config.CardinalityPrecision == 0 {
		config.CardinalityPrecision = consts.DefaultCardinalityPrecision
	}
	if config.BloomExpectedItems == 0 {
		config.BloomExpectedItems = consts.DefaultBloomExpectedItems
	}
	if config.BloomFalsePositiveRate == 0 {
		config.BloomFalsePositiveRate = consts.DefaultBloomFalsePositiveRate
	}
	if config.MaxKeySize <= 0 {
		config.MaxKeySize = DefaultMaxKeySize
	}
	if config.MaxKeySize >= int(storage.MaxLegalKeyLen) {
		return nil, fmt.Errorf("oshodi: MaxKeySize %d must stay below %d", config.MaxKeySize, storage.MaxLegalKeyLen)
	}
	if config.MaxValueSize <= 0 {
		config.MaxValueSize = int(storage.MaxLegalValLen)
	}
	if config.EnableWriteCoalescing && config.SplitMutexShards <= 0 {
		config.SplitMutexShards = DefaultShardCount
	}
	if config.BatchMinSize <= 0 {
		config.BatchMinSize = 1
	}
	if config.BatchMaxSize < config.BatchMinSize {
		config.BatchMaxSize = config.BatchMinSize
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = DefaultCoalesceWindow
	}
	if config.SetQueueCapacity <= 0 {
		config.SetQueueCapacity = DefaultQueueCapacity
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

	lockFile, err := storage.AcquireFileLock(config.FilePath)
	if err != nil {
		return nil, err
	}

	// Finish a compaction that crashed between its two renames before the
	// storage layer can create a fresh, empty main file in its place.
	if recovered, recErr := storage.RecoverInterruptedCompaction(config.FilePath); recErr != nil {
		_ = lockFile.Close()
		return nil, recErr
	} else if recovered {
		config.Logger.Fields("path", config.FilePath).Info("recovered interrupted compaction")
	}

	storeCfg := storage.DefaultFileConfig(config.FilePath)
	storeCfg.InitialSize = config.InitialFileSize
	storeCfg.BufferSize = config.WriterBufferSize
	storeCfg.Compressor = compressor
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		_ = lockFile.Close()
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
		_ = store.Close()
		_ = lockFile.Close()
		return nil, err
	}

	cache := pipeline.NewCache(config.CacheSize)
	metricsMgr := metrics.NewManager(config.EnableCardinality, config.CardinalityPrecision, config.EnableFrequency)

	db := &DB{
		writer:    writer,
		lockFile:  lockFile,
		cache:     cache,
		pool:      pool,
		metrics:   metricsMgr,
		config:    config,
		stop:      make(chan struct{}),
		compactCh: make(chan struct{}, 1),
		logger:    config.Logger,
		hooks:     config.Hooks,
		lifetime:  config.JackLifetime,
		doctor:    config.JackDoctor,
		ownPool:   ownPool,
	}

	if config.EnableWriteCoalescing {
		db.coalescer = newWriteCoalescer(
			config.SplitMutexShards,
			config.BatchMinSize,
			config.BatchMaxSize,
			config.SetQueueCapacity,
			config.BatchTimeout,
		)
	}

	db.storage.Store(store)
	db.index.Store(idx)
	db.pubsub = messaging.NewPubSub(pool, config.Logger, config.Hooks, config.PubSubPayloadCopy)
	db.handlers = messaging.NewReqResp(consts.ReqRespDefaultTimeout)

	// Initialise WAL only if explicitly enabled (disabled by default for performance)
	if !config.DisableWAL {
		if config.WALMaxBufSize <= 0 {
			config.WALMaxBufSize = consts.DefaultWALMaxBufferSize
		}
		walCfg := newWALConfig(config, config.FilePath+storage.FilenameSuffixWAL)
		wal, err := storage.NewWAL(walCfg)
		if err != nil {
			_ = store.Close()
			_ = lockFile.Close()
			return nil, err
		}
		db.wal.Store(wal)
	}

	// Rebuild the index whenever the file has logical bytes in it. The
	// first bytes may be a preallocation hole rather than the first record:
	// a run that preallocated the file and exited before its first write
	// leaves a zero prefix, and the next run appends records after it.
	// loadIndex skips the prefix and sets the write cursor to the last
	// record boundary.
	if store.LogicalSize() > 0 {
		if err := db.loadIndex(); err != nil {
			db.Close()
			return nil, err
		}
	}

	// Replay WAL entries to recover writes that were not flushed to main storage.
	// ReadNext returns raw bytes: keyLen[4LE] | key | valueLen[4LE] | value.
	if wal := db.wal.Load(); wal != nil && wal.FileSize() > 0 {
		replayed, err := db.replayWAL(wal.NewReader(0))
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		if replayed > 0 {
			db.logger.Fields("replayed", replayed).Info("WAL recovery successful")
			if err := db.Flush(); err != nil {
				_ = db.Close()
				return nil, err
			}
		}
	}

	if db.doctor != nil {
		db.doctor.Add(jack.NewPatient(jack.PatientConfig{
			ID:       "oshodi.storage",
			Interval: consts.DoctorInterval,
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
			}), consts.MetricsInterval)
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
		// Only the key and tombstone state are needed here; ReadKeyAt still
		// verifies each record's CRC but skips materialising values.
		key, tombstone, n, err := store.ReadKeyAt(offset)
		if err != nil {
			// Unreadable record: stop and trim to the last good boundary.
			break
		}

		if tombstone && len(key) == 0 {
			// Preallocation hole. Jump to the next complete record in one
			// pass instead of stepping through the zero prefix 8 bytes at a
			// time; records appended after a zero prefix must still be
			// indexed, not treated as end-of-file.
			next, ok := store.NextRecordStart(offset+n, limit)
			if !ok {
				break
			}
			offset = next
			continue
		}

		if !tombstone && len(key) > 0 {
			idx.Set(key, offset)
		} else if tombstone && len(key) > 0 {
			idx.Delete(key)
		}
		offset += n
		lastValid = offset
	}

	store.SetLogicalSize(lastValid)
	return nil
}

// Set stores a key-value pair. Hooks run first; the WAL entry (which holds
// the post-hook value), the record write and the index update then commit
// under a read-lock that blocks during compaction swaps and Flush rotation.
func (db *DB) Set(key, value []byte) error {
	if db.closed.Load() {
		return ErrDBClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > db.config.MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > db.config.MaxValueSize {
		return ErrValueTooLarge
	}

	if db.hooks != nil && db.hooks.OnWrite != nil {
		transformed, err := db.hooks.OnWrite(pipeline.HookContext{Bucket: "", Key: key, Value: value})
		if err != nil {
			return err
		}
		value = transformed
	}

	if db.coalescer != nil {
		return db.coalesceSet(key, value)
	}
	return db.commitDirect(key, value)
}

// replayWAL applies recovered records straight to main storage. It bypasses
// the WAL (these entries came from it), the write hooks (the WAL already
// holds post-hook values; applying OnWrite again would double-transform) and
// the coalescer (a solo leader per entry would wait out the batch window
// once per record). Errors abort the open rather than recovering silently
// with partial data.
func (db *DB) replayWAL(reader *storage.WALReader) (int, error) {
	replayed := 0
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

		isDelete := valueLen == storage.WALTombstoneValLen
		if isDelete {
			if err := db.commitRecoveredDelete(key); err != nil {
				return replayed, err
			}
		} else {
			var value []byte
			if valueLen > 0 {
				value = data[4+keyLen+4 : 4+keyLen+4+valueLen]
			}
			if err := db.commitRecovered(key, value); err != nil {
				return replayed, err
			}
		}
		replayed++
	}
	return replayed, nil
}

// commitRecovered writes one recovered record directly to main storage.
func (db *DB) commitRecovered(key, value []byte) error {
	db.writeCommitMu.RLock()
	defer db.writeCommitMu.RUnlock()
	offset, err := db.writer.WriteRecord(key, value)
	if err != nil {
		return err
	}
	db.metrics.Add(key)
	db.index.Load().SetIfNewer(key, offset)
	db.cache.Delete(key)
	db.invalidateOrderedKeys(key)
	return nil
}

// commitRecoveredDelete writes one recovered tombstone directly to main storage.
func (db *DB) commitRecoveredDelete(key []byte) error {
	db.writeCommitMu.RLock()
	defer db.writeCommitMu.RUnlock()
	off, err := db.writer.WriteTombstone(key)
	if err != nil {
		return err
	}
	db.index.Load().DeleteIfStale(key, off)
	db.cache.Delete(key)
	db.invalidateOrderedKeys(key)
	return nil
}

// commitDirect is the uncoalesced write path: WAL entry, record write, and
// index update all inside the shared commit lock so Flush's sync+rotation
// and a compaction swap can never straddle them.
func (db *DB) commitDirect(key, value []byte) error {
	db.writeCommitMu.RLock()
	defer db.writeCommitMu.RUnlock()

	if wal := db.wal.Load(); wal != nil {
		if err := db.writeWALRecord(wal, key, value); err != nil {
			return err
		}
	}

	offset, err := db.writer.WriteRecord(key, value)
	if err != nil {
		return err
	}
	db.metrics.Add(key)
	db.index.Load().SetIfNewer(key, offset)
	db.cache.Delete(key)
	db.invalidateOrderedKeys(key)
	return nil
}

// writeWALRecord appends one record to wal, falling back to the fresh WAL
// after a rotation.
func (db *DB) writeWALRecord(wal *storage.WAL, key, value []byte) error {
	_, err := wal.Write(context.Background(), key, value)
	if err != nil && errors.Is(err, storage.ErrWALClosed) {
		if newWal := db.wal.Load(); newWal != nil && newWal != wal {
			_, err = newWal.Write(context.Background(), key, value)
		}
	}
	return err
}

// Get retrieves an owned copy of the value for a key. It checks the cache
// first, then the Bloom filter, the index, and finally reads from storage.
// Reads race with compaction swaps: an index snapshot paired with a
// storage file that has since been replaced returns a mismatch. That is
// transient — reload index + storage and retry rather than surfacing a
// corruption error to the caller.
func (db *DB) Get(key []byte) ([]byte, error) {
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	if len(key) == 0 {
		return nil, ErrKeyNotFound
	}
	if val, ok := db.cache.Get(key); ok {
		return db.applyReadHook(key, val)
	}

	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
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
		val, err := store.ReadValueAt(offset, key, buf)
		if err != nil {
			db.cache.Pool.Put(buf)
			if errors.Is(err, storage.ErrCorrupt) || errors.Is(err, storage.ErrClosed) {
				// Stale index/storage pairing mid-compaction — retry.
				continue
			}
			return nil, err
		}

		db.cache.Set(key, val)
		// val aliases the pooled read buffer, which the caller cannot return.
		// Copy out, then recycle the buffer.
		out := make([]byte, len(val))
		copy(out, val)
		db.cache.Pool.Put(buf)
		return db.applyReadHook(key, out)
	}
	return nil, ErrKeyNotFound
}

// Getter resolves key and passes its value to fn without making an owned
// copy. On a cache hit the slice is the cache's immutable buffer; on a miss it
// is a pooled read buffer. fn must not retain or mutate the slice. When an
// OnRead hook is configured it receives a private copy, so hooks may still
// mutate their input safely.
func (db *DB) Getter(key []byte, fn func(value []byte) error) error {
	if db.closed.Load() {
		return ErrDBClosed
	}
	if len(key) == 0 {
		return ErrKeyNotFound
	}
	if ok, err := db.cache.View(key, func(raw []byte) error {
		return db.hookAndCall(key, raw, fn)
	}); ok {
		return err
	}

	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		idx := db.index.Load()
		if !idx.MaybeHas(key) {
			return ErrKeyNotFound
		}
		offset, ok := idx.Get(key)
		if !ok {
			return ErrKeyNotFound
		}
		store := db.storage.Load()

		buf := db.cache.Pool.Get()
		val, err := store.ReadValueAt(offset, key, buf)
		if err != nil {
			db.cache.Pool.Put(buf)
			if errors.Is(err, storage.ErrCorrupt) || errors.Is(err, storage.ErrClosed) {
				// Stale index/storage pairing mid-compaction — retry.
				continue
			}
			return err
		}

		db.cache.Set(key, val)
		err = db.hookAndCall(key, val, fn)
		db.cache.Pool.Put(buf)
		return err
	}
	return ErrKeyNotFound
}

// hookAndCall applies OnRead and invokes fn. A hook gets a private copy so it
// cannot mutate a cache-owned buffer in place.
func (db *DB) hookAndCall(key, raw []byte, fn func([]byte) error) error {
	if db.hooks != nil && db.hooks.OnRead != nil {
		cp := make([]byte, len(raw))
		copy(cp, raw)
		raw = cp
	}
	hooked, err := db.applyReadHook(key, raw)
	if err != nil {
		return err
	}
	return fn(hooked)
}

// applyReadHook runs OnRead after a successful read (cache hit or decode).
func (db *DB) applyReadHook(key, val []byte) ([]byte, error) {
	if db.hooks == nil || db.hooks.OnRead == nil {
		return val, nil
	}
	transformed, err := db.hooks.OnRead(pipeline.HookContext{Key: key, Value: val})
	if err != nil {
		return nil, err
	}
	return transformed, nil
}

// Delete removes a key. The OnDelete hook may veto; the tombstone, its WAL
// entry and the index retirement commit under the shared commit lock.
func (db *DB) Delete(key []byte) error {
	if db.closed.Load() {
		return ErrDBClosed
	}
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if db.hooks != nil && db.hooks.OnDelete != nil {
		if err := db.hooks.OnDelete(pipeline.HookContext{Key: key}); err != nil {
			return err
		}
	}

	db.writeCommitMu.RLock()
	defer db.writeCommitMu.RUnlock()

	if wal := db.wal.Load(); wal != nil {
		_, err := wal.WriteTombstone(context.Background(), key)
		if err != nil && errors.Is(err, storage.ErrWALClosed) {
			if newWal := db.wal.Load(); newWal != nil && newWal != wal {
				_, err = newWal.WriteTombstone(context.Background(), key)
			}
		}
		if err != nil {
			return err
		}
	}

	off, err := db.writer.WriteTombstone(key)
	if err != nil {
		return err
	}
	db.index.Load().DeleteIfStale(key, off)
	db.cache.Delete(key)
	db.invalidateOrderedKeys(key)
	return nil
}

// BatchDelete removes multiple keys in one batched tombstone write. Index and
// cache entries are retired in offset order so a concurrent Set of the same
// key always wins if it was written later.
func (db *DB) BatchDelete(keys [][]byte) error {
	if db.closed.Load() {
		return ErrDBClosed
	}
	if len(keys) == 0 {
		return nil
	}
	for _, k := range keys {
		if len(k) == 0 {
			return ErrKeyEmpty
		}
		if len(k) > db.config.MaxKeySize {
			return ErrKeyTooLarge
		}
	}

	if db.hooks != nil && db.hooks.OnDelete != nil {
		for _, k := range keys {
			if err := db.hooks.OnDelete(pipeline.HookContext{Key: k}); err != nil {
				return err
			}
		}
	}

	if wal := db.wal.Load(); wal != nil {
		err := wal.BatchWriteTombstones(context.Background(), keys)
		if err != nil && errors.Is(err, storage.ErrWALClosed) {
			if newWal := db.wal.Load(); newWal != nil && newWal != wal {
				err = newWal.BatchWriteTombstones(context.Background(), keys)
			}
		}
		if err != nil {
			return err
		}
	}

	offsets, err := func() ([]int64, error) {
		db.writeCommitMu.RLock()
		defer db.writeCommitMu.RUnlock()
		return db.writer.BatchWriteTombstones(keys)
	}()
	if err != nil {
		return err
	}

	idx := db.index.Load()
	for i := range keys {
		idx.DeleteIfStale(keys[i], offsets[i])
		db.cache.Delete(keys[i])
	}
	db.invalidateOrderedKeys(keys...)
	return nil
}

// BatchSet stores multiple key-value pairs efficiently.
// It writes all entries to the WAL (if enabled), then performs a single batch
// write to main storage under a read-lock, updating the index and cache.
func (db *DB) BatchSet(entries []KV) error {
	if db.closed.Load() {
		return ErrDBClosed
	}
	if len(entries) == 0 {
		return nil
	}
	for i := range entries {
		if len(entries[i].Key) == 0 {
			return ErrKeyEmpty
		}
		if len(entries[i].Key) > db.config.MaxKeySize {
			return ErrKeyTooLarge
		}
		if len(entries[i].Value) > db.config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	// OnWrite transforms each entry before anything is written; hook code
	// must not run under the commit lock.
	if db.hooks != nil && db.hooks.OnWrite != nil {
		for i := range entries {
			transformed, err := db.hooks.OnWrite(pipeline.HookContext{Bucket: "", Key: entries[i].Key, Value: entries[i].Value})
			if err != nil {
				return err
			}
			entries[i].Value = transformed
		}
	}

	db.writeCommitMu.RLock()
	defer db.writeCommitMu.RUnlock()

	if wal := db.wal.Load(); wal != nil {
		for i := range entries {
			if err := db.writeWALRecord(wal, entries[i].Key, entries[i].Value); err != nil {
				return err
			}
		}
	}

	offsets, err := db.writer.BatchWriteRecord(entries)
	if err != nil {
		return err
	}
	// Metrics and index only after the batch write succeeds.
	idx := db.index.Load()
	for i := range entries {
		db.metrics.Add(entries[i].Key)
		idx.SetIfNewer(entries[i].Key, offsets[i])
		db.cache.Delete(entries[i].Key)
	}
	batchKeys := make([][]byte, len(entries))
	for i := range entries {
		batchKeys[i] = entries[i].Key
	}
	db.invalidateOrderedKeys(batchKeys...)
	return nil
}

// Flush forces all pending writes to disk and rotates the WAL. Rotation is
// serialized (no concurrent double-rotation) and holds the commit lock, so a
// writer's WAL entry can never land after the final sync, and an Open during
// the drain always sees the old handle still live.
func (db *DB) Flush() error {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.writeCommitMu.Lock()
	defer db.writeCommitMu.Unlock()

	if err := db.writer.Flush(); err != nil {
		return err
	}

	if wal := db.wal.Load(); wal != nil {
		wal.Close()
		walPath := db.config.FilePath + storage.FilenameSuffixWAL
		os.Remove(walPath)
		newWal, err := storage.NewWAL(newWALConfig(db.config, walPath))
		if err != nil {
			return err
		}
		db.wal.Store(newWal)
	}
	return nil
}

// Compact rewrites the storage file, removing deleted records and reducing fragmentation.
// It snapshots live entries, copies records to a temporary file concurrently with
// traffic, drains writers, copies anything appended during the copy phase,
// reconciles entries deleted in the meantime, and only then renames the temp
// file into place (backup-first) before swapping pointers — a crash at any
// point leaves a valid database or a recoverable backup, never a half-renamed
// pair.
func (db *DB) Compact() error {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()
	if db.closed.Load() {
		return ErrDBClosed
	}

	idx := db.index.Load()
	if idx.Len() == 0 {
		return nil
	}

	mainPath := db.config.FilePath
	backupPath := mainPath + storage.FilenameSuffixBackup
	tmpPath := mainPath + storage.FilenameSuffixCompact
	_ = os.Remove(tmpPath)
	_ = os.Remove(backupPath)

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

	copyRecord := func(key, value []byte) bool {
		newOff, err := tmpStore.WriteRecord(key, value)
		if err != nil {
			return false
		}
		newIndex.Set(key, newOff)
		return true
	}

	// Reuse one read buffer for every live value; WriteRecord copies it into
	// the output buffer immediately, so the slice never outlives this call.
	var copyBuf []byte
	copyValue := func(key []byte, offset int64) ([]byte, bool) {
		value, err := oldStorage.ReadValueAt(offset, key, copyBuf)
		if err != nil {
			return nil, false
		}
		if cap(value) > cap(copyBuf) {
			copyBuf = value[:cap(value)]
		}
		return value, true
	}

	// Copy records written before the snapshot offset without holding the
	// lock. Entries deleted during this phase are reconciled below.
	oldIndex.Range(func(key []byte, offset int64) bool {
		if offset >= snapshotOffset {
			return true
		}
		value, ok := copyValue(key, offset)
		if !ok {
			return true
		}
		if !copyRecord(key, value) {
			return false
		}
		return true
	})

	if err := tmpStore.Sync(); err != nil {
		_ = tmpStore.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// Take the write lock to drain in-flight writers, then copy any records
	// appended after the snapshot (written during the first pass).
	db.writeCommitMu.Lock()

	oldIndex.Range(func(key []byte, offset int64) bool {
		if offset < snapshotOffset {
			return true
		}
		value, ok := copyValue(key, offset)
		if !ok {
			return true
		}
		if !copyRecord(key, value) {
			return false
		}
		return true
	})

	if err := tmpStore.Sync(); err != nil {
		db.writeCommitMu.Unlock()
		_ = tmpStore.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// Reconcile deletes: keys copied in phase 1 that were deleted while the
	// copy ran now carry a tombstone watermark (a negative offset) in
	// oldIndex, so Has reports them absent and they are dropped here.
	newIndex.Range(func(key []byte, offset int64) bool {
		if !oldIndex.Has(key) {
			newIndex.Delete(key)
		}
		return true
	})

	// Crash-safe file replacement runs while writers are still blocked:
	// main → backup, tmp → main. Pointers swap only after that succeeds;
	// a failed rename rolls back to the old file untouched.
	if err := upgradeRename(mainPath, backupPath); err != nil {
		db.writeCommitMu.Unlock()
		db.logger.Fields("error", err).Error("compaction: backup rename failed; main file untouched")
		_ = tmpStore.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := upgradeRename(tmpPath, mainPath); err != nil {
		// Roll back so the database keeps serving the old file.
		if rbErr := upgradeRename(backupPath, mainPath); rbErr != nil {
			db.logger.Fields("error", rbErr).Error("compaction: rollback ALSO failed; recover main data from " + backupPath)
		}
		db.writeCommitMu.Unlock()
		_ = oldStorage.Close()
		_ = tmpStore.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	// Atomic pointer swap — writers resume against the compacted file.
	db.writer.store.Store(tmpStore)
	db.storage.Store(tmpStore)
	db.index.Store(newIndex)
	db.cache.Clear()
	db.invalidateAllOrderedKeys()

	db.writeCommitMu.Unlock()

	// Old storage (now the backup) is out of the pointer path — close it.
	if err := oldStorage.Close(); err != nil {
		db.logger.Fields("error", err).Error("compaction: close old storage failed")
	}
	_ = os.Remove(backupPath)
	fsyncDir(mainPath)

	db.logger.Fields("live_keys", newIndex.Len()).Info("compaction completed")
	return nil
}

// upgradeRename renames src over the dst path, tolerating a missing dst.
func upgradeRename(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	return nil
}

// fsyncDir flushes directory metadata (rename/create durability).
// Failure is non-fatal on filesystems that forbid directory fsync.
func fsyncDir(path string) {
	dir := filepath.Dir(path)
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

// CompactIfNeeded checks whether fragmentation exceeds the configured threshold.
// If compaction is required, it triggers a full compaction.
func (db *DB) CompactIfNeeded() error {
	idx := db.index.Load()
	if idx.Len() < db.config.CompactionMinItems {
		return nil
	}
	store := db.storage.Load()
	// Use the logical (live-record) extent — the allocated size includes
	// pre-allocation padding and the writer's growth factor, which are not
	// fragmentation and would trigger spurious compactions.
	logical := store.LogicalSize()
	if logical <= 0 {
		return nil
	}
	live := estimateLiveBytes(idx, store, logical)
	if live <= 0 {
		return nil
	}
	fragmentation := 1 - float64(live)/float64(logical)
	if fragmentation > db.config.CompactionFragmentation {
		db.logger.Fields(
			"fragmentation", fragmentation,
			"threshold", db.config.CompactionFragmentation,
			"live_bytes", live,
			"logical_bytes", logical,
		).Info("compaction triggered")
		return db.Compact()
	}
	return nil
}

// estimateLiveBytes samples up to 1024 live records and extrapolates their
// total size. The index stores offsets, not record sizes, so an exact answer
// would require reading every record; sampling keeps the periodic check cheap
// even for millions of keys while remaining accurate enough for a threshold.
func estimateLiveBytes(idx *Index, store *storage.File, logical int64) int64 {
	n := idx.Len()
	if n <= 0 || logical <= 0 {
		return 0
	}
	const maxSamples = consts.CompactionSampleSize
	stride := n/maxSamples + 1
	var sampled, live int64
	i := 0
	idx.RangeString(func(_ string, offset int64) bool {
		if i%stride != 0 {
			i++
			return true
		}
		i++
		if sz, ok := store.RecordSizeAt(offset, logical); ok {
			sampled++
			live += sz
		}
		return true
	})
	if sampled == 0 {
		return 0
	}
	est := int64(float64(live) / float64(sampled) * float64(n))
	if est > logical {
		est = logical
	}
	return est
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
	ticker := time.NewTicker(consts.MetricsInterval)
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
// The handler will be invoked when Call is used with the same name; its
// context is cancelled when the caller stops waiting.
func (db *DB) RegisterHandler(name string, fn messaging.HandlerFunc) {
	db.handlers.Register(name, fn)
}

// UnregisterHandler removes a previously registered handler.
func (db *DB) UnregisterHandler(name string) {
	db.handlers.Unregister(name)
}

// Call invokes a registered handler with a timeout.
// It returns the handler's response or an error on timeout.
func (db *DB) Call(ctx context.Context, name string, payload []byte) ([]byte, error) {
	return db.handlers.Call(ctx, name, payload)
}

// CallDirect invokes a registered handler without timeout protection.
// It runs the handler in the caller's goroutine.
func (db *DB) CallDirect(ctx context.Context, name string, payload []byte) ([]byte, error) {
	return db.handlers.CallDirect(ctx, name, payload)
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
// Buckets isolate keys and provide their own set of methods. Bucket names
// must not contain NUL bytes: the prefix is name-terminated-by-NUL, and a
// NUL inside the name would make keys from two different buckets collide
// byte-for-byte.
func (db *DB) GetBucket(name string) (*Bucket, error) {
	if strings.IndexByte(name, 0) >= 0 {
		return nil, ErrInvalidBucketName
	}
	prefix := make([]byte, 0, len(name)+1)
	if name != "" {
		prefix = append(prefix, name...)
		prefix = append(prefix, bucketKeySeparator[0])
	}
	return &Bucket{
		name:   name,
		prefix: prefix,
		db:     db,
	}, nil
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
			db.pool.Shutdown(consts.PoolShutdownTimeout)
		}
		if db.lockFile != nil {
			_ = db.lockFile.Close()
		}
	})
	return err
}
