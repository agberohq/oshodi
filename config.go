package oshodi

import (
	"time"

	"github.com/agberohq/oshodi/internal/consts"
	"github.com/agberohq/oshodi/internal/pipeline"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
)

// Config holds database configuration
type Config struct {
	// Basic settings
	Path string

	// Performance tuning
	ShardCount       int
	QueueCapacity    int
	WriterBufferSize int
	InitialFileSize  int64
	CacheSize        int
	PoolSize         int

	// Batching
	BatchMinSize int
	BatchMaxSize int
	BatchTimeout time.Duration
	FlushTimeout time.Duration

	// Bloom filter
	BloomExpectedItems     uint
	BloomFalsePositiveRate float64

	// Compression
	EnableCompression    bool
	CompressionThreshold int
	CompressionLevel     int

	// Compaction
	CompactionMinItems      int
	CompactionFragmentation float64
	CompactionInterval      time.Duration

	// Analytics
	EnableCardinality    bool
	CardinalityPrecision uint8 // 10-16, higher = more accurate
	EnableFrequency      bool  // Enable Count-Min Sketch

	// Logging
	Logger       *ll.Logger
	JackPool     *jack.Pool
	JackDoctor   *jack.Doctor
	JackLifetime *jack.Lifetime

	WALMaxBufSize int
	DisableWAL    bool

	// WALSync enables per-write fsync durability (Sync mode) when the WAL
	// is enabled. Async flush (default) trades crash-window durability for
	// throughput; see the durability section in lab docs.
	WALSync bool

	// WALFlushInterval controls how often the async WAL flusher persists
	// buffered writes. 0 selects 10ms.
	WALFlushInterval time.Duration

	// MaxKeySize caps key length (protects the decoder and bounds memory
	// from corrupt length prefixes). 0 selects 1MB.
	MaxKeySize int

	// MaxValueSize caps value length on writes. 0 selects 1GB.
	MaxValueSize int

	// EnableWriteCoalescing switches Set() to leader-based group commit:
	// concurrent writers are merged into one physical write. Default off.
	EnableWriteCoalescing bool

	// PubSubPayloadCopy gives every subscriber its own payload copy.
	// Copying is the default: with a shared payload, one subscriber
	// mutating the slice corrupts it for every other subscriber on the
	// channel. Power users whose subscribers are provably read-only can
	// opt into the zero-copy path with WithPubSubPayloadCopy(false).
	PubSubPayloadCopy bool

	// Hooks intercept Set/Get/Delete (optional, may be nil).
	Hooks *pipeline.Hooks
}

// DefaultConfig returns a config with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		ShardCount:              consts.DefaultShardCount,
		QueueCapacity:           consts.DefaultQueueCapacity,
		WriterBufferSize:        consts.DefaultWriterBufferSize,
		InitialFileSize:         consts.DefaultInitialFileSize,
		CacheSize:               consts.DefaultCacheSize,
		PoolSize:                consts.DefaultPoolSize,
		BatchMinSize:            consts.DefaultBatchMinSize,
		BatchMaxSize:            consts.DefaultBatchMaxSize,
		BatchTimeout:            consts.DefaultBatchTimeout,
		FlushTimeout:            consts.DefaultFlushTimeout,
		BloomExpectedItems:      consts.DefaultBloomExpectedItems,
		BloomFalsePositiveRate:  consts.DefaultBloomFalsePositiveRate,
		EnableCompression:       false,
		CompressionThreshold:    consts.DefaultCompressionThreshold,
		CompressionLevel:        0,
		CompactionMinItems:      consts.DefaultCompactionMinItems,
		CompactionFragmentation: consts.DefaultCompactionFragmentation,
		CompactionInterval:      consts.DefaultCompactionInterval,
		EnableCardinality:       false, // opt-in via WithCardinality()
		CardinalityPrecision:    consts.DefaultCardinalityPrecision,
		EnableFrequency:         false, // opt-in via WithFrequency()
		Logger:                  ll.New(consts.AppName).Disable().Suspend(),
		// DisableWAL by default: no WAL overhead, main storage durability
		// only. Enable via WithWAL(true) and choose sync/async durability
		// via WithWALSyncMode.
		WALMaxBufSize: consts.DefaultWALMaxBufferSize,
		DisableWAL:    true,
		MaxKeySize:    consts.DefaultMaxKeySize, // keeps the decoder unambiguous
		MaxValueSize:  consts.DefaultMaxValueSize,

		// Subscribers get independent payload copies unless explicitly
		// opted out via WithPubSubPayloadCopy(false).
		PubSubPayloadCopy: true,
	}
}
