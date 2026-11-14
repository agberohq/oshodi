// Package consts centralises defaults, sizing, timeouts, ratios and shared
// literals. Constants that describe an on-disk format (record flags, CRCs,
// magic bytes) stay next to the codec that owns them; everything tunable or
// repeated across packages lives here.
package consts

import "time"

// Byte units.
const (
	KiB = 1 << 10
	MiB = 1 << 20
	GiB = 1 << 30
)

// Application identity and shared separators.
const (
	AppName       = "oshodi"
	LoggerStorage = "storage"
	LoggerPubSub  = "pubsub"
	KeySeparator  = "\x00"
)

// Database defaults.
const (
	DefaultShardCount              = 64
	DefaultQueueCapacity           = 10000
	DefaultWriterBufferSize        = 4 * MiB
	DefaultInitialFileSize         = 20 * MiB
	DefaultCacheSize               = 10000
	DefaultPoolSize                = 10
	DefaultBatchMinSize            = 10
	DefaultBatchMaxSize            = 1000
	DefaultBatchTimeout            = 50 * time.Millisecond
	DefaultFlushTimeout            = 10 * time.Second
	DefaultCoalesceWindow          = 200 * time.Microsecond
	DefaultBloomExpectedItems      = 1_000_000
	DefaultBloomFalsePositiveRate  = 0.01
	CMSDefaultEpsilon              = 0.01
	CMSDefaultConfidence           = 0.99
	DefaultCompressionThreshold    = 4 * KiB
	DefaultCompactionMinItems      = 1000
	DefaultCompactionFragmentation = 0.3
	DefaultCompactionInterval      = 30 * time.Minute
	DefaultCardinalityPrecision    = 14
	MinCardinalityPrecision        = 10
	MaxCardinalityPrecision        = 16
	DefaultMaxKeySize              = 1 * MiB
	DefaultMaxValueSize            = 1 * GiB
	DefaultWALMaxBufferSize        = 4 * MiB
	DefaultScanBatchSize           = 1024
	DefaultScanConcurrency         = 4
)

// Storage and file internals.
const (
	MaxLegalKeyLen          uint32 = 0x00FFFFFF
	MaxLegalValLen          uint32 = 1 << 30
	MetaBufferSize                 = 4 * KiB
	ScanBufferSize                 = 64 * KiB
	MinGrowChunk                   = 64 * KiB
	GrowthDivisor                  = 4
	CacheLineSize                  = 64
	RemapInterval                  = 1 * time.Second
	StorageRetryBackoff            = 100 * time.Millisecond
	DefaultWALFlushInterval        = 10 * time.Millisecond
	FileMode                       = 0o600
	MaxCompressionLevel            = 9
)

// Cache.
const (
	CacheShards             = 128
	CacheBufferSize         = 1 * KiB
	CacheEvictCheckMask     = 1023 // every 1024 sets
	CacheEvictTargetNum     = 3
	CacheEvictTargetDen     = 4
	CacheEvictSampleDivisor = 16
	CacheEvictSampleMin     = 128
	CacheEvictSampleMax     = 4096
)

// Index and writer internals.
const (
	IndexShards          = 128
	WriterEncoderBuffer  = 32 * KiB
	WriterRecordBuffer   = 1 * KiB
	MinCoalescePerShard  = 16
	GetAttempts          = 3
	CompactionSampleSize = 1024
)

// Engine background tasks.
const (
	DoctorInterval      = 30 * time.Second
	MetricsInterval     = 1 * time.Minute
	PoolShutdownTimeout = 30 * time.Second
)

// Messaging.
const (
	ReqRespDefaultTimeout = 5 * time.Second
	TimerIdle             = time.Hour
	PubSubSnapshotCap     = 64
)

// Graph layer.
const (
	GraphBucketVertices  = "graph:v"
	GraphBucketEdges     = "graph:e"
	GraphRecordMagic     = 0x47 // 'G'
	GraphRecordVersion   = 1
	GraphPageRankDamping = 0.85
	GraphPageRankIters   = 20
	GraphDedupKeySep     = "|"
	GraphDedupLenSep     = ":"
)

// Vector layer.
const (
	VectorBucketVectors  = "vector:v"
	VectorBucketEdges    = "vector:e"
	VectorRecordMagic    = 0x56 // 'V'
	VectorRecordVersion  = 1
	VectorM              = 16
	VectorMl             = 0.25
	VectorEfSearch       = 20
)
