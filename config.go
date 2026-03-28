package oshodi

import (
	"time"

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
}

// DefaultConfig returns a config with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		ShardCount:              64,
		QueueCapacity:           10000,
		WriterBufferSize:        4 << 20,  // 4MB
		InitialFileSize:         20 << 20, // 20MB
		CacheSize:               10000,
		PoolSize:                10,
		BatchMinSize:            10,
		BatchMaxSize:            1000,
		BatchTimeout:            50 * time.Millisecond,
		FlushTimeout:            10 * time.Second,
		BloomExpectedItems:      1_000_000,
		BloomFalsePositiveRate:  0.01,
		EnableCompression:       false,
		CompressionThreshold:    4096,
		CompressionLevel:        0,
		CompactionMinItems:      1000,
		CompactionFragmentation: 0.3,
		CompactionInterval:      30 * time.Minute,
		EnableCardinality:       true,
		CardinalityPrecision:    14,
		EnableFrequency:         true,
		Logger:                  ll.New("oshodi").Disable().Suspend(),
	}
}
