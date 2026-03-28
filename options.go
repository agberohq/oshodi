package oshodi

import (
	"time"

	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
)

// Option is a functional option for configuring the database
type Option func(*Config)

// WithLogger sets the logger
func WithLogger(logger *ll.Logger) Option {
	return func(c *Config) {
		c.Logger = logger
	}
}

// WithShardCount sets the number of shards for concurrency
func WithShardCount(count int) Option {
	return func(c *Config) {
		if count > 0 {
			c.ShardCount = count
		}
	}
}

// WithQueueCapacity sets the write queue capacity
func WithQueueCapacity(cap int) Option {
	return func(c *Config) {
		if cap > 0 {
			c.QueueCapacity = cap
		}
	}
}

// WithCacheSize sets the hot cache size
func WithCacheSize(size int) Option {
	return func(c *Config) {
		if size > 0 {
			c.CacheSize = size
		}
	}
}

// WithPoolSize sets the worker pool size
func WithPoolSize(size int) Option {
	return func(c *Config) {
		if size > 0 {
			c.PoolSize = size
		}
	}
}

// WithBloomFilter configures the Bloom filter
func WithBloomFilter(expectedItems uint, falsePositiveRate float64) Option {
	return func(c *Config) {
		if expectedItems > 0 {
			c.BloomExpectedItems = expectedItems
		}
		if falsePositiveRate > 0 && falsePositiveRate < 1 {
			c.BloomFalsePositiveRate = falsePositiveRate
		}
	}
}

// WithBatchSettings configures batching behavior
func WithBatchSettings(minSize, maxSize int, timeout time.Duration) Option {
	return func(c *Config) {
		if minSize > 0 {
			c.BatchMinSize = minSize
		}
		if maxSize > minSize {
			c.BatchMaxSize = maxSize
		}
		if timeout > 0 {
			c.BatchTimeout = timeout
		}
	}
}

// WithCompression enables compression
func WithCompression(threshold int, level int) Option {
	return func(c *Config) {
		c.EnableCompression = true
		if threshold > 0 {
			c.CompressionThreshold = threshold
		}
		if level >= 1 && level <= 9 {
			c.CompressionLevel = level
		}
	}
}

// WithCompaction configures compaction thresholds
func WithCompaction(minItems int, fragmentation float64, interval time.Duration) Option {
	return func(c *Config) {
		if minItems > 0 {
			c.CompactionMinItems = minItems
		}
		if fragmentation > 0 && fragmentation < 1 {
			c.CompactionFragmentation = fragmentation
		}
		if interval > 0 {
			c.CompactionInterval = interval
		}
	}
}

// WithInitialSize sets the initial file size
func WithInitialSize(size int64) Option {
	return func(c *Config) {
		if size > 0 {
			c.InitialFileSize = size
		}
	}
}

// WithWriterBufferSize sets the writer buffer size
func WithWriterBufferSize(size int) Option {
	return func(c *Config) {
		if size > 0 {
			c.WriterBufferSize = size
		}
	}
}

// WithFlushTimeout sets the flush timeout
func WithFlushTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		if timeout > 0 {
			c.FlushTimeout = timeout
		}
	}
}

// WithCardinality enables cardinality tracking with HyperLogLog
func WithCardinality(precision uint8) Option {
	return func(c *Config) {
		c.EnableCardinality = true
		if precision >= 10 && precision <= 16 {
			c.CardinalityPrecision = precision
		}
	}
}

// WithFrequency enables frequency tracking with Count-Min Sketch
func WithFrequency(enable bool) Option {
	return func(c *Config) {
		c.EnableFrequency = enable
	}
}

// WithJackPool injects an existing worker pool so the DB doesn't spawn its own
func WithJackPool(pool *jack.Pool) Option {
	return func(c *Config) {
		c.JackPool = pool
	}
}

// WithDoctor injects an existing health monitor
func WithDoctor(doctor *jack.Doctor) Option {
	return func(c *Config) {
		c.JackDoctor = doctor
	}
}

// WithLifetime injects an existing lifecycle manager for TTLs and background tasks
func WithLifetime(lifetime *jack.Lifetime) Option {
	return func(c *Config) {
		c.JackLifetime = lifetime
	}
}

// WithWALBufferSize options.go - Add WAL options:
func WithWALBufferSize(size int) Option {
	return func(c *Config) {
		if size > 0 {
			c.WALMaxBufSize = size
		}
	}
}

// WithWALDisabled disables the Write-Ahead Log
func WithWALDisabled(disable bool) Option {
	return func(c *Config) {
		c.DisableWAL = disable
	}
}
