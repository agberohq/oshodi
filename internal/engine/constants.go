package engine

import "time"

// Engine defaults centralized for single-edit tuning.
const (
	DefaultShardCount              = 64
	DefaultCompactionMinItems      = 1000
	DefaultCompactionFragmentation = 0.3
	DefaultCompactionInterval      = 30 * time.Minute
	DefaultCacheSize               = 10000
	DefaultPoolSize                = 10
	DefaultFlushTimeout            = 10 * time.Second
	DefaultCoalesceWindow          = 200 * time.Microsecond
	DefaultInitialFileSize         = 20 << 20 // 20MB pre-allocation
	DefaultScanBatchSize           = 1024
	DefaultScanConcurrency         = 4
	DefaultQueueCapacity           = 10000
)
