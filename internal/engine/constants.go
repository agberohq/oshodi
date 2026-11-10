package engine

import "github.com/agberohq/oshodi/internal/consts"

// Engine defaults are aliases of the central constants package so there is a
// single place to tune them.
const (
	DefaultShardCount              = consts.DefaultShardCount
	DefaultCompactionMinItems      = consts.DefaultCompactionMinItems
	DefaultCompactionFragmentation = consts.DefaultCompactionFragmentation
	DefaultCompactionInterval      = consts.DefaultCompactionInterval
	DefaultCacheSize               = consts.DefaultCacheSize
	DefaultPoolSize                = consts.DefaultPoolSize
	DefaultFlushTimeout            = consts.DefaultFlushTimeout
	DefaultCoalesceWindow          = consts.DefaultCoalesceWindow
	DefaultInitialFileSize         = consts.DefaultInitialFileSize
	DefaultScanBatchSize           = consts.DefaultScanBatchSize
	DefaultScanConcurrency         = consts.DefaultScanConcurrency
	DefaultQueueCapacity           = consts.DefaultQueueCapacity
	DefaultMaxKeySize              = consts.DefaultMaxKeySize
)
