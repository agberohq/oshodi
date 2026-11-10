package storage

import (
	"time"

	"github.com/agberohq/oshodi/internal/consts"
	"github.com/olekukonko/ll"
)

// FileConfig holds configuration for creating a storage file
type FileConfig struct {
	Path          string
	InitialSize   int64
	BufferSize    int
	Compressor    *Compressor
	Logger        *ll.Logger
	RetryAttempts int           // Number of retry attempts for mmap remapping
	RetryBackoff  time.Duration // Backoff duration between retries
}

// DefaultFileConfig returns a FileConfig with sensible defaults
func DefaultFileConfig(path string) *FileConfig {
	return &FileConfig{
		Path:          path,
		InitialSize:   consts.DefaultInitialFileSize,
		BufferSize:    consts.DefaultWriterBufferSize,
		RetryAttempts: consts.GetAttempts,
		RetryBackoff:  consts.StorageRetryBackoff,
		Logger:        ll.New(consts.LoggerStorage).Disable(),
	}
}
