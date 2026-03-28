package storage

import (
	"time"

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
		InitialSize:   20 << 20, // 20MB
		BufferSize:    4 << 20,  // 4MB
		RetryAttempts: 3,
		RetryBackoff:  100 * time.Millisecond,
		Logger:        ll.New("storage").Disable(),
	}
}
