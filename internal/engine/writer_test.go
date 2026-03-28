// internal/engine/writer_test.go
// engine/writer_test.go
package engine

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/agberohq/oshodi/internal/storage"
)

func TestNewShardedWriter(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	if writer == nil {
		t.Fatal("writer is nil")
	}
	if writer.Offset() != 0 {
		t.Errorf("expected offset 0, got %d", writer.Offset())
	}
}

func TestShardedWriterWriteRecord(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	key := []byte("test-key")
	value := []byte("test-value")
	offset, err := writer.WriteRecord(key, value)
	if err != nil {
		t.Fatalf("write record failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}
	newOffset := writer.Offset()
	if newOffset <= 0 {
		t.Errorf("expected offset > 0, got %d", newOffset)
	}
}

func TestShardedWriterWriteMultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	var lastOffset int64
	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))
		offset, err := writer.WriteRecord(key, value)
		if err != nil {
			t.Fatalf("write record %d failed: %v", i, err)
		}
		if offset < lastOffset {
			t.Errorf("offset %d is less than previous %d", offset, lastOffset)
		}
		lastOffset = offset
	}
}

func TestShardedWriterWriteTombstone(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	key := []byte("test-key")
	err = writer.WriteTombstone(key)
	if err != nil {
		t.Fatalf("write tombstone failed: %v", err)
	}
	if writer.Offset() <= 0 {
		t.Errorf("expected positive offset, got %d", writer.Offset())
	}
}

func TestShardedWriterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	key := []byte("test-key")
	value := []byte("test-value")
	_, err = writer.WriteRecord(key, value)
	if err != nil {
		t.Fatalf("write record failed: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
}

func TestShardedWriterSync(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	key := []byte("test-key")
	value := []byte("test-value")
	_, err = writer.WriteRecord(key, value)
	if err != nil {
		t.Fatalf("write record failed: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
}

func TestShardedWriterClose(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	key := []byte("test-key")
	value := []byte("test-value")
	_, err = writer.WriteRecord(key, value)
	if err != nil {
		t.Fatalf("write record failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestShardedWriterConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 100
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := []byte("key" + string(rune(id)) + "-" + string(rune(j)))
				value := []byte("value")
				_, err := writer.WriteRecord(key, value)
				if err != nil {
					t.Errorf("concurrent write failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
	if writer.Offset() <= 0 {
		t.Errorf("expected positive offset after writes")
	}
}

func TestShardedWriterLargeRecord(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	key := make([]byte, 2000)
	value := make([]byte, 5000)
	for i := range key {
		key[i] = byte('a')
	}
	for i := range value {
		value[i] = byte('b')
	}
	offset, err := writer.WriteRecord(key, value)
	if err != nil {
		t.Fatalf("write large record failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}
	if writer.Offset() <= 0 {
		t.Errorf("expected offset > 0, got %d", writer.Offset())
	}
}

func BenchmarkShardedWriterWriteRecord(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		b.Fatalf("failed to create writer: %v", err)
	}
	key := []byte("test-key")
	value := []byte("test-value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.WriteRecord(key, value)
	}
}

func BenchmarkShardedWriterConcurrentWrites(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")
	storeCfg := storage.DefaultFileConfig(filePath)
	storeCfg.InitialSize = 1024
	storeCfg.BufferSize = 1024
	store, err := storage.NewFile(storeCfg)
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer store.Close()
	writer, err := NewShardedWriter(ShardedWriterConfig{
		ShardCount: 64,
		Store:      store,
	})
	if err != nil {
		b.Fatalf("failed to create writer: %v", err)
	}
	key := []byte("test-key")
	value := []byte("test-value")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			writer.WriteRecord(key, value)
		}
	})
}
