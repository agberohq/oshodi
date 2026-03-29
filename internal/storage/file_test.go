package storage

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
)

func TestNewFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 1024

	f, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	if f == nil {
		t.Fatal("file is nil")
	}
	if f.Size() != 1024 {
		t.Errorf("expected size 1024, got %d", f.Size())
	}
}

func TestFileWriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 1024

	f, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	offset, err := f.WriteRecord(key, value)
	if err != nil {
		t.Fatalf("write record failed: %v", err)
	}

	readKey, readValue, tombstone, n, err := f.ReadRecordAt(offset)
	if err != nil {
		t.Fatalf("read record failed: %v", err)
	}
	if tombstone {
		t.Error("record should not be tombstone")
	}
	if !bytes.Equal(readKey, key) {
		t.Errorf("expected key %s, got %s", key, readKey)
	}
	if !bytes.Equal(readValue, value) {
		t.Errorf("expected value %s, got %s", value, readValue)
	}
	if n <= 0 {
		t.Errorf("expected positive n, got %d", n)
	}
}

func TestFileIsEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 1024

	f, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	if !f.IsEmpty() {
		t.Error("new file should be empty")
	}

	_, err = f.WriteRecord([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if f.IsEmpty() {
		t.Error("file should not be empty after write")
	}
}

func TestFileWriteTombstone(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_tombstone.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 1024

	f, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("test-key")

	err = f.WriteTombstone(key)
	if err != nil {
		t.Fatalf("write tombstone failed: %v", err)
	}

	readKey, _, tombstone, _, err := f.ReadRecordAt(0)
	if err != nil {
		t.Fatalf("read tombstone failed: %v", err)
	}
	if !tombstone {
		t.Error("expected tombstone, got regular record")
	}
	if !bytes.Equal(readKey, key) {
		t.Errorf("expected key %s, got %s", key, readKey)
	}
}

func TestFileConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_concurrent.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 10 * 1024 * 1024

	f, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	writesPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				key := []byte("key" + string(rune(id)) + "-" + string(rune(j)))
				value := []byte("value" + string(rune(id)) + "-" + string(rune(j)))
				_, err := f.WriteRecord(key, value)
				if err != nil {
					t.Errorf("concurrent write failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	if f.LogicalSize() == 0 {
		t.Error("logical size is zero after concurrent writes")
	}
}

func TestFileTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_truncate.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 1024

	f, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	for i := 0; i < 10; i++ {
		_, err := f.WriteRecord([]byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}

	logicalSize := f.LogicalSize()
	if logicalSize == 0 {
		t.Skip("no data written")
	}

	newSize := logicalSize / 2
	err = f.Truncate(newSize)
	if err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	if f.Size() != newSize {
		t.Errorf("expected size %d, got %d", newSize, f.Size())
	}
	if f.LogicalSize() > newSize {
		t.Errorf("logical size %d exceeds truncated size %d", f.LogicalSize(), newSize)
	}
}

func TestFileSync(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_sync.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 1024

	f, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	_, err = f.WriteRecord([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	err = f.Sync()
	if err != nil {
		t.Fatalf("sync failed: %v", err)
	}
}

// Benchmarks

func BenchmarkFileWriteRecord(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "bench_write.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 100 * 1024 * 1024 // 100MB

	f, err := NewFile(cfg)
	if err != nil {
		b.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("bench-key")
	value := []byte("bench-value-value")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := f.WriteRecord(key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileWriteRecordParallel(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "bench_write_parallel.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 100 * 1024 * 1024

	f, err := NewFile(cfg)
	if err != nil {
		b.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("bench-key")
	value := []byte("bench-value")

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := f.WriteRecord(key, value)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkFileReadRecord(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "bench_read.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 100 * 1024 * 1024

	f, err := NewFile(cfg)
	if err != nil {
		b.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("bench-key")
	value := []byte("bench-value")

	offset, err := f.WriteRecord(key, value)
	if err != nil {
		b.Fatalf("write failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _, err := f.ReadRecordAt(offset)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileReadAt(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "bench_readat.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 100 * 1024 * 1024

	f, err := NewFile(cfg)
	if err != nil {
		b.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("bench-key")
	value := []byte("bench-value")
	dstBuf := make([]byte, 0, 1024)

	offset, err := f.WriteRecord(key, value)
	if err != nil {
		b.Fatalf("write failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := f.ReadAt(offset, key, dstBuf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileWriteLargeValue(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "bench_write_large.db")

	cfg := DefaultFileConfig(filePath)
	cfg.InitialSize = 100 * 1024 * 1024

	f, err := NewFile(cfg)
	if err != nil {
		b.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("bench-key")
	value := make([]byte, 1024*100) // 100KB
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := f.WriteRecord(key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}
