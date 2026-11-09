package storage

import (
	"bytes"
	"fmt"
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

// TestNextRecordStartSkipsZeroPrefix covers index rebuild after a
// preallocated prefix: the first record must be found behind the zeros, and
// an all-zero region must report no record.
func TestNextRecordStartSkipsZeroPrefix(t *testing.T) {
	cfg := DefaultFileConfig(filepath.Join(t.TempDir(), "gap.db"))
	cfg.InitialSize = 128 << 10
	f, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("new file: %v", err)
	}
	defer f.Close()

	const prefix = 64 << 10
	if _, err := f.file.WriteAt(make([]byte, prefix), 0); err != nil {
		t.Fatalf("seed prefix: %v", err)
	}
	f.logicalSize.Store(prefix)
	f.allocatedSize.Store(128 << 10)

	off, err := f.WriteRecord([]byte("songs\x00one"), []byte("v"))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if off != prefix {
		t.Fatalf("record offset = %d, want %d", off, prefix)
	}
	if f.IsEmpty() {
		t.Fatal("file with a record behind the prefix reported empty")
	}
	got, ok := f.NextRecordStart(0, f.LogicalSize())
	if !ok || got != off {
		t.Fatalf("NextRecordStart = %d ok=%v, want %d", got, ok, off)
	}
	if _, ok := f.NextRecordStart(0, prefix); ok {
		t.Fatal("all-zero prefix reported a record")
	}

	// A record whose payload no longer matches its CRC must not be returned.
	if _, err := f.file.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, off+8); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if _, ok := f.NextRecordStart(0, f.LogicalSize()); ok {
		t.Fatal("corrupt record accepted as a valid record start")
	}
}

// TestReadAfterGrowthRefreshesMmap pins the stale-mapping fix: a read of a
// record appended after the last remap must refresh the mapping rather than
// fail through mmap and fall back to the fd for every record.
func TestReadAfterGrowthRefreshesMmap(t *testing.T) {
	cfg := DefaultFileConfig(filepath.Join(t.TempDir(), "grow.db"))
	cfg.InitialSize = 1024
	f, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("new file: %v", err)
	}
	defer f.Close()

	const records = 64
	value := bytes.Repeat([]byte("v"), 256)
	type rec struct {
		key string
		off int64
	}
	var written []rec
	for i := 0; i < records; i++ {
		k := []byte(fmt.Sprintf("key-%03d", i))
		off, err := f.WriteRecord(k, value)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		written = append(written, rec{key: string(k), off: off})
	}
	if f.LogicalSize() <= 1024 {
		t.Fatalf("test did not grow the file (logical=%d)", f.LogicalSize())
	}

	// The mapping is still the 1 KiB one from open; every read below is past
	// it and must trigger the refresh path.
	for _, r := range written {
		got, err := f.ReadValueAt(r.off, []byte(r.key), nil)
		if err != nil {
			t.Fatalf("read %q at %d: %v", r.key, r.off, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("value for %q corrupted after remap", r.key)
		}
	}
	if f.mappedSize.Load() < f.LogicalSize() {
		t.Fatalf("mapping not refreshed: mapped=%d logical=%d", f.mappedSize.Load(), f.LogicalSize())
	}
}
