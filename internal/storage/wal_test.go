// wal_test.go
package storage

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWALSyncMode(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_sync.wal")

	cfg := DefaultWALConfig(walPath)
	cfg.Mode = SyncMode

	wal, err := NewWAL(cfg)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	offset, err := wal.Write(context.Background(), key, value)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	gotKey, gotValue, err := wal.Read(context.Background(), offset)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(gotKey) != string(key) {
		t.Errorf("key mismatch: expected %s, got %s", key, gotKey)
	}
	if string(gotValue) != string(value) {
		t.Errorf("value mismatch: expected %s, got %s", value, gotValue)
	}
}

func TestWALAsyncMode(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_async.wal")

	cfg := DefaultWALConfig(walPath)
	cfg.Mode = AsyncMode
	cfg.FlushInterval = 10 * time.Millisecond

	wal, err := NewWAL(cfg)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	offset, err := wal.Write(context.Background(), key, value)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Wait for background flush
	time.Sleep(50 * time.Millisecond)

	gotKey, gotValue, err := wal.Read(context.Background(), offset)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(gotKey) != string(key) {
		t.Errorf("key mismatch: expected %s, got %s", key, gotKey)
	}
	if string(gotValue) != string(value) {
		t.Errorf("value mismatch: expected %s, got %s", value, gotValue)
	}
}

func TestWALMultipleWrites(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_multiple.wal")

	cfg := DefaultWALConfig(walPath)
	cfg.Mode = SyncMode

	wal, err := NewWAL(cfg)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	numWrites := 100
	offsets := make([]int64, numWrites)
	written := make(map[int64]string)

	for i := 0; i < numWrites; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))
		offset, err := wal.Write(context.Background(), key, value)
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
		offsets[i] = offset
		written[offset] = string(value)
	}

	// Verify all writes are readable
	for i := 0; i < numWrites; i++ {
		expectedKey := []byte("key" + string(rune(i)))
		expectedValue := []byte("value" + string(rune(i)))
		gotKey, gotValue, err := wal.Read(context.Background(), offsets[i])
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if string(gotKey) != string(expectedKey) {
			t.Errorf("key mismatch at %d: expected %s, got %s", i, expectedKey, gotKey)
		}
		if string(gotValue) != string(expectedValue) {
			t.Errorf("value mismatch at %d: expected %s, got %s", i, expectedValue, gotValue)
		}
	}
}

func TestWALConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_concurrent.wal")

	cfg := DefaultWALConfig(walPath)
	cfg.Mode = SyncMode

	wal, err := NewWAL(cfg)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

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
				_, err := wal.Write(context.Background(), key, value)
				if err != nil {
					t.Errorf("concurrent write failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	if wal.Size() == 0 {
		t.Error("WAL file is empty after concurrent writes")
	}
}

func TestWALRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_recovery.wal")

	// Create and write
	cfg := DefaultWALConfig(walPath)
	cfg.Mode = SyncMode

	wal1, err := NewWAL(cfg)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	testData := make(map[string]string)
	for i := 0; i < 50; i++ {
		key := "key" + string(rune(i))
		value := "value" + string(rune(i))
		testData[key] = value
		_, err := wal1.Write(context.Background(), []byte(key), []byte(value))
		if err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	wal1.Close()

	// Reopen and read all entries
	wal2, err := NewWAL(cfg)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	recovered := 0
	offset := int64(0)
	size := wal2.Size()

	for offset < size {
		key, value, err := wal2.Read(context.Background(), offset)
		if err != nil {
			if err == ErrWALClosed {
				break
			}
			t.Logf("read error at offset %d: %v", offset, err)
			break
		}
		expectedValue, ok := testData[string(key)]
		if !ok {
			t.Errorf("unexpected key: %s", key)
		} else if string(value) != expectedValue {
			t.Errorf("value mismatch for key %s: expected %s, got %s", key, expectedValue, value)
		}
		offset += 4 + int64(len(key)) + 4 + int64(len(value))
		recovered++
	}

	if recovered != 50 {
		t.Errorf("expected to recover 50 records, got %d", recovered)
	}
}

func TestWALCloseFlushesData(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_close.wal")

	cfg := DefaultWALConfig(walPath)
	cfg.Mode = AsyncMode
	cfg.FlushInterval = 1 * time.Second // Long interval to force close to flush

	wal, err := NewWAL(cfg)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	key := []byte("test-key")
	value := []byte("test-value")

	offset, err := wal.Write(context.Background(), key, value)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Close should flush
	err = wal.Close()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen and verify data is persisted
	cfg2 := DefaultWALConfig(walPath)
	cfg2.Mode = SyncMode
	wal2, err := NewWAL(cfg2)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	gotKey, gotValue, err := wal2.Read(context.Background(), offset)
	if err != nil {
		t.Fatalf("read from reopened WAL failed: %v", err)
	}
	if string(gotKey) != string(key) {
		t.Errorf("key mismatch after reopen: expected %s, got %s", key, gotKey)
	}
	if string(gotValue) != string(value) {
		t.Errorf("value mismatch after reopen: expected %s, got %s", value, gotValue)
	}
}

func TestWALWriteAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_closed.wal")

	cfg := DefaultWALConfig(walPath)
	cfg.Mode = SyncMode

	wal, err := NewWAL(cfg)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}

	_, err = wal.Write(context.Background(), []byte("key"), []byte("value"))
	if err != ErrWALClosed {
		t.Errorf("expected ErrWALClosed, got %v", err)
	}
}

// Benchmarks

func BenchmarkWALSyncModeWrite(b *testing.B) {
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench_sync.wal")

	cfg := DefaultWALConfig(walPath)
	cfg.Mode = SyncMode

	wal, err := NewWAL(cfg)
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	key := []byte("test-key")
	value := []byte("test-value")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := wal.Write(ctx, key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALAsyncModeWrite(b *testing.B) {
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench_async.wal")

	cfg := DefaultWALConfig(walPath)
	cfg.Mode = AsyncMode
	cfg.FlushInterval = 10 * time.Millisecond

	wal, err := NewWAL(cfg)
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	key := []byte("test-key")
	value := []byte("test-value")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := wal.Write(ctx, key, value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALParallelWrite(b *testing.B) {
	tmpDir := b.TempDir()
	walPath := filepath.Join(tmpDir, "bench_parallel.wal")

	cfg := DefaultWALConfig(walPath)
	cfg.Mode = SyncMode

	wal, err := NewWAL(cfg)
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	key := []byte("test-key")
	value := []byte("test-value")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := wal.Write(ctx, key, value)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
