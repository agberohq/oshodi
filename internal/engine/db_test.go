package engine

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNewDB(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("DB is nil")
	}
}

func TestNewDBWithDefaultConfig(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("DB is nil")
	}
}

func TestDBSetAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	err = db.Set(key, value)
	if err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	// Flush to ensure data is written
	if err := db.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("expected %s, got %s", value, got)
	}
}

func TestDBGetNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	_, err = db.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDBDelete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	err = db.Set(key, value)
	if err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	err = db.Delete(key)
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	_, err = db.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestDBEmptyKey(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	err = db.Set([]byte{}, []byte("value"))
	if err == nil {
		t.Error("expected error for empty key")
	}

	_, err = db.Get([]byte{})
	if err != ErrKeyNotFound {
		t.Error("expected ErrKeyNotFound for empty key")
	}
}

func TestDBFlush(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value")
		if err := db.Set(key, value); err != nil {
			t.Fatalf("set failed: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
}

func TestDBStats(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	for i := 0; i < 50; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value")
		if err := db.Set(key, value); err != nil {
			t.Fatalf("set failed: %v", err)
		}
	}

	stats := db.Stats()
	if stats.NumKeys != 50 {
		t.Errorf("expected 50 keys, got %d", stats.NumKeys)
	}
	if stats.FileSizeBytes <= 0 {
		t.Error("file size should be positive")
	}
}

func TestDBInfo(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	info := db.Info()
	expectedKeys := []string{"num_keys", "file_size_bytes", "logical_offset", "bloom_insertions", "bloom_fpr", "cache_size", "estimated_keys"}
	for _, key := range expectedKeys {
		if _, ok := info[key]; !ok {
			t.Errorf("info missing key: %s", key)
		}
	}
}

func TestDBEstimatedKeyCount(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath:             dbPath,
		EnableCardinality:    true,
		CardinalityPrecision: 14,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	for i := 0; i < 1000; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value")
		if err := db.Set(key, value); err != nil {
			t.Fatalf("set failed: %v", err)
		}
	}

	estimated := db.EstimatedKeyCount()
	if estimated < 900 || estimated > 1100 {
		t.Errorf("estimated count %d not in expected range", estimated)
	}
}

func TestDBConcurrentOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	numGoroutines := 50
	numOps := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := []byte("key" + string(rune(id)) + "-" + string(rune(j)))
				value := []byte("value")
				if err := db.Set(key, value); err != nil {
					t.Errorf("concurrent set failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all keys exist
	idx := db.index.Load()
	expected := numGoroutines * numOps
	if idx.Len() != expected {
		t.Errorf("expected %d keys, got %d", expected, idx.Len())
	}
}

func TestDBCompact(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath:                dbPath,
		CompactionMinItems:      10,
		CompactionFragmentation: 0.1,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	// Add data
	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value")
		if err := db.Set(key, value); err != nil {
			t.Fatalf("set failed: %v", err)
		}
	}

	// Flush to ensure all data is written
	if err := db.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Delete half
	for i := 0; i < 50; i++ {
		key := []byte("key" + string(rune(i)))
		if err := db.Delete(key); err != nil {
			t.Fatalf("delete failed: %v", err)
		}
	}

	// Flush deletes
	if err := db.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Compact
	if err := db.Compact(); err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Verify remaining keys exist
	for i := 50; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		_, err := db.Get(key)
		if err != nil {
			t.Errorf("key %d should exist: %v", i, err)
		}
	}
}

func TestDBCompactIfNeeded(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath:                dbPath,
		CompactionMinItems:      10,
		CompactionFragmentation: 0.1,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	// Add data
	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value")
		if err := db.Set(key, value); err != nil {
			t.Fatalf("set failed: %v", err)
		}
	}

	// Delete most to create fragmentation
	for i := 0; i < 90; i++ {
		key := []byte("key" + string(rune(i)))
		if err := db.Delete(key); err != nil {
			t.Fatalf("delete failed: %v", err)
		}
	}

	if err := db.CompactIfNeeded(); err != nil {
		t.Fatalf("compactIfNeeded failed: %v", err)
	}
}

func TestDBClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Try operations after close
	err = db.Set([]byte("key"), []byte("value"))
	if err == nil {
		t.Error("expected error after close")
	}
}

func TestDBHealthCheck(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	if err := db.healthCheck(nil); err != nil {
		t.Errorf("health check failed: %v", err)
	}
}

func TestDBWithCompression(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath:             dbPath,
		EnableCompression:    true,
		CompressionThreshold: 10,
		CompressionLevel:     3,
	})
	if err != nil {
		t.Fatalf("failed to create DB with compression: %v", err)
	}
	defer db.Close()

	// Test with small value (should not compress)
	smallKey := []byte("small-key")
	smallValue := []byte("small")
	if err := db.Set(smallKey, smallValue); err != nil {
		t.Fatalf("set small value failed: %v", err)
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	got, err := db.Get(smallKey)
	if err != nil {
		t.Fatalf("get small value failed: %v", err)
	}
	if !bytes.Equal(got, smallValue) {
		t.Errorf("expected %s, got %s", smallValue, got)
	}

	// Test with large value (should compress)
	largeKey := []byte("large-key")
	largeValue := make([]byte, 10000)
	for i := range largeValue {
		largeValue[i] = byte('a')
	}
	if err := db.Set(largeKey, largeValue); err != nil {
		t.Fatalf("set large value failed: %v", err)
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	got, err = db.Get(largeKey)
	if err != nil {
		t.Fatalf("get large value failed: %v", err)
	}
	if !bytes.Equal(got, largeValue) {
		t.Errorf("large value mismatch")
	}
}

func TestDBPersistenceAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// First session
	db1, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}

	testKey := []byte("persist-key")
	testValue := []byte("persist-value")
	if err := db1.Set(testKey, testValue); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := db1.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Second session
	db2, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to reopen DB: %v", err)
	}
	defer db2.Close()

	got, err := db2.Get(testKey)
	if err != nil {
		t.Fatalf("get after reopen failed: %v", err)
	}
	if !bytes.Equal(got, testValue) {
		t.Errorf("expected %s, got %s", testValue, got)
	}
}

func BenchmarkDBSet(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		b.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + string(rune(i%10000)))
		value := []byte("value")
		if err := db.Set(key, value); err != nil {
			b.Fatalf("set failed: %v", err)
		}
	}
}

func BenchmarkDBGet(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
	})
	if err != nil {
		b.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value")
		if err := db.Set(key, value); err != nil {
			b.Fatalf("pre-populate failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + string(rune(i%10000)))
		if _, err := db.Get(key); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}
