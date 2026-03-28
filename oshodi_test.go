package oshodi

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestOpenAndClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("database is nil")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

func TestOpenWithOptions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath,
		WithShardCount(128),
		WithCacheSize(5000),
		WithPoolSize(20),
		WithBloomFilter(2_000_000, 0.005),
		WithCompression(1024, 5),
		WithInitialSize(50<<20),
		WithWriterBufferSize(8<<20),
		WithFlushTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to open database with options: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("database is nil")
	}
}

func TestBucketBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	// Test Set and Get
	key := []byte("key1")
	value := []byte("value1")

	err = bucket.Set(key, value)
	if err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	got, err := bucket.Get(key)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("expected %s, got %s", value, got)
	}

	// Test Has
	if !bucket.Has(key) {
		t.Error("Has returned false for existing key")
	}

	// Test Delete
	err = bucket.Delete(key)
	if err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	_, err = bucket.Get(key)
	if err == nil {
		t.Error("expected error for deleted key, got nil")
	}
}

func TestBucketEmptyKey(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	err = bucket.Set([]byte{}, []byte("value"))
	if err == nil {
		t.Error("expected error for empty key, got nil")
	}

	_, err = bucket.Get([]byte{})
	if err == nil {
		t.Error("expected error for empty key, got nil")
	}
}

func TestBucketGetAll(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err := bucket.Set([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("failed to set key %s: %v", k, err)
		}
	}

	kvs, err := bucket.GetAll()
	if err != nil {
		t.Fatalf("failed to get all: %v", err)
	}

	if len(kvs) != len(testData) {
		t.Errorf("expected %d items, got %d", len(testData), len(kvs))
	}

	for _, kv := range kvs {
		expected, ok := testData[string(kv.Key)]
		if !ok {
			t.Errorf("unexpected key: %s", kv.Key)
		}
		if !bytes.Equal(kv.Value, []byte(expected)) {
			t.Errorf("key %s: expected %s, got %s", kv.Key, expected, kv.Value)
		}
	}
}

func TestBucketScan(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err := bucket.Set([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("failed to set key %s: %v", k, err)
		}
	}

	count := 0
	err = bucket.Scan(func(key, value []byte) bool {
		expected, ok := testData[string(key)]
		if !ok {
			t.Errorf("unexpected key: %s", key)
		}
		if !bytes.Equal(value, []byte(expected)) {
			t.Errorf("key %s: expected %s, got %s", key, expected, value)
		}
		count++
		return true
	})

	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if count != len(testData) {
		t.Errorf("expected %d items, scanned %d", len(testData), count)
	}
}

func TestMultipleBuckets(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket1 := db.Bucket("bucket1")
	bucket2 := db.Bucket("bucket2")

	err = bucket1.Set([]byte("key"), []byte("value1"))
	if err != nil {
		t.Fatalf("failed to set in bucket1: %v", err)
	}

	err = bucket2.Set([]byte("key"), []byte("value2"))
	if err != nil {
		t.Fatalf("failed to set in bucket2: %v", err)
	}

	val1, err := bucket1.Get([]byte("key"))
	if err != nil {
		t.Fatalf("failed to get from bucket1: %v", err)
	}
	if !bytes.Equal(val1, []byte("value1")) {
		t.Errorf("bucket1: expected value1, got %s", val1)
	}

	val2, err := bucket2.Get([]byte("key"))
	if err != nil {
		t.Fatalf("failed to get from bucket2: %v", err)
	}
	if !bytes.Equal(val2, []byte("value2")) {
		t.Errorf("bucket2: expected value2, got %s", val2)
	}
}

func TestConcurrentOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")
	var wg sync.WaitGroup
	numGoroutines := 100
	numOps := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := []byte(string(rune(id)) + "-" + string(rune(j)))
				value := []byte("value")
				if err := bucket.Set(key, value); err != nil {
					t.Errorf("concurrent set failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify data is accessible
	count := 0
	err = bucket.Scan(func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	expected := numGoroutines * numOps
	if count != expected {
		t.Errorf("expected %d keys, got %d", expected, count)
	}
}

func TestStats(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	// Add some data
	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))
		if err := bucket.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	stats := db.Stats()
	if stats == nil {
		t.Fatal("stats is nil")
	}

	// Check that stats contains expected keys
	expectedKeys := []string{"num_keys", "file_size_bytes", "logical_offset", "bloom_insertions", "bloom_fpr", "cache_size", "estimated_keys"}
	for _, key := range expectedKeys {
		if _, ok := stats[key]; !ok {
			t.Errorf("stats missing key: %s", key)
		}
	}
}

func TestSize(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	initialSize := db.Size()

	err = bucket.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	newSize := db.Size()
	if newSize <= initialSize {
		t.Errorf("expected size to increase, got %d <= %d", newSize, initialSize)
	}
}

func TestEstimatedKeyCount(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath,
		WithCardinality(14),
	)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	// Add some unique keys
	for i := 0; i < 1000; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value")
		if err := bucket.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Flush to ensure all data is written
	if err := db.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	estimated := db.EstimatedKeyCount()
	if estimated < 900 || estimated > 1100 {
		t.Errorf("estimated key count %d is not within expected range (900-1100)", estimated)
	}
}

func TestGetKeyFrequency(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath,
		WithFrequency(true),
	)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	key := []byte("frequent-key")
	value := []byte("value")

	// Set the same key multiple times
	for i := 0; i < 5; i++ {
		if err := bucket.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	freq := bucket.GetKeyFrequency(key)
	if freq < 1 {
		t.Errorf("expected frequency > 0, got %d", freq)
	}
}

func TestFlush(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))
		if err := bucket.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}
}

func TestCompact(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath,
		WithCompaction(10, 0.1, 1*time.Minute),
	)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	// Add some data
	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))
		if err := bucket.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Delete half of them
	for i := 0; i < 50; i++ {
		key := []byte("key" + string(rune(i)))
		if err := bucket.Delete(key); err != nil {
			t.Fatalf("failed to delete key: %v", err)
		}
	}

	if err := db.CompactIfNeeded(); err != nil {
		t.Fatalf("compact if needed failed: %v", err)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	// Verify remaining data
	for i := 50; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		if !bucket.Has(key) {
			t.Errorf("key %d should exist", i)
		}
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// First session - write data
	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	bucket1 := db1.Bucket("test")
	testData := map[string]string{
		"persist1": "value1",
		"persist2": "value2",
		"persist3": "value3",
	}

	for k, v := range testData {
		if err := bucket1.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	if err := db1.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	if err := db1.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	// Second session - read data
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db2.Close()

	bucket2 := db2.Bucket("test")

	for k, expected := range testData {
		got, err := bucket2.Get([]byte(k))
		if err != nil {
			t.Fatalf("failed to get key %s: %v", k, err)
		}
		if !bytes.Equal(got, []byte(expected)) {
			t.Errorf("key %s: expected %s, got %s", k, expected, got)
		}
	}
}

func TestErrorCases(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	// Test Get non-existent key
	_, err = bucket.Get([]byte("nonexistent"))
	if err == nil {
		t.Error("expected error for non-existent key")
	}

	// Test Delete non-existent key (should not error)
	err = bucket.Delete([]byte("nonexistent"))
	if err != nil {
		t.Errorf("delete of non-existent key should not error: %v", err)
	}
}

func BenchmarkSet(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + string(rune(i%10000)))
		value := []byte("value")
		if err := bucket.Set(key, value); err != nil {
			b.Fatalf("set failed: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	// Pre-populate data
	for i := 0; i < 10000; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value")
		if err := bucket.Set(key, value); err != nil {
			b.Fatalf("pre-populate failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + string(rune(i%10000)))
		if _, err := bucket.Get(key); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkConcurrentSet(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	bucket := db.Bucket("test")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte("key" + string(rune(i)))
			value := []byte("value")
			if err := bucket.Set(key, value); err != nil {
				b.Fatalf("set failed: %v", err)
			}
			i++
		}
	})
}

func TestOpenNonExistentDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nonexistent", "test.db")

	_, err := Open(dbPath)
	if err == nil {
		t.Error("expected error for non-existent directory")
	}
}
