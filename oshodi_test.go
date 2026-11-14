package oshodi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
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

// TestBucketScanner verifies the zero-allocation scan returns exactly what the
// copying Scan returns. Callbacks copy before retaining, as documented.
func TestBucketScanner(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(filepath.Join(tmpDir, "scanview.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	b := db.Bucket("s")
	want := map[string]string{
		"alpha":   "one",
		"beta":    "two",
		"gamma":   "three",
		"delta":   "four",
		"epsilon": "five",
	}
	for k, v := range want {
		if err := b.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	got := make(map[string]string, len(want))
	var lastKey, lastVal []byte
	if err := b.Scanner(func(k, v []byte) bool {
		got[string(k)] = string(v)
		lastKey, lastVal = k, v
		return true
	}); err != nil {
		t.Fatalf("Scanner: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("Scanner returned %d records, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("Scanner[%q] = %q, want %q", k, got[k], v)
		}
	}
	// Buffers are reused, so the final slices must still be readable and
	// consistent with the last callback (no dangling memory).
	if len(lastKey) == 0 || len(lastVal) == 0 {
		t.Fatal("Scanner handed back empty slices")
	}

	// An empty bucket must scan cleanly.
	if err := db.Bucket("empty").Scanner(func(k, v []byte) bool {
		t.Fatal("callback ran for empty bucket")
		return false
	}); err != nil {
		t.Fatalf("empty Scanner: %v", err)
	}
}

// TestBucketGetter covers the zero-copy read path: values are delivered on
// both the cache-miss and cache-hit paths, callback errors propagate, and a
// missing key reports ErrKeyNotFound.
func TestBucketGetter(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "getview.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	b := db.Bucket("b")
	if err := b.Set([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("set: %v", err)
	}

	for i := 0; i < 2; i++ { // miss, then hit
		var got string
		if err := b.Getter([]byte("alpha"), func(v []byte) error {
			got = string(v)
			return nil
		}); err != nil {
			t.Fatalf("Getter %d: %v", i, err)
		}
		if got != "one" {
			t.Fatalf("Getter %d value = %q, want one", i, got)
		}
	}

	if err := b.Getter([]byte("missing"), func(v []byte) error { return nil }); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("missing key err = %v, want ErrKeyNotFound", err)
	}

	want := errors.New("stop")
	if err := b.Getter([]byte("alpha"), func(v []byte) error { return want }); !errors.Is(err, want) {
		t.Fatalf("callback error = %v, want %v", err, want)
	}
}

// TestBucketScannerE covers the error-propagating reusable-buffer scan.
func TestBucketScannerE(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "scannere.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	b := db.Bucket("b")
	want := map[string]string{"a": "one", "b": "two", "c": "three"}
	for k, v := range want {
		if err := b.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	got := map[string]string{}
	if err := b.ScannerE(context.Background(), func(k, v []byte) error {
		got[string(k)] = string(v) // copy before the buffers are reused
		return nil
	}); err != nil {
		t.Fatalf("ScannerE: %v", err)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("ScannerE[%q] = %q, want %q", k, got[k], v)
		}
	}

	wantErr := errors.New("stop")
	if err := b.ScannerE(context.Background(), func(k, v []byte) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("callback error = %v, want %v", err, wantErr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.ScannerE(ctx, func(k, v []byte) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scan error = %v, want context.Canceled", err)
	}
}

func TestBucketBatchDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batchdel.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	b := db.Bucket("b")
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := b.Set([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}
	if err := b.BatchDelete([][]byte{[]byte("a"), []byte("c"), []byte("e")}); err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	for _, k := range []string{"a", "c", "e"} {
		if _, err := b.Get([]byte(k)); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("%q survived batch delete: %v", k, err)
		}
	}
	for _, k := range []string{"b", "d"} {
		if v, err := b.Get([]byte(k)); err != nil || string(v) != "v-"+k {
			t.Fatalf("%q = %q, %v", k, v, err)
		}
	}
	if err := b.BatchDelete([][]byte{[]byte("")}); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("empty key err = %v, want ErrKeyEmpty", err)
	}
	if err := b.BatchDelete(nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Deletions must survive a reopen.
	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	b = db.Bucket("b")
	if all, err := b.GetAll(); err != nil || len(all) != 2 {
		t.Fatalf("records after reopen = %d, %v; want 2", len(all), err)
	}
	for _, k := range []string{"a", "c", "e"} {
		if _, err := b.Get([]byte(k)); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("%q resurrected after reopen: %v", k, err)
		}
	}
}

func TestBucketBatchDeleteWithWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batchdel-wal.db")
	db, err := Open(path, WithWAL(true))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	b := db.Bucket("b")
	for _, k := range []string{"a", "b", "c", "d"} {
		if err := b.Set([]byte(k), []byte(k)); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}
	if err := b.BatchDelete([][]byte{[]byte("a"), []byte("c")}); err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(path, WithWAL(true))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	b = db.Bucket("b")
	if all, err := b.GetAll(); err != nil || len(all) != 2 {
		t.Fatalf("after reopen = %d records, %v; want 2", len(all), err)
	}
	for _, k := range []string{"a", "c"} {
		if _, err := b.Get([]byte(k)); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("%q resurrected: %v", k, err)
		}
	}
}

func TestBucketGetterMany(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gettermany.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	b := db.Bucket("b")
	for _, k := range []string{"a", "b", "c"} {
		if err := b.Set([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	got := map[string]string{}
	err = b.GetterMany([][]byte{
		[]byte("a"), []byte("missing"), []byte("c"),
	}, func(k, v []byte) error {
		got[string(k)] = string(v) // copy before reuse
		return nil
	})
	if err != nil {
		t.Fatalf("GetterMany: %v", err)
	}
	if len(got) != 2 || got["a"] != "v-a" || got["c"] != "v-c" {
		t.Fatalf("GetterMany values = %v", got)
	}

	want := errors.New("stop")
	if err := b.GetterMany([][]byte{[]byte("a"), []byte("b")}, func(k, v []byte) error { return want }); !errors.Is(err, want) {
		t.Fatalf("callback error = %v, want %v", err, want)
	}
	if err := b.GetterMany(nil, func(k, v []byte) error { return nil }); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
}

func TestBucketOrderedScans(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ordered.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	b := db.Bucket("songs")
	for _, k := range []string{"b", "a", "c", "aa", "ab"} {
		if err := b.Set([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}

	collect := func(scan func(fn func(k, v []byte) bool) error) (keys []string, vals []string) {
		t.Helper()
		if err := scan(func(k, v []byte) bool {
			keys = append(keys, string(k))
			vals = append(vals, string(v))
			return true
		}); err != nil {
			t.Fatalf("scan: %v", err)
		}
		return keys, vals
	}
	eq := func(got, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	}

	keys, vals := collect(func(fn func(k, v []byte) bool) error { return b.ScanRange(nil, nil, fn) })
	eq(keys, []string{"a", "aa", "ab", "b", "c"})
	eq(vals, []string{"v-a", "v-aa", "v-ab", "v-b", "v-c"})

	keys, _ = collect(func(fn func(k, v []byte) bool) error { return b.ScanRange([]byte("aa"), []byte("c"), fn) })
	eq(keys, []string{"aa", "ab", "b"})

	keys, _ = collect(func(fn func(k, v []byte) bool) error { return b.ScanPrefix([]byte("a"), fn) })
	eq(keys, []string{"a", "aa", "ab"})

	keys, _ = collect(func(fn func(k, v []byte) bool) error { return b.ScanRangeDesc(nil, nil, fn) })
	eq(keys, []string{"c", "b", "ab", "aa", "a"})

	keys, _ = collect(func(fn func(k, v []byte) bool) error { return b.ScanPrefixDesc([]byte("a"), fn) })
	eq(keys, []string{"ab", "aa", "a"})

	// Updates and deletes must invalidate the ordered snapshot.
	if err := b.Set([]byte("b"), []byte("B2")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("aa")); err != nil {
		t.Fatal(err)
	}
	keys, vals = collect(func(fn func(k, v []byte) bool) error { return b.ScanRange(nil, nil, fn) })
	eq(keys, []string{"a", "ab", "b", "c"})
	eq(vals, []string{"v-a", "v-ab", "B2", "v-c"})

	// Compaction swaps the index; scans must still resolve current offsets.
	if err := db.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	keys, _ = collect(func(fn func(k, v []byte) bool) error { return b.ScanRange(nil, nil, fn) })
	eq(keys, []string{"a", "ab", "b", "c"})

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	b = db.Bucket("songs")
	keys, _ = collect(func(fn func(k, v []byte) bool) error { return b.ScanRange(nil, nil, fn) })
	eq(keys, []string{"a", "ab", "b", "c"})

	// Early stop and empty ranges.
	var seen []string
	if err := b.ScanRange(nil, nil, func(k, v []byte) bool {
		seen = append(seen, string(k))
		return len(seen) < 2
	}); err != nil {
		t.Fatal(err)
	}
	eq(seen, []string{"a", "ab"})
	if err := b.ScanRange([]byte("b"), []byte("b"), func(k, v []byte) bool { t.Fatal("unexpected"); return false }); err != nil {
		t.Fatal(err)
	}
}

func TestBucketQuery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	b := db.Bucket("b")
	for i := 1; i <= 10; i++ {
		k := fmt.Sprintf("k%02d", i)
		if err := b.Set([]byte(k), []byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}

	if n, err := b.Query().Count(); err != nil || n != 10 {
		t.Fatalf("Count = %d, %v", n, err)
	}
	if n, err := b.Query().Range([]byte("k03"), []byte("k07")).Count(); err != nil || n != 4 {
		t.Fatalf("Range count = %d, %v", n, err)
	}
	if n, err := b.Query().Prefix([]byte("k0")).Count(); err != nil || n != 9 {
		t.Fatalf("Prefix count = %d, %v", n, err)
	}
	if n, err := b.Query().Limit(3).Count(); err != nil || n != 3 {
		t.Fatalf("Limit count = %d, %v", n, err)
	}
	if n, err := b.Query().Limit(0).Count(); err != nil || n != 0 {
		t.Fatalf("Limit(0) count = %d, %v", n, err)
	}

	var desc []string
	if err := b.Query().Descending().Limit(1).ForEach(func(k, v []byte) error {
		desc = append(desc, string(k))
		return nil
	}); err != nil || len(desc) != 1 || desc[0] != "k10" {
		t.Fatalf("descending = %v, %v", desc, err)
	}

	even := func(k, v []byte) bool {
		n, _ := strconv.Atoi(string(v))
		return n%2 == 0
	}
	if n, err := b.Query().Filter(even).Count(); err != nil || n != 5 {
		t.Fatalf("filtered count = %d, %v", n, err)
	}
	value := func(k, v []byte) (float64, error) {
		return strconv.ParseFloat(string(v), 64)
	}
	if sum, err := b.Query().Sum(value); err != nil || sum != 55 {
		t.Fatalf("sum = %v, %v", sum, err)
	}
	if min, found, err := b.Query().Min(value); err != nil || !found || min != 1 {
		t.Fatalf("min = %v found=%v err=%v", min, found, err)
	}
	if max, found, err := b.Query().Max(value); err != nil || !found || max != 10 {
		t.Fatalf("max = %v found=%v err=%v", max, found, err)
	}
	groupKey := func(k, v []byte) (string, error) { return string(k[:2]), nil }
	if groups, err := b.Query().GroupByCount(groupKey); err != nil || groups["k0"] != 9 || groups["k1"] != 1 {
		t.Fatalf("group count = %v, %v", groups, err)
	}
	if groups, err := b.Query().GroupBySum(groupKey, value); err != nil || groups["k0"] != 45 || groups["k1"] != 10 {
		t.Fatalf("group sum = %v, %v", groups, err)
	}

	if _, err := b.Query().Range([]byte("a"), []byte("z")).Prefix([]byte("k")).Count(); !errors.Is(err, ErrQueryConflict) {
		t.Fatalf("Range+Prefix err = %v, want ErrQueryConflict", err)
	}
	wantErr := errors.New("stop")
	if err := b.Query().ForEach(func(k, v []byte) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("callback error = %v, want %v", err, wantErr)
	}
	if err := b.Query().Prefix(nil).Descending().ForEach(func(k, v []byte) error { return nil }); err != nil {
		t.Fatalf("prefix-all desc: %v", err)
	}
}

func TestOrderedScanBatchInvalidation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "ordered-batch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := db.Bucket("b")

	if err := b.BatchSet([]KV{
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}); err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err := b.ScanRangeKeys(nil, nil, func(k []byte) bool {
		keys = append(keys, string(k))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Fatalf("after BatchSet: %v", keys)
	}

	if err := b.BatchDelete([][]byte{[]byte("b")}); err != nil {
		t.Fatal(err)
	}
	keys = keys[:0]
	if err := b.ScanPrefixKeys([]byte(""), func(k []byte) bool {
		keys = append(keys, string(k))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "c" {
		t.Fatalf("after BatchDelete: %v", keys)
	}
}

func TestOrderedScansAcrossBuckets(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "ordered-buckets.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	a, b := db.Bucket("a"), db.Bucket("b")
	for _, k := range []string{"a2", "a1", "a3"} {
		if err := a.Set([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{"b2", "b1"} {
		if err := b.Set([]byte(k), []byte(k)); err != nil {
			t.Fatal(err)
		}
	}

	keys := func(bk *Bucket) []string {
		t.Helper()
		var out []string
		if err := bk.ScanRangeKeys(nil, nil, func(k []byte) bool {
			out = append(out, string(k))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := keys(a); len(got) != 3 || got[0] != "a1" || got[1] != "a2" || got[2] != "a3" {
		t.Fatalf("bucket a = %v", got)
	}
	if got := keys(b); len(got) != 2 || got[0] != "b1" || got[1] != "b2" {
		t.Fatalf("bucket b = %v", got)
	}

	// A write in b must not disturb a's ordered view.
	if err := b.Set([]byte("b0"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if got := keys(a); len(got) != 3 || got[0] != "a1" {
		t.Fatalf("bucket a after b write = %v", got)
	}
	if got := keys(b); len(got) != 3 || got[0] != "b0" {
		t.Fatalf("bucket b after write = %v", got)
	}

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := keys(a); len(got) != 3 || got[2] != "a3" {
		t.Fatalf("bucket a after compact = %v", got)
	}
	if got := keys(b); len(got) != 3 || got[2] != "b2" {
		t.Fatalf("bucket b after compact = %v", got)
	}
}
