package storage

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

func TestWALBasicWriteRead(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := NewWAL(filepath.Join(tmpDir, "test.wal"), 1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Write
	offset, err := wal.Write(context.Background(), key, value)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if offset < 0 {
		t.Errorf("expected positive offset, got %d", offset)
	}

	// Read back
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
	wal, err := NewWAL(filepath.Join(tmpDir, "test.wal"), 1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write multiple records
	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))
		_, err := wal.Write(context.Background(), key, value)
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}
}

func TestWALRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create and write
	wal1, err := NewWAL(walPath, 1024*1024)
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

	// Reopen and read all entries (simulate recovery)
	wal2, err := NewWAL(walPath, 1024*1024)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	reader := wal2.NewReader(0)
	recovered := 0
	for {
		data, err := reader.ReadNext(nil)
		if err != nil {
			break // EOF or error
		}
		// Simple decode check
		if len(data) > 8 {
			recovered++
		}
	}
	if recovered != 50 {
		t.Errorf("expected to recover 50 records, got %d", recovered)
	}
}

func TestWALConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := NewWAL(filepath.Join(tmpDir, "test.wal"), 4*1024*1024)
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
				value := []byte("value")
				_, err := wal.Write(context.Background(), key, value)
				if err != nil {
					t.Errorf("concurrent write failed: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()
}

func BenchmarkWALWrite(b *testing.B) {
	tmpDir := b.TempDir()
	wal, err := NewWAL(filepath.Join(tmpDir, "bench.wal"), 4*1024*1024)
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
