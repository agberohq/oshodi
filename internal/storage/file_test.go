// internal/storage/file_test.go
package storage

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
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

	// Wait a bit for retry queue to process
	time.Sleep(100 * time.Millisecond)

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

	// Initially, file should be empty
	if !f.IsEmpty() {
		t.Error("new file should be empty")
	}

	// Write something
	_, err = f.WriteRecord([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Wait for retry queue
	time.Sleep(100 * time.Millisecond)

	// Now file should not be empty
	if f.IsEmpty() {
		t.Error("file should not be empty after write")
	}
}
