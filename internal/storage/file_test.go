package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestNewFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	f, err := NewFile(filePath, 1024, 1024, nil)
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

	f, err := NewFile(filePath, 1024, 1024, nil)
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

	t.Logf("Wrote record at offset %d, file size: %d", offset, f.Size())

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

func TestFileWriteTombstone(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	f, err := NewFile(filePath, 1024, 1024, nil)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("test-key")

	// Write tombstone directly
	err = f.WriteTombstone(key)
	if err != nil {
		t.Fatalf("write tombstone failed: %v", err)
	}

	// Read the tombstone back
	var offset int64
	var found bool

	// Scan through the file to find the tombstone
	for offset < f.Size() {
		readKey, _, tombstone, n, err := f.ReadRecordAt(offset)
		if err != nil {
			break
		}
		if tombstone && bytes.Equal(readKey, key) {
			found = true
			break
		}
		offset += n
	}

	if !found {
		t.Error("tombstone not found in file")
	}
}

func TestFileSync(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	f, err := NewFile(filePath, 1024, 1024, nil)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	if err := f.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
}

func TestFileTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	f, err := NewFile(filePath, 1024, 1024, nil)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	if f.Size() != 1024 {
		t.Errorf("expected size 1024, got %d", f.Size())
	}

	if err := f.Truncate(2048); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	if f.Size() != 2048 {
		t.Errorf("expected size 2048, got %d", f.Size())
	}
}

func TestFileIsEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	f, err := NewFile(filePath, 1024, 1024, nil)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	// Empty file with zeros
	if !f.IsEmpty() {
		t.Error("file should be empty")
	}

	// Write something
	_, err = f.WriteRecord([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if f.IsEmpty() {
		t.Error("file should not be empty")
	}
}

func TestFileClose(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	f, err := NewFile(filePath, 1024, 1024, nil)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestFileCompression(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	compressor, err := NewCompressor(10, 3)
	if err != nil {
		t.Fatalf("failed to create compressor: %v", err)
	}
	defer compressor.Close()

	f, err := NewFile(filePath, 1024, 1024, compressor)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	// Create a large, repetitive value that compresses well
	key := []byte("compress-key")
	value := make([]byte, 10000)
	for i := range value {
		value[i] = byte('a')
	}

	offset, err := f.WriteRecord(key, value)
	if err != nil {
		t.Fatalf("write record failed: %v", err)
	}

	t.Logf("Wrote compressed record at offset %d, file size: %d", offset, f.Size())

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
		t.Errorf("value mismatch")
	}
	if n <= 0 {
		t.Errorf("expected positive n, got %d", n)
	}
}

func TestFileMultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	f, err := NewFile(filePath, 1024, 1024, nil)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	records := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := "key" + string(rune(i))
		value := "value" + string(rune(i))
		records[key] = value

		_, err := f.WriteRecord([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("write record %d failed: %v", i, err)
		}
	}

	// Read all records
	var offset int64
	for offset < f.Size() {
		key, value, tombstone, n, err := f.ReadRecordAt(offset)
		if err != nil {
			break
		}
		if !tombstone {
			expected, ok := records[string(key)]
			if !ok {
				t.Errorf("unexpected key: %s", key)
			}
			if !bytes.Equal(value, []byte(expected)) {
				t.Errorf("key %s: expected %s, got %s", key, expected, value)
			}
		}
		offset += n
	}
}

func BenchmarkFileWriteRecord(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	f, err := NewFile(filePath, 1024, 1024, nil)
	if err != nil {
		b.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.WriteRecord(key, value)
	}
}

func BenchmarkFileReadRecord(b *testing.B) {
	tmpDir := b.TempDir()
	filePath := filepath.Join(tmpDir, "test.db")

	f, err := NewFile(filePath, 1024, 1024, nil)
	if err != nil {
		b.Fatalf("failed to create file: %v", err)
	}
	defer f.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	offset, err := f.WriteRecord(key, value)
	if err != nil {
		b.Fatalf("write failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.ReadRecordAt(offset)
	}
}
