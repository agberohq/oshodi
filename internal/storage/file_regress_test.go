package storage

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/agberohq/oshodi/internal/consts"
)

// Trimming the logical tail after a torn write must never shrink the file
// allocation, or the next append physically truncates whatever valid bytes
// sit beyond the trim point.
func TestSetLogicalSizeKeepsAllocation(t *testing.T) {
	f, err := NewFile(DefaultFileConfig(filepath.Join(t.TempDir(), "s.db")))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	before := f.Size()
	if before <= 0 {
		t.Fatalf("expected preallocation, got %d", before)
	}

	f.SetLogicalSize(64)
	if f.Size() < before {
		t.Fatalf("SetLogicalSize shrank the allocation: %d -> %d", before, f.Size())
	}
}

// A read of a record with a huge key must not return an oversized buffer to
// the metadata pool, or one large key poisons it for every future read.
func TestMetaBufferPoolNotPoisoned(t *testing.T) {
	f, err := NewFile(DefaultFileConfig(filepath.Join(t.TempDir(), "m.db")))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	bigKey := bytes.Repeat([]byte{8}, 1<<20)
	off, err := f.WriteRecord(bigKey, []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadValueAt(off, bigKey, nil); err != nil {
		t.Fatal(err)
	}

	bufPtr := metaBufPool.Get().(*[]byte)
	if cap(*bufPtr) > consts.MetaBufferSize*4 {
		t.Fatalf("meta buffer pool holds a %d-byte buffer", cap(*bufPtr))
	}
}
