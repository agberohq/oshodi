package storage

// Jepsen-style WAL recovery: bit-rot anywhere must trim to the last intact
// record; surviving records decode exactly; nothing torn can be returned.

import (
	"fmt"
	"os"
	"testing"
)

// jepsenFrame returns the physical on-disk frame length for a WAL record.
func jepsenFrame(key, value []byte) int64 {
	return int64(4 + len(key) + 4 + len(value) + RecordCRCSize)
}

func TestWALBitRotRecovery(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/jepsen.wal"

	wal, err := NewWAL(&WALConfig{Path: path, Mode: SyncMode})
	if err != nil {
		t.Fatal(err)
	}
	var offset int64
	for i := 1; i <= 40; i++ {
		key := []byte(fmt.Sprintf("wk%d", i))
		val := []byte(fmt.Sprintf("payload-%03d", i))
		if _, err := wal.Write(nil, key, val); err != nil {
			t.Fatal(err)
		}
		if i == 10 {
			offset = wal.Size() - jepsenFrame(key, val)
		}
	}
	wal.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(raw)) < int64(offset+80) {
		t.Fatalf("short WAL: %d", len(raw))
	}

	// Bit-rot one byte inside record 10's payload. The loader must discard
	// record 10 and everything after it, trimming to record 9's boundary.
	bound := int64(0)
	for i := 1; i <= 9; i++ {
		bound += jepsenFrame([]byte(fmt.Sprintf("wk%d", i)), []byte(fmt.Sprintf("payload-%03d", i)))
	}
	bad := append([]byte(nil), raw...)
	bad[offset+9] ^= 0xFF
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}

	wal2, err := NewWAL(&WALConfig{Path: path})
	if err != nil {
		t.Fatalf("reopen after bit-rot: %v", err)
	}
	defer wal2.Close()

	if got := wal2.Size(); got != bound {
		t.Fatalf("WAL trimmed to %d, want %d (record 9 boundary)", got, bound)
	}
	reader := wal2.NewReader(0)
	n := 0
	for {
		if _, err := reader.ReadNext(nil); err != nil {
			break
		}
		n++
	}
	if n != 9 {
		t.Fatalf("expected 9 intact records, got %d", n)
	}
}
