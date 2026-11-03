package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/agberohq/oshodi/internal/storage"
)

// ---- C1 regression: BatchSet + compression must decode correctly ----

func TestBatchSetWithCompressionRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "t.db")

	db, err := NewDB(&Config{
		FilePath:             dbPath,
		EnableCompression:    true,
		CompressionThreshold: 64,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Easily compressible payload > threshold.
	payload := bytes.Repeat([]byte("compress-me"), 64)
	entries := make([]KV, 10)
	for i := range entries {
		entries[i] = KV{Key: []byte{byte('a' + i)}, Value: append([]byte(nil), payload...)}
	}

	if err := db.BatchSet(entries); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}

	gtk := []byte("g")
	got, err := db.Get(gtk)
	if err != nil {
		t.Fatalf("get after batch: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("compressed batch value corrupted: len=%d want=%d", len(got), len(payload))
	}

	// The caller's entries must not be replaced with compressed bytes.
	if !bytes.Equal(entries[3].Value, payload) {
		t.Fatal("BatchSet mutated caller's entry values")
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	got, err = db.Get(gtk)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("post-flush read: %v", err)
	}
}

// ---- C3 regression: empty values are first-class, never deletes ----

func TestEmptyValueSurvivesRestart(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "t.db")

	db, err := NewDB(&Config{FilePath: dbPath, DisableWAL: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := db.Set([]byte("empty"), []byte{}); err != nil {
		t.Fatalf("set empty: %v", err)
	}
	if err := db.Set([]byte("full"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Restart: empty value must load back (not be mistaken for a tombstone).
	db2, err := NewDB(&Config{FilePath: dbPath, DisableWAL: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	got, err := db2.Get([]byte("empty"))
	if err != nil {
		t.Fatalf("empty value lost on restart: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty value, got %q", got)
	}
	got, err = db2.Get([]byte("full"))
	if err != nil || string(got) != "v" {
		t.Fatalf("full value lost on restart: %v", err)
	}
}

func TestSetThenDeleteStaysDeletedAcrossRestart(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "t.db")

	db, err := NewDB(&Config{FilePath: dbPath})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := NewDB(&Config{FilePath: dbPath})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if _, err := db2.Get([]byte("k")); err != ErrKeyNotFound {
		t.Fatalf("deleted key resurrected on restart: %v", err)
	}
}

// ---- C4 regression: same-key writers must not leave a stale index ----

func TestConcurrentSameKeyWritesAlwaysReadLatest(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := NewDB(&Config{FilePath: filepath.Join(tmpDir, "t.db")})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const rounds = 400
	for i := 0; i < rounds; i++ {
		v := []byte{byte(i / 256), byte(i % 256)}
		if err := db.Set([]byte("hot"), v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
		// The acknowledged write must be observable immediately.
		got, err := db.Get([]byte("hot"))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("round %d: stale index returned %x, want %x", i, got, v)
		}
	}
}

// ---- C5 regression: delete during compaction must not resurrect ----

func TestCompactWithDeletesNoResurrection(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := NewDB(&Config{
		FilePath:                filepath.Join(tmpDir, "t.db"),
		CompactionMinItems:      1,
		CompactionFragmentation: 0.01,
		CompactionInterval:      0, // effectively idle; we compact manually
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	deleted := []byte("doomed")
	if err := db.Set(deleted, []byte("x")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Delete(deleted); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Tombstone must not be reusable as data for a stil-live read.
	if _, err := db.Get(deleted); err != ErrKeyNotFound {
		t.Fatalf("pre-compact read of deleted key: %v", err)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, err := db.Get(deleted); err != ErrKeyNotFound {
		t.Fatalf("deleted key RESURRECTED by compaction: %v", err)
	}

	// Live data survives compaction intact.
	if err := db.Set([]byte("survivor"), []byte("s")); err != nil {
		t.Fatalf("post-compact set: %v", err)
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("second compact: %v", err)
	}
	got, err := db.Get([]byte("survivor"))
	if err != nil || string(got) != "s" {
		t.Fatalf("survivor lost: %v", err)
	}
}

// ---- R3 regression: oversized keys rejected, never OOM-parsed ----

func TestMaxKeySizeEnforced(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := NewDB(&Config{
		FilePath:   filepath.Join(tmpDir, "t.db"),
		MaxKeySize: 16,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Set(make([]byte, 17), []byte("v")); err != ErrKeyTooLarge {
		t.Fatalf("expected ErrKeyTooLarge, got %v", err)
	}
	if err := db.BatchSet([]KV{{Key: make([]byte, 17), Value: []byte("v")}}); err != ErrKeyTooLarge {
		t.Fatalf("expected ErrKeyTooLarge from BatchSet, got %v", err)
	}
}

// ---- WAL end-to-end: tombstone sentinel replay + WAL rotation ----

func TestWALTombstoneAndEmptyValueReplay(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "t.db")

	db, err := NewDB(&Config{
		FilePath: dbPath,
		WALSync:  true, // sync mode: WAL bytes durable when Set returns
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := db.Set([]byte("gone"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Delete([]byte("gone")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := db.Set([]byte("stays"), []byte("s")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Set([]byte("blank"), []byte{}); err != nil {
		t.Fatalf("set empty: %v", err)
	}

	// Simulate a crash: do NOT flush. Close hard, then reopen — the WAL
	// must replay with correct tombstone vs empty-value semantics.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := NewDB(&Config{FilePath: dbPath, WALSync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	if _, err := db2.Get([]byte("gone")); err != ErrKeyNotFound {
		t.Fatalf("deleted key resurrected by WAL replay: %v", err)
	}
	got, err := db2.Get([]byte("stays"))
	if err != nil || string(got) != "s" {
		t.Fatalf("set lost by WAL replay: %v", err)
	}
	got, err = db2.Get([]byte("blank"))
	if err != nil || len(got) != 0 {
		t.Fatalf("empty value lost or corrupted by WAL replay: %v (err=%v)", got, err)
	}
}

// ---- WAL copy-on-write: caller mutating its buffer must not corrupt records ----

func TestWALCallerBufferMutationSafe(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "t.wal")

	wal, err := storage.NewWAL(&storage.WALConfig{
		Path:       walPath,
		BufferSize: 4096,
		Mode:       storage.SyncMode,
		Sync:       true,
	})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}

	key := []byte("key-1")
	value := make([]byte, 32)
	copy(value, "the-original-value")

	bg := context.Background()
	if _, err := wal.Write(bg, key, value); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Caller reuses/overwrites its buffers — perfectly legal after Write returns.
	for i := range value {
		value[i] = 0xFF
	}
	for i := range key {
		key[i] = 'z'
	}

	if _, _, err := wal.Read(bg, 0); err != nil {
		t.Fatalf("read: %v", err)
	}

	// Flush to disk, then scan the raw file: the original value bytes must
	// have been serialised, not the caller's trailing garbage.
	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}
	if !bytes.Contains(raw, []byte("the-original-value")) {
		t.Fatal("WAL serialised the caller's MUTATED buffer — copy-on-write broken")
	}
	if bytes.Contains(raw, []byte("zzz")) {
		t.Fatal("WAL serialised the caller's MUTATED key — copy-on-write broken")
	}
}

// ---- R1 regression: torn WAL tail truncates instead of failing open ----

func TestWALTornTailTolerated(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "t.wal")

	wal, err := storage.NewWAL(&storage.WALConfig{
		Path: walPath,
		Mode: storage.AsyncMode,
		Sync: true,
	})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	bg := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := wal.Write(bg, []byte("kkk"), []byte{byte(i)}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := wal.Flush(bg); err != nil {
		t.Fatalf("wal flush: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrogate the tail: chop mid-record.
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(walPath, int64(len(raw)-2)); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	wal2, err := storage.NewWAL(&storage.WALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("reopen after torn tail: %v", err)
	}
	defer wal2.Close()
	if wal2.Size() == 0 {
		t.Fatal("healthy records were discarded by torn-tail handling")
	}
}
