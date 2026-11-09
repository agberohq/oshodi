package engine

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/agberohq/oshodi/internal/pipeline"
	"github.com/agberohq/oshodi/internal/storage"
)

// C1 regression: BatchSet + compression must decode correctly

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

// C3 regression: empty values are first-class, never deletes

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

// C4 regression: same-key writers must not leave a stale index

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

// C5 regression: delete during compaction must not resurrect

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

// R3 regression: oversized keys rejected, never OOM-parsed

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

// WAL end-to-end: tombstone sentinel replay + WAL rotation

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

// WAL copy-on-write: caller mutating its buffer must not corrupt records

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

// R1 regression: torn WAL tail truncates instead of failing open

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

// C3 regression: a preallocated zero prefix must not hide the catalogue

// TestOpenIndexesRecordsAfterZeroPrefix pins the muca incident: a run that
// preallocated the file and exited before its first write left a zero prefix;
// records appended after it must be indexed on open, not treated as an empty
// database.
func TestOpenIndexesRecordsAfterZeroPrefix(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")

	// Build a healthy record region, then transplant it behind a zero prefix.
	src, err := NewDB(&Config{FilePath: srcPath, DisableWAL: true, InitialFileSize: 1 << 20})
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	for _, kv := range []struct{ k, v string }{
		{"songs\x00testing", `{"title":"Testing"}`},
		{"songs\x00june-express-v2", `{"title":"June Express V2"}`},
	} {
		if err := src.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("seed %q: %v", kv.k, err)
		}
	}
	logical := src.storage.Load().LogicalSize()
	if err := src.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	raw, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}

	const prefix = 1 << 20
	dstPath := filepath.Join(dir, "muca.db")
	damaged := append(make([]byte, prefix), raw...)
	if err := os.WriteFile(dstPath, damaged, 0o600); err != nil {
		t.Fatalf("write damaged db: %v", err)
	}

	db, err := NewDB(&Config{FilePath: dstPath, DisableWAL: true})
	if err != nil {
		t.Fatalf("open damaged: %v", err)
	}
	if got := db.Stats().NumKeys; got != 2 {
		t.Fatalf("keys after reopen = %d, want 2", got)
	}
	if got := db.storage.Load().LogicalSize(); got != prefix+logical {
		t.Fatalf("write cursor = %d, want %d (last record boundary)", got, prefix+logical)
	}
	if v, err := db.Get([]byte("songs\x00testing")); err != nil || !bytes.Contains(v, []byte("Testing")) {
		t.Fatalf("get recovered record: %v %q", err, v)
	}

	// New writes append at the recovered boundary and survive another reopen.
	if err := db.Set([]byte("songs\x00later"), []byte(`{"title":"Later"}`)); err != nil {
		t.Fatalf("set after recovery: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, err = NewDB(&Config{FilePath: dstPath, DisableWAL: true})
	if err != nil {
		t.Fatalf("reopen after write: %v", err)
	}
	defer db.Close()
	if got := db.Stats().NumKeys; got != 3 {
		t.Fatalf("keys after second reopen = %d, want 3", got)
	}
}

// TestOpenPreallocatedEmptyStartsAtZero covers the origin of the incident: an
// empty preallocated file must reset the write cursor to offset 0 so records
// never land behind a zero prefix in the first place.
func TestOpenPreallocatedEmptyStartsAtZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.db")
	const initial = 1 << 20
	if err := os.WriteFile(path, make([]byte, initial), 0o600); err != nil {
		t.Fatalf("seed preallocated file: %v", err)
	}

	db, err := NewDB(&Config{FilePath: path, DisableWAL: true, InitialFileSize: initial})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := db.storage.Load().LogicalSize(); got != 0 {
		t.Fatalf("write cursor = %d, want 0 for an all-zero file", got)
	}
	if err := db.Set([]byte("songs\x00first"), []byte(`{"title":"First"}`)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if off, ok := db.index.Load().Get([]byte("songs\x00first")); !ok || off != 0 {
		t.Fatalf("first record offset = %d ok=%v, want 0", off, ok)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = NewDB(&Config{FilePath: path, DisableWAL: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if got := db.Stats().NumKeys; got != 1 {
		t.Fatalf("keys after reopen = %d, want 1", got)
	}
	if got := db.storage.Load().LogicalSize(); got >= initial {
		t.Fatalf("write cursor = %d, still behind the preallocated region", got)
	}
}

// C4 regression: recovery boundaries

// validRecordEnds returns the end offset of every contiguous, valid record at
// the start of path. It stops at the first hole or unreadable record.
func validRecordEnds(t *testing.T, path string) []int64 {
	t.Helper()
	st, err := storage.NewFile(&storage.FileConfig{Path: path})
	if err != nil {
		t.Fatalf("open raw storage: %v", err)
	}
	defer st.Close()
	var ends []int64
	var off int64
	for {
		key, _, tomb, n, err := st.ReadRecordAt(off)
		if err != nil || n == 0 {
			break
		}
		if tomb && len(key) == 0 {
			break
		}
		off += n
		ends = append(ends, off)
	}
	return ends
}

func seedTwoRecords(t *testing.T, dir string) (path string, raw []byte, ends []int64) {
	t.Helper()
	path = filepath.Join(dir, "src.db")
	db, err := NewDB(&Config{FilePath: path, DisableWAL: true, InitialFileSize: 1 << 20})
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	for _, kv := range []struct{ k, v string }{
		{"songs\x00alpha", `{"title":"Alpha"}`},
		{"songs\x00beta", `{"title":"Beta"}`},
	} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("seed %q: %v", kv.k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	ends = validRecordEnds(t, path)
	if len(ends) != 2 {
		t.Fatalf("seed records = %d, want 2", len(ends))
	}
	return path, raw, ends
}

func openKeys(t *testing.T, path string) (*DB, map[string]bool) {
	t.Helper()
	db, err := NewDB(&Config{FilePath: path, DisableWAL: true})
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	keys := map[string]bool{}
	for _, k := range []string{"songs\x00alpha", "songs\x00beta", "songs\x00gamma"} {
		if _, err := db.Get([]byte(k)); err == nil {
			keys[k] = true
		}
	}
	return db, keys
}

// TestOpenRecoversMultipleZeroRegions: a zero prefix is not only a leading
// artifact; holes between valid records (interrupted growth, sparse writes)
// must be skipped without stopping the scan.
func TestOpenRecoversMultipleZeroRegions(t *testing.T) {
	dir := t.TempDir()
	_, raw, ends := seedTwoRecords(t, dir)

	const prefix, gap = 1 << 20, 64 << 10
	target := filepath.Join(dir, "multi.db")
	var buf []byte
	buf = append(buf, make([]byte, prefix)...)
	buf = append(buf, raw[:ends[0]]...)
	buf = append(buf, make([]byte, gap)...)
	buf = append(buf, raw[ends[0]:ends[1]]...)
	buf = append(buf, make([]byte, prefix)...)
	if err := os.WriteFile(target, buf, 0o600); err != nil {
		t.Fatalf("write damaged db: %v", err)
	}

	db, keys := openKeys(t, target)
	defer db.Close()
	if !keys["songs\x00alpha"] || !keys["songs\x00beta"] {
		t.Fatalf("recovered keys = %v, want both records", keys)
	}
	want := int64(prefix+gap) + ends[1]
	if got := db.storage.Load().LogicalSize(); got != want {
		t.Fatalf("logical size = %d, want %d", got, want)
	}
}

// TestOpenTrimsAtCorruptRecord pins the safe boundary: recovery stops at the
// first unreadable record instead of skipping a mutation and resurrecting
// later (possibly stale) records.
func TestOpenTrimsAtCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	_, raw, ends := seedTwoRecords(t, dir)

	bad := append([]byte(nil), raw...)
	bad[ends[0]+5] ^= 0xFF // corrupt the second record's payload
	target := filepath.Join(dir, "bitrot.db")
	if err := os.WriteFile(target, bad, 0o600); err != nil {
		t.Fatalf("write bitrot db: %v", err)
	}

	db, keys := openKeys(t, target)
	if keys["songs\x00beta"] {
		t.Fatalf("bit-rotted record was indexed: %v", keys)
	}
	if !keys["songs\x00alpha"] {
		t.Fatalf("healthy prefix lost: %v", keys)
	}
	if got := db.storage.Load().LogicalSize(); got != ends[0] {
		t.Fatalf("logical size = %d, want trim at %d", got, ends[0])
	}
	// The next write must land at the trim point, not resurrect the record.
	if err := db.Set([]byte("songs\x00gamma"), []byte(`{"title":"Gamma"}`)); err != nil {
		t.Fatalf("set after bit rot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, keys = openKeys(t, target)
	defer db.Close()
	if keys["songs\x00beta"] || !keys["songs\x00gamma"] {
		t.Fatalf("state after rewrite = %v, want alpha+gamma only", keys)
	}
}

// TestOpenTrimsTornTail: a partial trailing record is discarded, earlier
// records survive, and the next write starts at the last good boundary.
func TestOpenTrimsTornTail(t *testing.T) {
	dir := t.TempDir()
	_, raw, ends := seedTwoRecords(t, dir)

	cut := ends[0] + (ends[1]-ends[0])/2
	target := filepath.Join(dir, "torn.db")
	if err := os.WriteFile(target, raw[:cut], 0o600); err != nil {
		t.Fatalf("write torn db: %v", err)
	}

	db, keys := openKeys(t, target)
	if keys["songs\x00beta"] || !keys["songs\x00alpha"] {
		t.Fatalf("after torn tail keys = %v, want alpha only", keys)
	}
	if got := db.storage.Load().LogicalSize(); got != ends[0] {
		t.Fatalf("logical size = %d, want %d", got, ends[0])
	}
	if err := db.Set([]byte("songs\x00gamma"), []byte(`{"title":"Gamma"}`)); err != nil {
		t.Fatalf("set after torn tail: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, keys = openKeys(t, target)
	defer db.Close()
	if keys["songs\x00beta"] || !keys["songs\x00gamma"] {
		t.Fatalf("state after rewrite = %v, want alpha+gamma only", keys)
	}
}

// TestCompactReclaimsZeroPrefix: after recovery, compaction must write the
// live records from offset 0, dropping the preallocated hole.
func TestCompactReclaimsZeroPrefix(t *testing.T) {
	dir := t.TempDir()
	_, raw, ends := seedTwoRecords(t, dir)

	const prefix = 1 << 20
	target := filepath.Join(dir, "compact.db")
	if err := os.WriteFile(target, append(make([]byte, prefix), raw[:ends[1]]...), 0o600); err != nil {
		t.Fatalf("write damaged db: %v", err)
	}

	db, keys := openKeys(t, target)
	if !keys["songs\x00alpha"] || !keys["songs\x00beta"] {
		t.Fatalf("recovered keys = %v", keys)
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if got := db.storage.Load().LogicalSize(); got != ends[1] {
		t.Fatalf("logical size after compact = %d, want %d", got, ends[1])
	}
	for _, k := range []string{"songs\x00alpha", "songs\x00beta"} {
		off, ok := db.index.Load().Get([]byte(k))
		if !ok || off >= prefix {
			t.Fatalf("compacted offset for %q = %d ok=%v, still behind the prefix", k, off, ok)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, keys = openKeys(t, target)
	defer db.Close()
	if !keys["songs\x00alpha"] || !keys["songs\x00beta"] {
		t.Fatalf("keys after compact+reopen = %v", keys)
	}
}

// TestOpenRecoversInterruptedCompaction covers the crash window between
// Compact's two renames: the completed .compact file wins over the backup.
func TestOpenRecoversInterruptedCompaction(t *testing.T) {
	dir := t.TempDir()
	main := filepath.Join(dir, "db")

	mk := func(name string, keys ...string) []byte {
		t.Helper()
		p := filepath.Join(dir, name)
		db, err := NewDB(&Config{FilePath: p, DisableWAL: true, InitialFileSize: 1 << 20})
		if err != nil {
			t.Fatalf("build %s: %v", name, err)
		}
		for _, k := range keys {
			if err := db.Set([]byte(k), []byte(k)); err != nil {
				t.Fatalf("set %s: %v", k, err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close %s: %v", name, err)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return raw
	}

	old := mk("old.db", "songs\x00alpha")
	newer := mk("new.db", "songs\x00alpha", "songs\x00beta")

	// Crash after main -> .bak, before .compact -> main.
	if err := os.WriteFile(main+storage.FilenameSuffixBackup, old, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(main+storage.FilenameSuffixCompact, newer, 0o600); err != nil {
		t.Fatal(err)
	}

	db, keys := openKeys(t, main)
	defer db.Close()
	if !keys["songs\x00alpha"] || !keys["songs\x00beta"] {
		t.Fatalf("keys after interrupted compaction = %v, want both", keys)
	}
	if _, err := os.Stat(main); err != nil {
		t.Fatalf("main file not restored: %v", err)
	}
	if _, err := os.Stat(main + storage.FilenameSuffixBackup); !os.IsNotExist(err) {
		t.Fatal("backup not cleaned up after completing compaction")
	}

	// Crash with only the backup on disk: roll back.
	main2 := filepath.Join(dir, "db2")
	if err := os.WriteFile(main2+storage.FilenameSuffixBackup, old, 0o600); err != nil {
		t.Fatal(err)
	}
	db2, keys2 := openKeys(t, main2)
	defer db2.Close()
	if !keys2["songs\x00alpha"] || keys2["songs\x00beta"] {
		t.Fatalf("keys after backup rollback = %v", keys2)
	}
}

// TestCompactIfNeededTriggersOnFragmentation pins the compaction threshold:
// overwriting every key eight times must be recognised as fragmentation and
// trigger a rewrite. Before the fix the estimate was algebraically equal to
// the logical size, so fragmentation was always ~0 and this never ran.
func TestCompactIfNeededTriggersOnFragmentation(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDB(&Config{
		FilePath:                filepath.Join(dir, "frag.db"),
		DisableWAL:              true,
		InitialFileSize:         1 << 20,
		CompactionMinItems:      10,
		CompactionFragmentation: 0.3,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for v := 0; v < 8; v++ {
		for i := 0; i < 20; i++ {
			k := []byte(fmt.Sprintf("k%02d", i))
			if err := db.Set(k, bytes.Repeat([]byte{byte('a' + v)}, 64)); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
	}

	before := db.storage.Load()
	if err := db.CompactIfNeeded(); err != nil {
		t.Fatalf("compact if needed: %v", err)
	}
	after := db.storage.Load()
	if after == before {
		t.Fatal("fragmented database was not compacted")
	}
	if after.LogicalSize()*2 >= before.LogicalSize() {
		t.Fatalf("compaction did not reclaim space: logical %d -> %d", before.LogicalSize(), after.LogicalSize())
	}
	if v, err := db.Get([]byte("k00")); err != nil || !bytes.Equal(v, bytes.Repeat([]byte{'h'}, 64)) {
		t.Fatalf("latest value lost after compaction: %v %q", err, v)
	}
}

// TestCompactIfNeededSkipsFreshDB: with no overwrites the sampled live bytes
// equal the logical extent, so the periodic check must not rewrite the file.
func TestCompactIfNeededSkipsFreshDB(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDB(&Config{
		FilePath:                filepath.Join(dir, "fresh.db"),
		DisableWAL:              true,
		InitialFileSize:         1 << 20,
		CompactionMinItems:      10,
		CompactionFragmentation: 0.3,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 20; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k%02d", i)), bytes.Repeat([]byte("v"), 64)); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	before := db.storage.Load()
	if err := db.CompactIfNeeded(); err != nil {
		t.Fatalf("compact if needed: %v", err)
	}
	if db.storage.Load() != before {
		t.Fatal("fresh database was compacted unnecessarily")
	}
}

// TestGetterHookGetsPrivateCopy: Getter may hand the cache's internal slice
// to the callback, but an OnRead hook that mutates its input must still only
// touch a copy. Otherwise a read hook would corrupt the cache for everyone.
func TestGetterHookGetsPrivateCopy(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDB(&Config{
		FilePath:   filepath.Join(dir, "hook.db"),
		DisableWAL: true,
		Hooks: &pipeline.Hooks{OnRead: func(ctx pipeline.HookContext) ([]byte, error) {
			for i := range ctx.Value {
				ctx.Value[i] = 'X'
			}
			return ctx.Value, nil
		}},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("songs\x00secret")
	if err := db.Set(key, []byte("secret")); err != nil {
		t.Fatalf("set: %v", err)
	}

	for i := 0; i < 2; i++ { // miss fills the cache, then hit reads it
		var got string
		if err := db.Getter(key, func(v []byte) error {
			got = string(v)
			return nil
		}); err != nil {
			t.Fatalf("Getter %d: %v", i, err)
		}
		if got != "XXXXXX" {
			t.Fatalf("hooked value = %q, want XXXXXX", got)
		}
		raw, ok := db.cache.Get(key)
		if !ok || string(raw) != "secret" {
			t.Fatalf("cache after hook = %q ok=%v, want secret", raw, ok)
		}
	}
}
