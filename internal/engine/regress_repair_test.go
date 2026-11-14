package engine

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agberohq/oshodi/internal/consts"
	"github.com/agberohq/oshodi/internal/pipeline"
	"github.com/agberohq/oshodi/internal/storage"
)

func coalescingConfig(path string) *Config {
	return &Config{
		FilePath:              path,
		EnableWriteCoalescing: true,
		BatchMinSize:          8,
		BatchMaxSize:          16,
		BatchTimeout:          50 * time.Millisecond,
	}
}

// A delete must survive a Set whose index update is still in flight: the
// tombstone is recorded as a watermark that refuses older offsets.
func TestIndexDeleteWatermarkBlocksStaleSet(t *testing.T) {
	idx := NewIndex(16, 0.01)

	idx.DeleteIfStale([]byte("k"), 101)
	if _, ok := idx.Get([]byte("k")); ok {
		t.Fatal("deleted key present in index")
	}

	// The delete committed; the Set's record was written before it but its
	// index update has not landed yet.
	idx.SetIfNewer([]byte("k"), 100)
	if _, ok := idx.Get([]byte("k")); ok {
		t.Fatal("stale Set resurrected a deleted key")
	}

	// A genuinely newer write wins.
	idx.SetIfNewer([]byte("k"), 200)
	if off, ok := idx.Get([]byte("k")); !ok || off != 200 {
		t.Fatalf("newer Set should win: got %d, %v", off, ok)
	}
}

// The coalescer leader is the caller whose Set must report the outcome; a
// failed batch write must surface as its error, not nil.
func TestCoalescedSetReportsWriteError(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "c.db")
	db, err := NewDB(coalescingConfig(tmp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.storage.Load().Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Set([]byte("k"), []byte("v")); err == nil {
		t.Fatal("Set returned nil although the batch write failed")
	}
}

// Coalesced writes must invalidate ordered scan snapshots like every other
// write path, or the key is invisible to ScanPrefix forever.
func TestCoalescedSetVisibleToOrderedScan(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "c.db")
	db, err := NewDB(coalescingConfig(tmp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b, err := db.GetBucket("b")
	if err != nil {
		t.Fatal(err)
	}
	// Prime the ordered snapshot so visibility needs an invalidation, not a
	// fresh build.
	if err := b.ScanPrefix([]byte(""), func(key, value []byte) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	found := false
	if err := b.ScanPrefix([]byte(""), func(key, value []byte) bool {
		if bytes.HasSuffix(key, []byte("k")) {
			found = true
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("coalesced Set never became visible to the ordered scan")
	}
}

// A solo writer must not wait out the whole batch window for followers that
// never arrive.
func TestCoalescedSetSkipsBatchWindow(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "c.db")
	cfg := coalescingConfig(tmp)
	db, err := NewDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	start := time.Now()
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d >= cfg.BatchTimeout {
		t.Fatalf("solo Set waited out the batch window: %v", d)
	}
}

// A failed NewDB must not leak the process-wide flock, or every retry on the
// same path fails with ErrFileBusy until the process exits.
func TestNewDBFailureReleasesFileLock(t *testing.T) {
	dir := t.TempDir()
	// A directory at the database path makes storage.NewFile fail after the
	// lock has been acquired.
	path := filepath.Join(dir, "occupied")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := NewDB(&Config{FilePath: path}); err == nil {
		t.Fatal("expected NewDB to fail on a directory path")
	}

	_, err := NewDB(&Config{FilePath: path})
	if err == nil {
		t.Fatal("expected second attempt to fail too")
	}
	if errors.Is(err, storage.ErrFileBusy) {
		t.Fatalf("file lock leaked by the failed NewDB: %v", err)
	}
}

// BatchSet must run the same OnWrite hook as Set, or untransformed values
// land next to transformed ones.
func TestBatchSetAppliesWriteHook(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "h.db")
	db, err := NewDB(&Config{
		FilePath: tmp,
		Hooks: &pipeline.Hooks{OnWrite: func(ctx pipeline.HookContext) ([]byte, error) {
			return append(append([]byte(nil), ctx.Value...), '!'), nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("a"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get([]byte("a")); err != nil || !bytes.Equal(v, []byte("v!")) {
		t.Fatalf("Set hook: got %q, %v", v, err)
	}

	if err := db.BatchSet([]KV{{Key: []byte("b"), Value: []byte("v")}}); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get([]byte("b")); err != nil || !bytes.Equal(v, []byte("v!")) {
		t.Fatalf("BatchSet hook: got %q, %v", v, err)
	}
}

// An oversized encoding buffer must not be recycled into the pool, or a few
// large records poison it for every shard.
func TestWriterBufferPoolNotPoisoned(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "p.db")
	db, err := NewDB(&Config{FilePath: tmp})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	big := bytes.Repeat([]byte{7}, 1<<20)
	if err := db.Set([]byte("big"), big); err != nil {
		t.Fatal(err)
	}

	for i, sh := range db.writer.shards {
		if bp, ok := sh.bufferPool.Get().(*[]byte); ok && bp != nil && *bp != nil {
			if cap(*bp) > consts.WriterRecordBuffer*4 {
				t.Fatalf("shard %d pool holds a %d-byte buffer", i, cap(*bp))
			}
		}
	}
}

// Compaction under concurrent deletes must neither resurrect the deleted
// keys nor lose the live ones.
func TestCompactUnderConcurrentDeletes(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "cc.db")
	db, err := NewDB(&Config{FilePath: tmp})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for round := 0; round < 10; round++ {
		live := make([]string, 0, 20)
		for i := 0; i < 20; i++ {
			live = append(live, liveKey(round, i))
			if err := db.Set([]byte(liveKey(round, i)), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		dead := make([]string, 0, 20)
		for i := 0; i < 20; i++ {
			dead = append(dead, deadKey(round, i))
			if err := db.Set([]byte(deadKey(round, i)), []byte("v")); err != nil {
				t.Fatal(err)
			}
			if err := db.Delete([]byte(deadKey(round, i))); err != nil {
				t.Fatal(err)
			}
		}

		written := make(chan struct{})
		go func() {
			defer close(written)
			for i := 0; i < 50; i++ {
				if err := db.Set([]byte(writeKey(round, i)), []byte("v")); err != nil {
					t.Errorf("concurrent write: %v", err)
					return
				}
			}
		}()

		if err := db.Compact(); err != nil {
			t.Fatalf("round %d: compact: %v", round, err)
		}
		<-written

		for _, k := range live {
			if _, err := db.Get([]byte(k)); err != nil {
				t.Fatalf("round %d: live key %s lost: %v", round, k, err)
			}
		}
		for _, k := range dead {
			if _, err := db.Get([]byte(k)); err == nil {
				t.Fatalf("round %d: deleted key %s resurrected", round, k)
			}
		}
		for i := 0; i < 50; i++ {
			if _, err := db.Get([]byte(writeKey(round, i))); err != nil {
				t.Fatalf("round %d: concurrent write %s lost: %v", round, writeKey(round, i), err)
			}
		}
	}
}

func liveKey(round, i int) string  { return keyOf("live", round, i) }
func deadKey(round, i int) string  { return keyOf("dead", round, i) }
func writeKey(round, i int) string { return keyOf("w", round, i) }

func keyOf(kind string, round, i int) string {
	return kind + ":" + string(rune('0'+round)) + ":" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
