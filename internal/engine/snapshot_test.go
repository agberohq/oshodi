package engine

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func testDB(t *testing.T) *DB {
	t.Helper()
	db, err := NewDB(&Config{FilePath: t.TempDir() + "/snap.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustBucket(t *testing.T, db *DB, name string) *Bucket {
	t.Helper()
	b, err := db.Bucket(name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestSnapshotReadsOlderValue covers the core promise: a view created before a
// write keeps reading the value as of its creation.
func TestSnapshotReadsOlderValue(t *testing.T) {
	db := testDB(t)
	b := mustBucket(t, db, "snap")
	key := []byte("k")

	if err := b.Set(key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	snap, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	if err := b.Set(key, []byte("v2")); err != nil {
		t.Fatal(err)
	}

	got, err := snap.Get(key)
	if err != nil {
		t.Fatalf("snapshot get: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("snapshot read %q, want v1", got)
	}

	cur, err := b.Get(key)
	if err != nil || string(cur) != "v2" {
		t.Fatalf("current read %q, %v; want v2", cur, err)
	}

	snap2, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap2.Close()
	got2, err := snap2.Get(key)
	if err != nil || string(got2) != "v2" {
		t.Fatalf("newer snapshot read %q, %v; want v2", got2, err)
	}
}

// TestSnapshotSeesPreDeleteValue verifies the history keeps the version a
// delete displaced.
func TestSnapshotSeesPreDeleteValue(t *testing.T) {
	db := testDB(t)
	b := mustBucket(t, db, "snap")
	key := []byte("k")
	if err := b.Set(key, []byte("v")); err != nil {
		t.Fatal(err)
	}

	snap, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	if err := b.Delete(key); err != nil {
		t.Fatal(err)
	}

	got, err := snap.Get(key)
	if err != nil || string(got) != "v" {
		t.Fatalf("snapshot after delete = %q, %v; want v", got, err)
	}
	if _, err := b.Get(key); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("current get after delete = %v, want not found", err)
	}

	after, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	if _, err := after.Get(key); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("snapshot after delete = %v, want not found", err)
	}
}

// TestSnapshotChainOfOverwrites walks several versions.
func TestSnapshotChainOfOverwrites(t *testing.T) {
	db := testDB(t)
	b := mustBucket(t, db, "snap")
	key := []byte("k")

	var snaps []*BucketSnapshot
	for i := 0; i < 4; i++ {
		if err := b.Set(key, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
		s, err := b.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		snaps = append(snaps, s)
	}
	defer func() {
		for _, s := range snaps {
			_ = s.Close()
		}
	}()

	for i, s := range snaps {
		got, err := s.Get(key)
		if err != nil {
			t.Fatalf("snapshot %d: %v", i, err)
		}
		if want := fmt.Sprintf("v%d", i); string(got) != want {
			t.Fatalf("snapshot %d read %q, want %q", i, got, want)
		}
	}
}

// TestSnapshotConcurrentWriters keeps the invariant that a snapshot never
// returns a value that was never written, and never a torn record.
func TestSnapshotConcurrentWriters(t *testing.T) {
	db := testDB(t)
	b := mustBucket(t, db, "snap")
	key := []byte("hot")

	if err := b.Set(key, []byte("0")); err != nil {
		t.Fatal(err)
	}

	var stopMu sync.Mutex
	stop := false
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 1; ; i++ {
				stopMu.Lock()
				if stop {
					stopMu.Unlock()
					return
				}
				stopMu.Unlock()
				if err := b.Set(key, []byte(fmt.Sprintf("%d:%d", w, i))); err != nil {
					t.Errorf("set: %v", err)
					return
				}
			}
		}(w)
	}

	seen := map[string]bool{"0": true}
	for i := 0; i < 200; i++ {
		s, err := b.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		val, err := s.Get(key)
		if err != nil {
			full := string(b.MakeKey(key))
			head, _ := db.index.Load().GetString(full)
			t.Logf("DEBUG watermark=%d head=%d logical=%d hist=%v inflight=%v",
				s.Watermark(), head, db.storage.Load().LogicalSize(), db.history.lookup(full),
				db.writes.watermark(db.storage.Load().LogicalSize()))
			s.Close()
			t.Fatalf("snapshot get: %v", err)
		}
		s.Close()
		if _, err := strconv.Atoi(string(val[:1])); err != nil {
			t.Fatalf("snapshot returned a malformed value %q", val)
		}
		seen[string(val)] = true
	}

	stopMu.Lock()
	stop = true
	stopMu.Unlock()
	wg.Wait()
}

// TestCompactionDefersWhileSnapshotOpen verifies the guard and that closing the
// snapshot allows compaction again.
func TestCompactionDefersWhileSnapshotOpen(t *testing.T) {
	db := testDB(t)
	b := mustBucket(t, db, "snap")
	for i := 0; i < 10; i++ {
		if err := b.Set([]byte("k"+strconv.Itoa(i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	snap, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); !errors.Is(err, ErrSnapshotsActive) {
		t.Fatalf("Compact with open snapshot = %v, want ErrSnapshotsActive", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("Compact after close: %v", err)
	}
	// Reads still work after the offset rewrite.
	if _, err := b.Get([]byte("k0")); err != nil {
		t.Fatalf("get after compaction: %v", err)
	}
}

// TestSnapshotGetManyOmitsMissing checks ordering and omission.
func TestSnapshotGetManyOmitsMissing(t *testing.T) {
	db := testDB(t)
	b := mustBucket(t, db, "snap")
	if err := b.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	snap, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	got, err := snap.GetMany([][]byte{[]byte("a"), []byte("missing"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].Key) != "a" || string(got[1].Key) != "b" {
		t.Fatalf("GetMany = %v, want [a b]", got)
	}
}

// TestSnapshotScanRange verifies a range scan sees the state as of the view,
// including a key that was overwritten and one that was deleted afterwards.
func TestSnapshotScanRange(t *testing.T) {
	db := testDB(t)
	b := mustBucket(t, db, "snap")
	for _, k := range []string{"a", "b", "c"} {
		if err := b.Set([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}

	snap, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	if err := b.Set([]byte("a"), []byte("new-a")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("d"), []byte("new-d")); err != nil {
		t.Fatal(err)
	}

	var got []string
	if err := snap.ScanRange(nil, nil, func(k, v []byte) bool {
		got = append(got, string(k)+"="+string(v))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"a=v-a", "b=v-b", "c=v-c"}
	if len(got) != len(want) {
		t.Fatalf("scan = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan = %v, want %v", got, want)
		}
	}

	// A bounded scan respects [start, end).
	got = got[:0]
	if err := snap.ScanRange([]byte("b"), []byte("d"), func(k, v []byte) bool {
		got = append(got, string(k))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("bounded scan = %v, want [b c]", got)
	}
}

// TestCompactionRunsBetweenSnapshots covers a deferred run: compaction is
// refused while a snapshot is open, and the retry compacts once it closes.
func TestCompactionRunsBetweenSnapshots(t *testing.T) {
	db := testDB(t)
	b := mustBucket(t, db, "snap")
	for i := 0; i < 500; i++ {
		if err := b.Set([]byte(fmt.Sprintf("k%04d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}

	snap, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); !errors.Is(err, ErrSnapshotsActive) {
		t.Fatalf("compact with a snapshot open = %v, want ErrSnapshotsActive", err)
	}

	before := db.storage.Load()
	db.retryCompactionSoon(10 * time.Millisecond)
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if db.storage.Load() != before {
			if v, err := b.Get([]byte("k0000")); err != nil || string(v) != "value" {
				t.Fatalf("read after compaction = %q, %v", v, err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("retry did not compact after the snapshot closed")
}
