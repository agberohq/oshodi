package engine

// Jepsen-style adversarial tests: model-checked mixed operations, chaos
// compaction, truncation + bit-rot recovery, and coalescing group-commit
// ordering. Every value carries a [writer][seq][filler] nonce the tests
// decode to detect torn/foreign bytes; an in-memory model decides what the
// state MUST be after quiesce.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agberohq/oshodi/internal/storage"
)

// value domain

func makeValue(writer int, seq uint64, fill int) []byte {
	v := make([]byte, 12+fill)
	binary.LittleEndian.PutUint32(v[0:4], uint32(writer))
	binary.BigEndian.PutUint64(v[4:12], seq)
	for i := 12; i < len(v); i++ {
		v[i] = byte(seq + uint64(i))
	}
	return v
}

func parseValue(v []byte) (writer uint32, seq uint64, ok bool) {
	if len(v) < 12 {
		return 0, 0, false
	}
	writer = binary.LittleEndian.Uint32(v[0:4])
	seq = binary.BigEndian.Uint64(v[4:12])
	for i := 12; i < len(v); i++ {
		if v[i] != byte(seq+uint64(i)) {
			return 0, 0, false
		}
	}
	return writer, seq, true
}

// model

type jmodel struct {
	mu   sync.Mutex
	keys map[string][]byte
}

func newJModel() *jmodel {
	return &jmodel{keys: make(map[string][]byte)}
}

func (m *jmodel) set(key, value []byte) {
	m.mu.Lock()
	m.keys[string(key)] = append([]byte(nil), value...)
	m.mu.Unlock()
}

func (m *jmodel) delete(key []byte) {
	m.mu.Lock()
	delete(m.keys, string(key))
	m.mu.Unlock()
}

func (m *jmodel) snapshot() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]byte, len(m.keys))
	for k, v := range m.keys {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

// assertJState quiesced live state must equal model exactly.
func assertJState(t *testing.T, db *DB, want map[string][]byte) {
	t.Helper()
	got := map[string][]byte{}
	if err := db.Scan(context.Background(), &ScanOptions{}, func(key, value []byte) error {
		got[string(key)] = append([]byte(nil), value...)
		return nil
	}); err != nil {
		t.Fatalf("final scan: %v", err)
	}
	for k, v := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("key %q lost (have %d keys: %v)", k, len(got), keysOf(got))
		}
		if !bytes.Equal(g, v) {
			t.Fatalf("key %q mismatch: got %x want %x", k, g, v)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Fatalf("key %q resurrected", k)
		}
	}
}

// mixed-operation model check

func TestJepsenMixedOps(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDB(&Config{FilePath: dir + "/x.db", MaxKeySize: 64, MaxValueSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const writers = 8
	const opsPer = 400
	m := newJModel()
	var globalSeq uint64

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < opsPer; i++ {
				key := []byte(fmt.Sprintf("k%d-%d", id, rng.Intn(30)))
				switch op := rng.Intn(10); {
				case op < 5:
					seq := atomic.AddUint64(&globalSeq, 1)
					val := makeValue(id, seq, rng.Intn(24))
					if err := db.Set(key, val); err != nil {
						t.Errorf("set: %v", err)
						return
					}
					m.set(key, val)
					// Read-your-writes.
					if got, err := db.Get(key); err != nil || !bytes.Equal(got, val) {
						t.Errorf("post-ack read-your-writes violated: err=%v got=%x want=%x", err, got, val)
						return
					}
				case op < 6:
					if err := db.Delete(key); err != nil {
						t.Errorf("delete: %v", err)
						return
					}
					m.delete(key)
				case op < 7:
					batch := make([]KV, 0, 4)
					for b := 0; b < 4; b++ {
						bkey := []byte(fmt.Sprintf("b%d-%d-%d", id, i, b))
						val := makeValue(id, atomic.AddUint64(&globalSeq, 1), 8)
						batch = append(batch, KV{Key: bkey, Value: val})
						m.set(bkey, val)
					}
					if err := db.BatchSet(batch); err != nil {
						t.Errorf("batchset: %v", err)
						return
					}
				default:
					// Invariant sweep: every observable value must decode.
					if err := db.Scan(context.Background(), &ScanOptions{}, func(key, value []byte) error {
						if _, _, ok := parseValue(value); !ok {
							t.Errorf("torn/foreign value for key %q", key)
							return fmt.Errorf("stop")
						}
						return nil
					}); err != nil {
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	assertJState(t, db, m.snapshot())
}

// chaos under compaction

func TestJepsenCompactChaos(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDB(&Config{
		FilePath:           dir + "/x.db",
		CompactionMinItems: 1,
		CompactionInterval: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stop := make(chan struct{})
	m := newJModel()
	var wg sync.WaitGroup
	var ferr atomic.Value // first error from chaos goroutines

	setErr := func(err error) {
		if err != nil && ferr.Load() == nil {
			ferr.Store(err)
		}
	}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id) + 7))
			for i := 0; i < 800; i++ {
				key := []byte(fmt.Sprintf("c%d-%d", id, rng.Intn(48)))
				val := makeValue(id, uint64(i), 8)
				if rng.Intn(6) == 0 {
					if err := db.Delete(key); err != nil {
						setErr(err)
						return
					}
					m.delete(key)
				} else if err := db.Set(key, val); err != nil {
					setErr(err)
					return
				} else {
					m.set(key, val)
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; n < 6; n++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := db.Compact(); err != nil {
				select {
				case <-stop:
					return
				default:
				}
				setErr(err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; n < 30; n++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := db.CompactIfNeeded(); err != nil {
				select {
				case <-stop:
					return
				default:
				}
				setErr(err)
				return
			}
			_ = db.Scan(context.Background(), &ScanOptions{}, func(key, value []byte) error {
				if _, _, ok := parseValue(value); !ok {
					t.Errorf("chaos reader: torn value for key %q", key)
					return fmt.Errorf("stop")
				}
				return nil
			})
		}
	}()

	// Writers finish quickly; close the chaos loops.
	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()

	if e := ferr.Load(); e != nil {
		t.Fatalf("chaos error: %v", e)
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Compact(); err != nil {
		t.Fatalf("final compact: %v", err)
	}
	assertJState(t, db, m.snapshot())
}

// main-file truncation + bit-rot recovery

// jepsenPrefix walks record boundaries of a closed database file and
// returns the derived state after each record, plus per-record offsets.
type jprefix struct {
	offset int64
	keys   map[string][]byte // final per-key value; tombstones delete
}

func buildJPrefixes(t *testing.T, path string) []jprefix {
	t.Helper()
	store, err := storage.NewFile(&storage.FileConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var out []jprefix
	current := map[string][]byte{}
	out = append(out, jprefix{offset: 0, keys: cloneMap(current)})
	var off int64
	for {
		key, value, tomb, n, err := store.ReadRecordAt(off)
		if err != nil || n == 0 {
			break
		}
		if tomb && len(key) == 0 {
			// First padding sentinel: writes are append-only, nothing
			// legal follows padding.
			break
		}
		sh := out[len(out)-1]
		next := cloneMap(sh.keys)
		if tomb {
			delete(next, string(key))
		} else {
			next[string(key)] = append([]byte(nil), value...)
		}
		off += n
		out = append(out, jprefix{offset: off, keys: next})
	}
	return out
}

func cloneMap(src map[string][]byte) map[string][]byte {
	dst := make(map[string][]byte, len(src))
	for k, v := range src {
		dst[k] = append([]byte(nil), v...)
	}
	return dst
}

func TestJepsenMainFileRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/rec.db"

	db, err := NewDB(&Config{FilePath: dbPath, DisableWAL: true})
	if err != nil {
		t.Fatal(err)
	}

	const keys = 6
	const rounds = 30
	for r := 1; r <= rounds; r++ {
		for k := 0; k < keys; k++ {
			key := []byte(fmt.Sprintf("rk%d", k))
			val := makeValue(k, uint64(r), 12)
			if err := db.Set(key, val); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	base := readFileBytes(t, dbPath)
	prefixes := buildJPrefixes(t, dbPath)
	if len(prefixes) < 10 {
		t.Fatalf("expected many records, got %d", len(prefixes))
	}

	// Truncation at every record boundary: reopen must yield exactly the
	// prefix state (deterministic loader trim).
	for _, st := range prefixes[1:] {
		if err := osWriteBytes(dbPath, base[:st.offset]); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		db2, err := NewDB(&Config{FilePath: dbPath, DisableWAL: true})
		if err != nil {
			t.Fatalf("reopen at offset %d: %v", st.offset, err)
		}
		assertJState(t, db2, st.keys)
		db2.Close()
	}

	// Bit-rot: corrupt one byte inside record j (offset prefixes[j]); the
	// loader must trim there, so every recovered key/value must be a
	// member of the prefix state BEFORE the corrupt record. No torn
	// values, no resurrected foreign keys.
	raw := readFileBytes(t, dbPath)
	for j := 1; j < len(prefixes)-1 && j < 12; j++ {
		corruptAt := prefixes[j].offset + 5
		bad := append([]byte(nil), raw...)
		bad[corruptAt] ^= 0xFF
		if err := osWriteBytes(dbPath, bad); err != nil {
			t.Fatal(err)
		}
		db3, err := NewDB(&Config{FilePath: dbPath, DisableWAL: true})
		if err != nil {
			t.Fatalf("bitrot reopen at %d: %v", corruptAt, err)
		}
		err = db3.Scan(context.Background(), &ScanOptions{}, func(key, value []byte) error {
			prev := prefixes[j]
			v, ok := prev.keys[string(key)]
			if !ok {
				t.Errorf("bitrot@%d: key %q not in prefix state", corruptAt, key)
				return fmt.Errorf("stop")
			}
			if !bytes.Equal(v, value) {
				t.Errorf("bitrot@%d: key %q value drifted", corruptAt, key)
				return fmt.Errorf("stop")
			}
			return nil
		})
		db3.Close()
		if err != nil {
			t.Fatalf("bitrot@%d scan: %v", corruptAt, err)
		}
	}
}

// coalescing group-commit ordering

func TestJepsenCoalescedGroupCommit(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDB(&Config{
		FilePath:              dir + "/x.db",
		EnableWriteCoalescing: true,
		BatchMinSize:          8,
		BatchMaxSize:          64,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	m := newJModel()

	// Contended single key: whoever completes last (global completion
	// order) must win the final value — completed writes that commit
	// later always carry a higher offset (monotonic CAS).
	const contenders = 32
	const writesEach = 20
	var wg sync.WaitGroup
	// Group commit does not preserve ack order, so "last ack wins" is not a
	// valid expectation. Check that the survivor is one of the written values,
	// then verify exact last-write-wins with serialized writes.
	var writtenMu sync.Mutex
	written := make(map[string]struct{}, contenders*writesEach)
	for w := 0; w < contenders; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesEach; i++ {
				val := makeValue(id, uint64(id*writesEach+i), 16)
				if err := db.Set([]byte("hot"), val); err != nil {
					t.Errorf("coalesced set: %v", err)
					return
				}
				writtenMu.Lock()
				written[string(val)] = struct{}{}
				writtenMu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	preFlush, err := db.Get([]byte("hot"))
	if err != nil {
		t.Fatalf("hot read before flush: %v", err)
	}
	if _, ok := written[string(preFlush)]; !ok {
		t.Fatalf("surviving value %x was never written", preFlush)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	postCompact, err := db.Get([]byte("hot"))
	if err != nil {
		t.Fatalf("hot read after compaction: %v", err)
	}
	if string(postCompact) != string(preFlush) {
		t.Fatalf("compaction changed the surviving value: %x -> %x", preFlush, postCompact)
	}

	// Serialized writes are exactly last-write-wins; keep the model in sync for
	// the comparison at the end.
	for i := 0; i < 8; i++ {
		val := makeValue(99, uint64(i), 16)
		if err := db.Set([]byte("hot"), val); err != nil {
			t.Fatalf("serialized set: %v", err)
		}
		m.set([]byte("hot"), val)
	}

	// Multi-key coalescing: the leader path must not drop or duplicate.
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("multi-%d-%d", id, i%17))
				val := makeValue(id, uint64(i), 8)
				if err := db.Set(key, val); err != nil {
					t.Errorf("multi-key coalesced set: %v", err)
					return
				}
				m.set(key, val)
			}
		}(w)
	}
	wg.Wait()

	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	assertJState(t, db, m.snapshot())
}

// single-process guard

func TestJepsenProcessLock(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/lock.db"

	db, err := NewDB(&Config{FilePath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := storage.AcquireFileLock(dbPath); err == nil {
		t.Fatal("second process lock allowed on an open database")
	}
}

var dbgTesting = true

// shared helpers

func readFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func osWriteBytes(path string, data []byte) error {
	return os.WriteFile(path, data, 0600)
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	if len(out) > 8 {
		out = out[:8]
	}
	return out
}
