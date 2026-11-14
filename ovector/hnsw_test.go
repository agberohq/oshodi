package ovector

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	oshodi "github.com/agberohq/oshodi"
)

func testDB(t *testing.T) *oshodi.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := oshodi.Open(filepath.Join(dir, "vector.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestOpenClose(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if idx.Len() != 0 {
		t.Errorf("empty index: expected 0, got %d", idx.Len())
	}

	if err := idx.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Double close should fail
	if err := idx.Close(); err != ErrVectorClosed {
		t.Errorf("double close: expected ErrVectorClosed, got %v", err)
	}
}

func TestOpenAlreadyOpen(t *testing.T) {
	db := testDB(t)
	idx1, err := Open(db)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}

	idx2, err := Open(db)
	if err != ErrVectorAlreadyOpen {
		t.Fatalf("second open: expected ErrVectorAlreadyOpen, got %v", err)
	}
	if idx2 != nil {
		t.Fatal("expected nil index")
	}

	idx1.Close()

	// Should work after close
	idx3, err := Open(db)
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	idx3.Close()
}

func TestAddAndLen(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	vec := []float32{0.1, 0.2, 0.3}
	if err := idx.Add("v1", vec); err != nil {
		t.Fatalf("add v1: %v", err)
	}
	if idx.Len() != 1 {
		t.Errorf("len after add: expected 1, got %d", idx.Len())
	}

	if err := idx.Add("v2", []float32{0.4, 0.5, 0.6}); err != nil {
		t.Fatalf("add v2: %v", err)
	}
	if idx.Len() != 2 {
		t.Errorf("len after add: expected 2, got %d", idx.Len())
	}
}

func TestAddEmptyKey(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	err = idx.Add("", []float32{1, 2, 3})
	if err != ErrInvalidIdentity {
		t.Errorf("empty key: expected ErrInvalidIdentity, got %v", err)
	}
}

func TestAddDimMismatch(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if err := idx.Add("v1", []float32{1, 2, 3}); err != nil {
		t.Fatalf("add v1: %v", err)
	}

	err = idx.Add("v2", []float32{1, 2}) // 2 dims vs 3
	if err != ErrDimMismatch {
		t.Errorf("dim mismatch: expected ErrDimMismatch, got %v", err)
	}
}

func TestDims(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if idx.Dims() != 0 {
		t.Errorf("empty dims: expected 0, got %d", idx.Dims())
	}

	if err := idx.Add("v1", []float32{1, 2, 3, 4, 5}); err != nil {
		t.Fatalf("add: %v", err)
	}

	if idx.Dims() != 5 {
		t.Errorf("dims: expected 5, got %d", idx.Dims())
	}
}

func TestLookup(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	original := []float32{0.1, 0.2, 0.3}
	if err := idx.Add("v1", original); err != nil {
		t.Fatalf("add: %v", err)
	}

	fetched, err := idx.Lookup("v1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}

	if len(fetched) != len(original) {
		t.Fatalf("length mismatch: expected %d, got %d", len(original), len(fetched))
	}
	for i := range original {
		if fetched[i] != original[i] {
			t.Errorf("index %d: expected %f, got %f", i, original[i], fetched[i])
		}
	}
}

func TestLookupNotFound(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	_, err = idx.Lookup("missing")
	if err != ErrVectorNotFound {
		t.Errorf("lookup missing: expected ErrVectorNotFound, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if err := idx.Add("v1", []float32{1, 2, 3}); err != nil {
		t.Fatalf("add: %v", err)
	}

	if err := idx.Delete("v1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if idx.Len() != 0 {
		t.Errorf("len after delete: expected 0, got %d", idx.Len())
	}

	_, err = idx.Lookup("v1")
	if err != ErrVectorNotFound {
		t.Errorf("lookup after delete: expected ErrVectorNotFound, got %v", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	err = idx.Delete("missing")
	if err != ErrVectorNotFound {
		t.Errorf("delete missing: expected ErrVectorNotFound, got %v", err)
	}
}

func TestSearch(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	// Add vectors with known directions
	vectors := map[string][]float32{
		"north": {1, 0, 0},
		"south": {-1, 0, 0},
		"east":  {0, 1, 0},
		"west":  {0, -1, 0},
	}

	for key, vec := range vectors {
		if err := idx.Add(key, vec); err != nil {
			t.Fatalf("add %s: %v", key, err)
		}
	}

	// Search for something close to north
	query := []float32{0.9, 0.1, 0.0}
	results, err := idx.Search(query, 2)
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("search returned no results")
	}

	// First result should be "north" (closest)
	if results[0] != "north" {
		t.Errorf("search: expected north first, got %s", results[0])
	}
}

func TestSearchEmpty(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	results, err := idx.Search([]float32{1, 2, 3}, 5)
	if err != nil {
		t.Fatalf("search empty: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("search empty: expected 0 results, got %d", len(results))
	}
}

func TestSearchDimMismatch(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if err := idx.Add("v1", []float32{1, 2, 3}); err != nil {
		t.Fatalf("add: %v", err)
	}

	_, err = idx.Search([]float32{1, 2}, 1) // 2 dims vs 3
	if err != ErrDimMismatch {
		t.Errorf("search dim mismatch: expected ErrDimMismatch, got %v", err)
	}
}

func TestReplace(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if err := idx.Add("v1", []float32{1, 0, 0}); err != nil {
		t.Fatalf("add v1: %v", err)
	}

	// Replace with a different vector
	if err := idx.Add("v1", []float32{0, 1, 0}); err != nil {
		t.Fatalf("replace v1: %v", err)
	}

	if idx.Len() != 1 {
		t.Errorf("len after replace: expected 1, got %d", idx.Len())
	}

	fetched, err := idx.Lookup("v1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}

	if fetched[0] != 0 || fetched[1] != 1 || fetched[2] != 0 {
		t.Errorf("replace: expected [0,1,0], got %v", fetched)
	}
}

func TestPersistenceRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vector.db")

	// First session: add vectors
	db1, err := oshodi.Open(dbPath)
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}

	idx1, err := Open(db1)
	if err != nil {
		t.Fatalf("open idx1: %v", err)
	}

	vectors := map[string][]float32{
		"a": {1, 0, 0},
		"b": {0, 1, 0},
		"c": {0, 0, 1},
	}

	for key, vec := range vectors {
		if err := idx1.Add(key, vec); err != nil {
			t.Fatalf("add %s: %v", key, err)
		}
	}

	if err := idx1.Close(); err != nil {
		t.Fatalf("close idx1: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	// Second session: reopen and verify
	db2, err := oshodi.Open(dbPath)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()

	idx2, err := Open(db2)
	if err != nil {
		t.Fatalf("open idx2: %v", err)
	}
	defer idx2.Close()

	if idx2.Len() != 3 {
		t.Errorf("len after recovery: expected 3, got %d", idx2.Len())
	}

	// Verify each vector
	for key, expected := range vectors {
		fetched, err := idx2.Lookup(key)
		if err != nil {
			t.Fatalf("lookup %s: %v", key, err)
		}

		if len(fetched) != len(expected) {
			t.Fatalf("%s: length mismatch", key)
		}
		for i := range expected {
			if fetched[i] != expected[i] {
				t.Errorf("%s[%d]: expected %f, got %f", key, i, expected[i], fetched[i])
			}
		}
	}

	// Search should still work
	query := []float32{0.9, 0.1, 0.0}
	results, err := idx2.Search(query, 1)
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if len(results) == 0 || results[0] != "a" {
		t.Errorf("search after recovery: expected 'a' first, got %v", results)
	}
}

func TestSearchKGreaterThanEfSearch(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	// A tiny exploration budget must not silently cap the result count.
	idx.EfSearch = 4

	for i := 0; i < 50; i++ {
		vec := []float32{float32(i), float32(i*2 + 1), float32(i*3 + 1)}
		key := fmt.Sprintf("vec_%03d", i)
		if err := idx.Add(key, vec); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}

	results, err := idx.Search([]float32{5, 11, 16}, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("k > EfSearch: expected 10 results, got %d", len(results))
	}
}

func TestSearchNonPositiveK(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if err := idx.Add("v1", []float32{1, 2, 3}); err != nil {
		t.Fatalf("add: %v", err)
	}

	results, err := idx.Search([]float32{1, 2, 3}, 0)
	if err != nil {
		t.Fatalf("search k=0: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("search k=0: expected no results, got %v", results)
	}

	results, err = idx.Search([]float32{1, 2, 3}, -1)
	if err != nil {
		t.Fatalf("search k=-1: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("search k=-1: expected no results, got %v", results)
	}
}

func TestConcurrentOperations(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				key := fmt.Sprintf("w%d_%03d", w, i)
				if err := idx.Add(key, []float32{float32(i), 1, 0}); err != nil {
					t.Errorf("add: %v", err)
					return
				}
				if _, err := idx.Search([]float32{1, 0, 0}, 3); err != nil {
					t.Errorf("search: %v", err)
					return
				}
				if _, err := idx.Lookup(key); err != nil {
					t.Errorf("lookup: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if idx.Len() != 100 {
		t.Errorf("len after concurrent adds: expected 100, got %d", idx.Len())
	}
}

func TestCloseDuringOperations(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := idx.Add(fmt.Sprintf("v%02d", i), []float32{float32(i), 1, 0}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, err := idx.Search([]float32{1, 0, 0}, 2)
				if err == ErrVectorClosed {
					return
				}
				if err != nil {
					t.Errorf("search: %v", err)
					return
				}
			}
		}()
	}

	// Close while searches are in flight; must not panic.
	if err := idx.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	wg.Wait()
}

func TestRebuildDimMismatchCounted(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vector.db")

	db1, err := oshodi.Open(dbPath)
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}

	idx1, err := Open(db1)
	if err != nil {
		t.Fatalf("open idx1: %v", err)
	}

	// Seed a mixed-dim record directly into the bucket.
	if err := idx1.Add("a", []float32{1, 0, 0}); err != nil {
		t.Fatalf("add a: %v", err)
	}
	if err := idx1.Close(); err != nil {
		t.Fatalf("close idx1: %v", err)
	}
	if err := db1.Bucket(vectorBucket).Set([]byte("b"), encodeVectorRecord([]float32{1, 0})); err != nil {
		t.Fatalf("seed mismatched record: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	db2, err := oshodi.Open(dbPath)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()

	idx3, err := Open(db2)
	if err != nil {
		t.Fatalf("open idx3: %v", err)
	}
	defer idx3.Close()

	if idx3.Len() != 1 {
		t.Errorf("len after rebuild: expected 1, got %d", idx3.Len())
	}
	stats := idx3.Stats()
	if stats["dim_mismatches"] != 1 {
		t.Errorf("dim_mismatches: expected 1, got %d", stats["dim_mismatches"])
	}
}

func TestStats(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	stats := idx.Stats()
	if stats["vectors"] != 0 {
		t.Errorf("empty stats: expected 0 vectors, got %d", stats["vectors"])
	}

	if err := idx.Add("v1", []float32{1, 2, 3}); err != nil {
		t.Fatalf("add: %v", err)
	}

	stats = idx.Stats()
	if stats["vectors"] != 1 {
		t.Errorf("stats after add: expected 1 vector, got %d", stats["vectors"])
	}
}

func TestClosedOperations(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}

	if err := idx.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Operations on closed index should fail
	if err := idx.Add("v1", []float32{1, 2, 3}); err != ErrVectorClosed {
		t.Errorf("add on closed: expected ErrVectorClosed, got %v", err)
	}

	if _, err := idx.Search([]float32{1, 2, 3}, 1); err != ErrVectorClosed {
		t.Errorf("search on closed: expected ErrVectorClosed, got %v", err)
	}

	if err := idx.Delete("v1"); err != ErrVectorClosed {
		t.Errorf("delete on closed: expected ErrVectorClosed, got %v", err)
	}

	if _, err := idx.Lookup("v1"); err != ErrVectorClosed {
		t.Errorf("lookup on closed: expected ErrVectorClosed, got %v", err)
	}
}

func TestBulkAdd(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	// Add 100 vectors with unique keys
	for i := 0; i < 100; i++ {
		vec := []float32{float32(i), float32(i * 2), float32(i * 3)}
		key := fmt.Sprintf("vec_%03d", i)
		if err := idx.Add(key, vec); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}

	if idx.Len() != 100 {
		t.Errorf("bulk add: expected 100, got %d", idx.Len())
	}
}

func TestSearchMany(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	// Add vectors along axes
	for i := 0; i < 50; i++ {
		vec := make([]float32, 3)
		vec[i%3] = 1.0
		if err := idx.Add(string(rune('A'+i)), vec); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	// Search for something close to X-axis
	query := []float32{0.99, 0.01, 0.01}
	results, err := idx.Search(query, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("search returned no results")
	}
}

func TestEuclideanDistanceFunc(t *testing.T) {
	db := testDB(t)
	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	idx.Distance = EuclideanDistance

	// Add vectors
	if err := idx.Add("near", []float32{1, 1, 1}); err != nil {
		t.Fatalf("add near: %v", err)
	}
	if err := idx.Add("far", []float32{10, 10, 10}); err != nil {
		t.Fatalf("add far: %v", err)
	}

	// Search for something close to (1,1,1)
	query := []float32{1.1, 1.1, 1.1}
	results, err := idx.Search(query, 1)
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	if len(results) == 0 || results[0] != "near" {
		t.Errorf("euclidean search: expected 'near' first, got %v", results)
	}
}

func TestTempDirCleanup(t *testing.T) {
	// Ensure temp dirs are cleaned up properly
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := oshodi.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	idx, err := Open(db)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := idx.Add("v1", []float32{1, 2, 3}); err != nil {
		t.Fatalf("add: %v", err)
	}

	if err := idx.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	// Verify files exist
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	if len(entries) == 0 {
		t.Error("expected files in temp dir")
	}
}
