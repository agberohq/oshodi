package vector

import (
	"cmp"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"testing"

	oshodi "github.com/agberohq/oshodi"
)

// item is a keyed vector used by the recall harness.
type item struct {
	key string
	vec Vector
}

// bruteTopK computes the exact k nearest neighbors of q among items using
// dist, as the ground truth for recall measurement.
func bruteTopK(items []item, q Vector, k int, dist DistanceFunc) map[string]bool {
	type scored struct {
		key string
		d   float32
	}
	all := make([]scored, len(items))
	for i, it := range items {
		all[i] = scored{key: it.key, d: dist(it.vec, q)}
	}
	slices.SortFunc(all, func(a, b scored) int {
		return cmp.Compare(a.d, b.d)
	})
	out := make(map[string]bool, k)
	for _, s := range all[:k] {
		out[s.key] = true
	}
	return out
}

func randomVectors(rng *rand.Rand, n, d int) []Vector {
	vecs := make([]Vector, n)
	for i := range vecs {
		v := make(Vector, d)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vecs[i] = v
	}
	return vecs
}

// buildRecallFixture builds a deterministic index of n random d-dimensional
// vectors plus a query set, using the index's own distance function. ef is
// the exploration budget used both for construction and for queries.
func buildRecallFixture(tb testing.TB, n, d, nQueries, ef int) (*Index, []item, []Vector) {
	tb.Helper()

	dataRng := rand.New(rand.NewSource(7))
	data := randomVectors(dataRng, n, d)
	items := make([]item, n)
	for i, v := range data {
		items[i] = item{key: fmt.Sprintf("v%05d", i), vec: v}
	}
	queries := randomVectors(rand.New(rand.NewSource(11)), nQueries, d)

	dir := tb.TempDir()
	db, err := oshodi.Open(filepath.Join(dir, "recall.db"))
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	tb.Cleanup(func() { db.Close() })

	idx, err := Open(db)
	if err != nil {
		tb.Fatalf("open index: %v", err)
	}
	tb.Cleanup(func() { idx.Close() })

	// Fixed seeds keep construction deterministic across runs.
	idx.Rng = rand.New(rand.NewSource(42))
	idx.EfSearch = ef

	for _, it := range items {
		if err := idx.Add(it.key, it.vec); err != nil {
			tb.Fatalf("add %s: %v", it.key, err)
		}
	}
	if idx.Len() != n {
		tb.Fatalf("len: expected %d, got %d", n, idx.Len())
	}
	return idx, items, queries
}

// TestSearchRecallQuality measures recall@k against a brute-force baseline
// using the index's own distance function, guarding against navigation
// regressions. Thresholds leave headroom because entry-point selection is
// map-order nondeterministic.
func TestSearchRecallQuality(t *testing.T) {
	const (
		n        = 2000
		d        = 64
		nQueries = 50
		k        = 10
	)

	for _, tc := range []struct {
		ef        int
		minRecall float64
	}{
		{ef: 20, minRecall: 0.5},  // default EfSearch
		{ef: 64, minRecall: 0.85}, // healthy budget; measured ~0.93
	} {
		t.Run(fmt.Sprintf("ef%d", tc.ef), func(t *testing.T) {
			idx, items, queries := buildRecallFixture(t, n, d, nQueries, tc.ef)

			hits := 0
			for _, q := range queries {
				exact := bruteTopK(items, q, k, idx.Distance)
				got, err := idx.Search(q, k)
				if err != nil {
					t.Fatalf("search: %v", err)
				}
				gotSet := make(map[string]bool, len(got))
				for _, g := range got {
					gotSet[g] = true
				}
				for key := range exact {
					if gotSet[key] {
						hits++
					}
				}
			}
			recall := float64(hits) / float64(nQueries*k)
			t.Logf("recall@%d: %.3f (n=%d, d=%d, queries=%d, M=%d, EfSearch=%d)",
				k, recall, n, d, nQueries, idx.M, idx.EfSearch)
			if recall < tc.minRecall {
				t.Errorf("recall@%d at EfSearch=%d unexpectedly low: %.3f (min %.3f)",
					k, tc.ef, recall, tc.minRecall)
			}
		})
	}
}

func BenchmarkVectorSearch(b *testing.B) {
	const (
		n        = 2000
		d        = 64
		nQueries = 50
		k        = 10
	)
	idx, _, queries := buildRecallFixture(b, n, d, nQueries, 64)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := idx.Search(queries[i%len(queries)], k); err != nil {
			b.Fatal(err)
		}
	}
}
