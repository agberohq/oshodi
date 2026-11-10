package graph

// Model-based concurrency test, Jepsen-inspired: random graph mutations
// across workers are checked against an in-memory model. The core
// invariant is that the adjacency index agrees with the bucket state after
// every quiesce and after reopen.

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

type adjacencyModel struct {
	vertices map[string]string // id → label
	edges    map[string][3]string
}

func newAdjacencyModel() *adjacencyModel {
	return &adjacencyModel{
		vertices: map[string]string{},
		edges:    map[string][3]string{},
	}
}

// modelRebuild derives the adjacency index from bucket state, exactly as
// graph.Open must.
func adjacencyFromModel(m *adjacencyModel) *adjacency {
	adj := newAdjacency()
	for id := range m.edges {
		s := m.edges[id]
		adj.add(s[0], s[1], s[2], id)
	}
	return adj
}

func TestModelConcurrentGraphMutations(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	m := newAdjacencyModel()
	var mu sync.Mutex

	const workers = 6
	const opsEach = 150
	var lost sync.WaitGroup

	vertexNames := make([]string, 0, 30)
	for i := 0; i < 30; i++ {
		id := fmt.Sprintf("v%d", i)
		label := "artist"
		if i%3 == 0 {
			label = "song"
		}
		if err := g.AddVertex(id, label, nil); err != nil {
			t.Fatalf("seed vertex: %v", err)
		}
		m.vertices[id] = label
		vertexNames = append(vertexNames, id)
	}

	for w := 0; w < workers; w++ {
		lost.Add(1)
		go func(id int) {
			defer lost.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < opsEach; i++ {
				a := vertexNames[rng.Intn(len(vertexNames))]
				b := vertexNames[rng.Intn(len(vertexNames))]
				switch rng.Intn(5) {
				case 0, 1:
					eid := fmt.Sprintf("e-%d-%d", id, rng.Int63())
					err := g.AddEdgeID(eid, a, b, "likes", nil)
					mu.Lock()
					switch {
					case err == nil:
						m.edges[eid] = [3]string{a, b, "likes"}
					case errors.Is(err, ErrVertexNotFound), errors.Is(err, ErrEdgeExists):
						// vertex was concurrently removed / id reused: skip
					default:
						mu.Unlock()
						t.Errorf("add edge: %v", err)
						return
					}
					mu.Unlock()
				case 2:
					mu.Lock()
					var candidates []string
					for eid, s := range m.edges {
						if s[0] == a || s[1] == a {
							candidates = append(candidates, eid)
						}
					}
					var victim string
					if len(candidates) > 0 {
						victim = candidates[rng.Intn(len(candidates))]
					}
					var existed bool
					if victim != "" {
						existed = true
						delete(m.edges, victim)
					}
					mu.Unlock()
					if !existed {
						continue
					}
					err := g.RemoveEdge(victim)
					if err != nil && !errors.Is(err, ErrEdgeNotFound) {
						t.Errorf("remove edge %q: %v", victim, err)
						return
					}
				case 3:
					err := g.RemoveVertex(a)
					mu.Lock()
					switch {
					case err == nil || errors.Is(err, ErrVertexNotFound):
						delete(m.vertices, a)
						for eid, s := range m.edges {
							if s[0] == a || s[1] == a {
								delete(m.edges, eid)
							}
						}
					default:
						mu.Unlock()
						t.Errorf("remove vertex %q: %v", a, err)
						return
					}
					mu.Unlock()
					// Recover: re-create the vertex even if a competing
					// worker removed it, so the workload keeps going.
					if err := g.AddVertex(a, "artist", nil); err != nil && !errors.Is(err, ErrGraphClosed) {
						t.Errorf("re-add vertex %q: %v", a, err)
						return
					}
					mu.Lock()
					m.vertices[a] = "artist"
					mu.Unlock()
				case 4:
					eid := fmt.Sprintf("e-fix-%d", rng.Int63())
					label := "likes"
					err := g.AddEdgeID(eid, a, b, label, nil)
					mu.Lock()
					switch {
					case err == nil:
						m.edges[eid] = [3]string{a, b, label}
					case errors.Is(err, ErrVertexNotFound), errors.Is(err, ErrEdgeExists):
						// concurrent vertex removal or id reuse: skip
					default:
						mu.Unlock()
						t.Errorf("add edge fixed id: %v", err)
						return
					}
					mu.Unlock()
				}
			}
		}(w)
	}
	lost.Wait()

	// Quiesced: adjacency index must equal the model-derived adjacency.
	if err := compareAdjacency(g, m); err != nil {
		t.Fatal(err)
	}

	// Reopen: rebuild must reproduce the same index.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2, err := Open(g.db)
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	if err := compareAdjacency(g2, m); err != nil {
		t.Fatalf("reopen rebuild mismatch: %v", err)
	}
}

// adjKey canonicalises one directed edge for exact adjacency comparison.
type adjKey struct {
	from, label, to, id string
}

func flattenAdjacency(root map[string]map[string][]adjEdge, incoming bool) map[adjKey]int {
	out := map[adjKey]int{}
	for a, lm := range root {
		for label, list := range lm {
			for _, e := range list {
				k := adjKey{from: a, label: label, to: e.dst, id: e.id}
				if incoming {
					k.from, k.to = e.dst, a
				}
				out[k]++
			}
		}
	}
	return out
}

// compareAdjacency checks the live adjacency against the model exactly,
// including multiplicities and both directions.
func compareAdjacency(g *Graph, m *adjacencyModel) error {
	g.coord.RLock()
	defer g.coord.RUnlock()

	want := adjacencyFromModel(m)
	for _, dir := range []struct {
		name      string
		want, got map[string]map[string][]adjEdge
		incoming  bool
	}{
		{"out", want.out, g.adj.out, false},
		{"in", want.in, g.adj.in, true},
	} {
		w := flattenAdjacency(dir.want, dir.incoming)
		got := flattenAdjacency(dir.got, dir.incoming)
		if len(w) != len(got) {
			return fmt.Errorf("%s adjacency size %d != %d", dir.name, len(got), len(w))
		}
		for k, n := range w {
			if got[k] != n {
				return fmt.Errorf("%s adjacency mismatch for %+v: got %d want %d", dir.name, k, got[k], n)
			}
		}
		for k := range got {
			if w[k] == 0 {
				return fmt.Errorf("%s adjacency has stale edge %+v", dir.name, k)
			}
		}
	}
	return nil
}
