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

// compareAdjacency checks the live adjacency against the model exactly.
func compareAdjacency(g *Graph, m *adjacencyModel) error {
	g.coord.RLock()
	defer g.coord.RUnlock()

	want := adjacencyFromModel(m)

	for src, lm := range want.out {
		for label, targets := range lm {
			for dst, ids := range targets {
				got, ok := g.adj.out[src]
				if !ok {
					return fmt.Errorf("missing out index for src %q", src)
				}
				t, ok := got[label]
				if !ok {
					return fmt.Errorf("missing out label %q for src %q", label, src)
				}
				for eid := range ids {
					if _, ok := t[dst]; !ok {
						return fmt.Errorf("missing out target %q for src %q", dst, src)
					}
					if _, ok := t[dst]; !ok {
						return fmt.Errorf("edge %q missing", eid)
					}
				}
			}
		}
	}
	for src, lm := range g.adj.out {
		for label, targets := range lm {
			_ = label
			for dst := range targets {
				for id := range targets[dst] {
					s, ok := m.edges[id]
					if !ok {
						return fmt.Errorf("stale adjacency edge %q", id)
					}
					if s[0] != src || s[1] != dst || s[2] != label {
						return fmt.Errorf("adjacency edge %q wrong identity", id)
					}
				}
			}
		}
	}
	for id, s := range m.edges {
		lv, ok := g.adj.out[s[0]]
		if !ok {
			return fmt.Errorf("model edge %q missing from out adjacency", id)
		}
		if _, ok := lv[s[2]][s[1]]; !ok {
			return fmt.Errorf("model edge %q missing in adjacency out", id)
		}
		if _, ok := g.adj.in[s[1]][s[2]][s[0]]; !ok {
			return fmt.Errorf("model edge %q missing in adjacency in", id)
		}
	}
	return nil
}
