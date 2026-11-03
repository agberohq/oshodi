package graph

// Algorithm contracts from plan v3 §6.

import (
	"fmt"
	"testing"
)

func TestPageRankContract(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// star topology: a → b, a → c, a → d
	g.AddVertex("a", "artist", nil)
	g.AddVertex("b", "song", nil)
	g.AddVertex("c", "song", nil)
	g.AddVertex("d", "song", nil)
	g.AddVertex("isolated", "label", nil)
	g.AddEdge("a", "b", "performs", nil)
	g.AddEdge("a", "c", "performs", nil)
	g.AddEdge("a", "d", "performs", nil)

	pr, err := g.PageRank(0.85, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(pr) != 5 {
		t.Fatalf("every vertex must participate, got %d", len(pr))
	}
	var total float64
	for _, v := range pr {
		total += v
	}
	if total < 0.99 || total > 1.01 {
		t.Fatalf("rank mass not conserved: %f", total)
	}
	// star children receive rank from a; isolated vertex gets the base flow
	if pr["b"] <= pr["isolated"] {
		t.Fatalf("star target should outrank isolated vertex: %f vs %f", pr["b"], pr["isolated"])
	}
}

func TestBFSReturnsDistances(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.AddVertex("a", "artist", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.AddVertex("b", "song", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.AddVertex("c", "genre", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.AddVertex("unreachable", "label", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddEdge("a", "b", "performs", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddEdge("b", "c", "in", nil); err != nil {
		t.Fatal(err)
	}

	dist, err := g.BFS("a", Outgoing)
	if err != nil {
		t.Fatal(err)
	}
	if len(dist) != 3 {
		t.Fatalf("unreachable vertex leaked into BFS: %v", dist)
	}
	if dist["a"] != 0 || dist["b"] != 1 || dist["c"] != 2 {
		t.Fatalf("distance map wrong: %v", dist)
	}

	if _, err := g.BFS("missing", Outgoing); err != ErrVertexNotFound {
		t.Fatalf("BFS on missing vertex: %v", err)
	}
}

func TestKHopInclusive(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	for i := 0; i < 4; i++ {
		if err := g.AddVertex(fmt.Sprintf("v%d", i), "song", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := g.AddEdge(fmt.Sprintf("v%d", i), fmt.Sprintf("v%d", i+1), "next", nil); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := g.KHop("v0", 2, KHopOptions{Direction: Outgoing})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 { // v1, v2 (start excluded, depth ≤ 2)
		t.Fatalf("KHop: %v", ids)
	}

	all, err := g.KHop("v0", 2, KHopOptions{Direction: Outgoing, IncludeStart: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 { // v0, v1, v2
		t.Fatalf("KHop inclusive: %v", all)
	}
}

func TestComponentsWeak(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("a", "artist", nil)
	g.AddVertex("b", "artist", nil)
	g.AddVertex("c", "song", nil)
	g.AddVertex("d", "song", nil)
	g.AddEdge("a", "b", "likes", nil)
	g.AddEdge("d", "c", "likes", nil) // reverse direction still same weak component

	comp, err := g.Components()
	if err != nil {
		t.Fatal(err)
	}
	if len(comp) != 4 {
		t.Fatalf("components size: %v", comp)
	}
	if comp["a"] != comp["b"] {
		t.Fatalf("a/b should share a component: %v", comp)
	}
	if comp["c"] != comp["d"] {
		t.Fatalf("c/d should share a component (weak connectivity ignores direction): %v", comp)
	}
	if comp["a"] == comp["c"] {
		t.Fatalf("separate components merged: %v", comp)
	}
}
