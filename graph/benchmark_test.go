package graph

// Benchmarks for adjacency mutation, traversal and algorithms.

import (
	"fmt"
	"testing"
	"time"

	oshodi "github.com/agberohq/oshodi"
)

func benchGraph(b *testing.B, vertices int) (*Graph, *oshodi.DB) {
	b.Helper()
	dir := b.TempDir()
	db, err := oshodi.Open(dir + "/bench.db")
	if err != nil {
		b.Fatal(err)
	}
	g, err := Open(db)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < vertices; i++ {
		if err := g.AddVertex(fmt.Sprintf("v%d", i), "song", nil); err != nil {
			b.Fatal(err)
		}
	}
	return g, db
}

func vertexID(i int) string { return fmt.Sprintf("v%d", i) }

func BenchmarkGraphEdgeInsert(b *testing.B) {
	g, db := benchGraph(b, 200)
	defer func() {
		g.Close()
		db.Close()
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.AddEdge(vertexID(i%200), vertexID((i+1)%200), "rel", nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGraphEdgeRemove(b *testing.B) {
	g, db := benchGraph(b, 200)
	defer func() {
		g.Close()
		db.Close()
	}()
	ids := make([]string, 0, 5000)
	for i := 0; i < 5000; i++ {
		id, err := g.AddEdge(vertexID(i%200), vertexID((i*7)%200), "rel", nil)
		if err != nil {
			b.Fatal(err)
		}
		ids = append(ids, id)
	}
	// Removals consume the fixture, so the measured count is bounded by it
	// and reported per remove instead of per b.N.
	n := len(ids)
	if b.N > 0 && b.N < n {
		n = b.N
	}
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := g.RemoveEdge(ids[i]); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(n), "ns/remove")
}

func BenchmarkTwoHopTraversal(b *testing.B) {
	g, db := benchGraph(b, 200)
	defer func() {
		g.Close()
		db.Close()
	}()
	for i := 0; i < 1000; i++ {
		if _, err := g.AddEdge(vertexID(i%200), vertexID((i+1)%200), "rel", nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.V(vertexID(i % 200)).Out("rel").Out("rel").IDs(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPageRankIteration(b *testing.B) {
	g, db := benchGraph(b, 300)
	defer func() {
		g.Close()
		db.Close()
	}()
	for i := 0; i < 1500; i++ {
		if _, err := g.AddEdge(vertexID(i%300), vertexID((i*7)%300), "rel", nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.PageRank(0.85, 10); err != nil {
			b.Fatal(err)
		}
	}
}
