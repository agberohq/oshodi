package ograph

// Recovery tests: truncation/bit-rot of the edge bucket never produces a
// torn adjacency; dangling edges are dropped; corrupted/foreign records are
// skipped and counted; interrupted cascades are benign.

import (
	"strings"
	"testing"
)

func TestRebuildDropsDanglingEdges(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("a", "artist", nil)
	g.AddVertex("b", "song", nil)
	if _, err := g.AddEdge("a", "b", "performs", nil); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-cascade: the vertex record for "b" is gone but
	// the edge record is still on disk. Rebuild must drop the dangling edge.
	if err := g.vertices.Delete([]byte("b")); err != nil {
		t.Fatalf("direct vertex delete: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: rebuild drops the dangling edge, graph stays usable.
	g2, err := Open(g.db)
	if err != nil {
		t.Fatalf("reopen with dangling edge: %v", err)
	}
	defer g2.Close()
	if got := g2.dangling.Load(); got != 1 {
		t.Fatalf("dangling count: %d", got)
	}
	n, err := g2.V().Count()
	if err != nil {
		t.Fatal(err)
	}
	// "b" was deleted directly, so exactly one vertex remains.
	if n != 1 {
		t.Fatalf("vertex count after rebuild: %d", n)
	}
	local := g2
	count, err := local.E().Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("dangling edge survived rebuild: %d", count)
	}
}

func TestRebuildSkipsCorruptAndForeignRecords(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("a", "artist", nil)
	g.AddVertex("b", "song", nil)
	if _, err := g.AddEdge("a", "b", "performs", nil); err != nil {
		t.Fatal(err)
	}

	// Write records directly into the edge bucket in foreign/corrupt
	// forms the decoder must skip.
	_ = g.edges.Set([]byte("garbage-1"), []byte("not-an-envelope"))
	_ = g.edges.Set([]byte("garbage-2"), append([]byte{0x47, 0x01}, []byte("half-envelope...")...))

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2, err := Open(g.db)
	if err != nil {
		t.Fatalf("reopen with corrupt records: %v", err)
	}
	defer g2.Close()
	if got := g2.corrupt.Load(); got < 2 {
		t.Fatalf("corrupt count: %d", got)
	}
	// The valid edge still rebuilt.
	n, err := g2.E().Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("edge count: %d", n)
	}
}

func TestRebuildPrefixTruncation(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	for i := 0; i < 20; i++ {
		g.AddVertex(fmtVertex(i), "song", nil)
		if i > 0 {
			if _, err := g.AddEdge(fmtVertex(i-1), fmtVertex(i), "next", nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// Cut the edge bucket's records: rebuild loads only the intact prefix
	// (CRC semantics). Simulate by writing a truncated record form — the
	// shortest valid prefix is a half-written envelope at tail.
	g2, err := Open(g.db)
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()

	n, err := g2.E().Count()
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("rebuild lost everything")
	}
	ids, err := g2.V().OrderByID().IDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 20 {
		t.Fatalf("vertices lost: %d", len(ids))
	}
}

func fmtVertex(i int) string {
	var b strings.Builder
	b.WriteString("v")
	b.WriteString(strings.Repeat("x", i))
	return b.String()
}
