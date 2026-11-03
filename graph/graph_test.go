package graph

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	oshodi "github.com/agberohq/oshodi"
)

func testDB(t *testing.T) *oshodi.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := oshodi.Open(filepath.Join(dir, "graph.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestAddVertexExplicitLabel(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.AddVertex("artist:drake", "artist", []byte(`{"name":"drake"}`)); err != nil {
		t.Fatalf("add vertex: %v", err)
	}
	el, err := g.Vertex("artist:drake")
	if err != nil {
		t.Fatal(err)
	}
	if el.Kind != VertexElement || el.Label != "artist" || el.ID != "artist:drake" {
		t.Fatalf("vertex mismatch: %+v", el)
	}
	if string(el.Props) != `{"name":"drake"}` {
		t.Fatalf("props mismatch: %s", el.Props)
	}

	// label immutable: same id, different label is a conflict
	if err := g.AddVertex("artist:drake", "song", nil); err == nil {
		t.Fatal("label conflict not detected")
	}
	// same label = props upsert
	if err := g.AddVertex("artist:drake", "artist", []byte(`{"name":"drake-v2"}`)); err != nil {
		t.Fatalf("props upsert: %v", err)
	}
	el, _ = g.Vertex("artist:drake")
	if string(el.Props) != `{"name":"drake-v2"}` {
		t.Fatalf("props upsert failed: %s", el.Props)
	}

	// SetVertex preserves the label
	if err := g.SetVertex("artist:drake", []byte(`{"name":"drake-v3"}`)); err != nil {
		t.Fatal(err)
	}
	el, _ = g.Vertex("artist:drake")
	if el.Label != "artist" || string(el.Props) != `{"name":"drake-v3"}` {
		t.Fatalf("SetVertex semantics: %+v", el)
	}

	if err := g.AddVertex("", "artist", nil); err != ErrInvalidIdentity {
		t.Fatalf("empty id accepted: %v", err)
	}
}

func TestEdgeIdentity(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("a", "artist", nil)
	g.AddVertex("s", "song", nil)

	id, err := g.AddEdge("a", "s", "performs", nil)
	if err != nil {
		t.Fatalf("add edge: %v", err)
	}

	// same id + same identity = props upsert
	if err := g.AddEdgeID(id, "a", "s", "performs", []byte(`{"main":true}`)); err != nil {
		t.Fatalf("props upsert: %v", err)
	}
	el, err := g.Edge(id)
	if err != nil || el.EdgeLabel != "performs" || el.Src != "a" || el.Dst != "s" {
		t.Fatalf("edge mismatch: %+v err=%v", el, err)
	}

	// same id + different identity = ErrEdgeExists (no identity theft)
	if err := g.AddEdgeID(id, "s", "a", "performs", nil); !errors.Is(err, ErrEdgeExists) {
		t.Fatalf("expected ErrEdgeExists, got %v", err)
	}

	// endpoint validation
	if _, err := g.AddEdge("a", "missing", "performs", nil); err == nil {
		t.Fatal("edge to missing vertex accepted")
	}
	if _, err := g.AddEdge("missing", "s", "performs", nil); err == nil {
		t.Fatal("edge from missing vertex accepted")
	}

	// remove
	if err := g.RemoveEdge(id); err != nil {
		t.Fatalf("remove edge: %v", err)
	}
	if err := g.RemoveEdge(id); err != ErrEdgeNotFound {
		t.Fatalf("expected ErrEdgeNotFound, got %v", err)
	}
}

func TestRemoveVertexCascade(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("hub", "artist", nil)
	g.AddVertex("a", "song", nil)
	g.AddVertex("b", "song", nil)
	g.AddEdge("hub", "a", "performs", nil)
	e2, _ := g.AddEdge("b", "hub", "featured", nil)

	if err := g.RemoveVertex("hub"); err != nil {
		t.Fatalf("remove vertex: %v", err)
	}
	if _, err := g.Edge("b-hub-featured"); err == nil {
	}
	if _, err := g.Edge(e2); err != ErrEdgeNotFound {
		t.Fatalf("incident edge survived cascade: %v", err)
	}

	// neighbors survive; hub is gone
	if _, err := g.Vertex("a"); err != nil {
		t.Fatalf("vertex a missing: %v", err)
	}
	if _, err := g.Vertex("b"); err != nil {
		t.Fatalf("vertex b missing: %v", err)
	}
	if _, err := g.Vertex("hub"); err != ErrVertexNotFound {
		t.Fatalf("hub survived: %v", err)
	}

	if err := g.RemoveVertex("hub"); err != ErrVertexNotFound {
		t.Fatalf("double remove allowed: %v", err)
	}
}

func TestEdgesWithNULInIDs(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	v1 := "artist:we\x00ird"
	g.AddVertex(v1, "artist", nil)
	g.AddVertex("song:x", "song", nil)
	id, err := g.AddEdge(v1, "song:x", "per\x00forms", nil)
	if err != nil {
		t.Fatalf("NUL-bearing ids rejected: %v", err)
	}
	el, err := g.Edge(id)
	if err != nil {
		t.Fatal(err)
	}
	if el.Src != v1 || el.EdgeLabel != "per\x00forms" {
		t.Fatalf("NUL ids corrupted: %+v", el)
	}
}

func TestGraphLifecycle(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}

	go g.AddVertex("bg", "song", nil)

	// give the background op a moment, then close
	if err := g.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := g.Close(); err != ErrGraphClosed {
		t.Fatalf("double close: %v", err)
	}
	if err := g.AddVertex("after", "song", nil); err != ErrGraphClosed {
		t.Fatalf("post-close mutation: %v", err)
	}
	if n, err := g.V().Count(); err != ErrGraphClosed {
		t.Fatalf("post-close traversal: %v %d", err, n)
	}
	_ = os.RemoveAll(t.TempDir())
}
