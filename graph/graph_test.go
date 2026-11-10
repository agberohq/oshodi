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

func TestAddVerticesBulk(t *testing.T) {
	dir := t.TempDir()
	db, err := oshodi.Open(dir + "/bulk.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	g, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	specs := []VertexSpec{
		{ID: "a", Label: "song", Props: []byte(`{"n":1}`)},
		{ID: "b", Label: "song", Props: []byte(`{"n":2}`)},
		{ID: "c", Label: "artist", Props: nil},
	}
	if err := g.AddVertices(specs); err != nil {
		t.Fatalf("AddVertices: %v", err)
	}
	for _, sp := range specs {
		el, err := g.Vertex(sp.ID)
		if err != nil {
			t.Fatalf("Vertex(%q): %v", sp.ID, err)
		}
		if el.Label != sp.Label {
			t.Fatalf("Vertex(%q) label = %q, want %q", sp.ID, el.Label, sp.Label)
		}
		if string(el.Props) != string(sp.Props) {
			t.Fatalf("Vertex(%q) props = %q, want %q", sp.ID, el.Props, sp.Props)
		}
	}

	// Label is immutable, and validation happens before any write.
	err = g.AddVertices([]VertexSpec{
		{ID: "a", Label: "album"},
		{ID: "d", Label: "song"},
	})
	if !errors.Is(err, ErrLabelConflict) {
		t.Fatalf("label conflict err = %v, want ErrLabelConflict", err)
	}
	if g.vertices.Exists([]byte("d")) {
		t.Fatal("batch wrote a vertex after validation failed")
	}

	if err := g.AddVertices([]VertexSpec{{ID: "x"}, {ID: "x"}}); !errors.Is(err, ErrDuplicateIdentity) {
		t.Fatalf("duplicate err = %v, want ErrDuplicateIdentity", err)
	}

	// Properties update through the same path.
	if err := g.AddVertices([]VertexSpec{{ID: "a", Label: "song", Props: []byte(`{"n":9}`)}}); err != nil {
		t.Fatalf("update props: %v", err)
	}
	el, err := g.Vertex("a")
	if err != nil || string(el.Props) != `{"n":9}` {
		t.Fatalf("updated props = %q err=%v", el.Props, err)
	}
}

func TestRemoveVertexSelfLoop(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.AddVertex("v", "song", nil); err != nil {
		t.Fatal(err)
	}
	id, err := g.AddEdge("v", "v", "self", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.RemoveVertex("v"); err != nil {
		t.Fatalf("remove self-loop vertex: %v", err)
	}
	if _, err := g.Edge(id); err != ErrEdgeNotFound {
		t.Fatalf("self-loop edge survived: %v", err)
	}
	ids, err := g.E().IDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("edge stream after cascade = %v, want empty", ids)
	}
	if _, err := g.Vertex("v"); err != ErrVertexNotFound {
		t.Fatalf("vertex survived: %v", err)
	}
}

func TestAddEdgesIDReuse(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("a", "song", nil)
	g.AddVertex("b", "song", nil)
	g.AddVertex("c", "song", nil)
	id, err := g.AddEdge("a", "b", "rel", []byte(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}

	// A different identity under an existing id is rejected before writing.
	_, err = g.AddEdges([]EdgeSpec{{ID: id, Src: "a", Dst: "c", Label: "rel"}})
	if !errors.Is(err, ErrEdgeExists) {
		t.Fatalf("identity reuse err = %v, want ErrEdgeExists", err)
	}
	el, err := g.Edge(id)
	if err != nil || el.Src != "a" || el.Dst != "b" {
		t.Fatalf("edge changed by rejected batch: %+v %v", el, err)
	}

	// Same identity is a property upsert and must not duplicate adjacency.
	if _, err := g.AddEdges([]EdgeSpec{{ID: id, Src: "a", Dst: "b", Label: "rel", Props: []byte(`{"v":2}`)}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	ids, err := g.V("a").OutE("rel").IDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("adjacency after upsert = %v, want [%s]", ids, id)
	}
	el, err = g.Edge(id)
	if err != nil || string(el.Props) != `{"v":2}` {
		t.Fatalf("props after upsert = %q, %v", el.Props, err)
	}
}

// TestVertexLabelIndexMaintenance pins the in-memory label index against every
// mutation path and against rebuild after reopen.
func TestVertexLabelIndexMaintenance(t *testing.T) {
	db := testDB(t)
	g, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}

	if err := g.AddVertex("a", "song", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.AddVertex("b", "artist", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.AddVertices([]VertexSpec{
		{ID: "c", Label: "song"},
		{ID: "d", Label: "song"},
	}); err != nil {
		t.Fatal(err)
	}

	countLabel := func(g *Graph, label string) int {
		t.Helper()
		n, err := g.V().HasLabel(label).Count()
		if err != nil {
			t.Fatalf("HasLabel(%q): %v", label, err)
		}
		return n
	}
	if got := countLabel(g, "song"); got != 3 {
		t.Fatalf("song count = %d, want 3", got)
	}

	// SetVertex preserves the label; RemoveVertex retires it.
	if err := g.SetVertex("c", []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	if got := countLabel(g, "song"); got != 3 {
		t.Fatalf("song count after SetVertex = %d, want 3", got)
	}
	if err := g.RemoveVertex("a"); err != nil {
		t.Fatal(err)
	}
	if got := countLabel(g, "song"); got != 2 {
		t.Fatalf("song count after remove = %d, want 2", got)
	}

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err = Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if got := countLabel(g, "song"); got != 2 {
		t.Fatalf("song count after reopen = %d, want 2", got)
	}
	if got := countLabel(g, "artist"); got != 1 {
		t.Fatalf("artist count after reopen = %d, want 1", got)
	}
	if ids, err := g.V("a").IDs(); err != nil || len(ids) != 0 {
		t.Fatalf("removed vertex reappeared: %v %v", ids, err)
	}
}

func TestVertexCountAndIDs(t *testing.T) {
	db := testDB(t)
	g, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range []struct{ id, label string }{
		{"a", "song"}, {"b", "artist"}, {"c", "song"},
		{"d", ""}, {"e", "artist"},
	} {
		if err := g.AddVertex(v.id, v.label, nil); err != nil {
			t.Fatalf("add %q: %v", v.id, err)
		}
	}
	if err := g.AddVertices([]VertexSpec{{ID: "f", Label: "song"}}); err != nil {
		t.Fatalf("bulk add: %v", err)
	}

	check := func(g *Graph, when string) {
		t.Helper()
		cases := []struct {
			labels []string
			want   int
		}{
			{nil, 6},
			{[]string{"song"}, 3},
			{[]string{"artist"}, 2},
			{[]string{""}, 1},
			{[]string{"song", "artist"}, 5},
			{[]string{"song", "song"}, 3}, // duplicate labels count once
			{[]string{"missing"}, 0},
		}
		for _, c := range cases {
			got, err := g.VertexCount(c.labels...)
			if err != nil {
				t.Fatalf("%s VertexCount(%v): %v", when, c.labels, err)
			}
			if got != c.want {
				t.Fatalf("%s VertexCount(%v) = %d, want %d", when, c.labels, got, c.want)
			}
		}

		// The fast path must agree with the traversal it replaces.
		eager, err := g.V().HasLabel("song").IDs()
		if err != nil {
			t.Fatal(err)
		}
		fast, err := g.VertexIDs("song")
		if err != nil {
			t.Fatal(err)
		}
		if len(eager) != len(fast) {
			t.Fatalf("%s VertexIDs length %d != eager %d", when, len(fast), len(eager))
		}
		es := make(map[string]struct{}, len(eager))
		for _, id := range eager {
			es[id] = struct{}{}
		}
		for _, id := range fast {
			if _, ok := es[id]; !ok {
				t.Fatalf("%s VertexIDs returned unexpected %q", when, id)
			}
		}
		if all, err := g.VertexIDs(); err != nil || len(all) != 6 {
			t.Fatalf("%s VertexIDs() = %d, %v; want 6", when, len(all), err)
		}
	}
	check(g, "live")

	if err := g.RemoveVertex("a"); err != nil {
		t.Fatal(err)
	}
	if n, err := g.VertexCount("song"); err != nil || n != 2 {
		t.Fatalf("after remove VertexCount(song) = %d, %v; want 2", n, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	g, err = Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	for _, c := range []struct {
		labels []string
		want   int
	}{
		{nil, 5}, // "a" was removed
		{[]string{"song"}, 2},
		{[]string{"artist"}, 2},
		{[]string{""}, 1},
		{[]string{"song", "artist"}, 4},
	} {
		if got, err := g.VertexCount(c.labels...); err != nil || got != c.want {
			t.Fatalf("after reopen VertexCount(%v) = %d, %v; want %d", c.labels, got, err, c.want)
		}
	}
	eager, err := g.V().HasLabel("song").IDs()
	if err != nil {
		t.Fatal(err)
	}
	fast, err := g.VertexIDs("song")
	if err != nil {
		t.Fatal(err)
	}
	if len(eager) != len(fast) || len(fast) != 2 {
		t.Fatalf("after reopen VertexIDs = %v, eager = %v", fast, eager)
	}
}

// TestDeferredVertexSource covers the fused V/HasLabel/Limit plan against the
// eager equivalents.
func TestDeferredVertexSource(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	for _, v := range []struct{ id, label string }{
		{"a", "song"}, {"b", "artist"}, {"c", "song"}, {"d", "song"}, {"e", "artist"},
	} {
		if err := g.AddVertex(v.id, v.label, []byte(`{"n":1}`)); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := g.V().Count(); err != nil || n != 5 {
		t.Fatalf("V().Count() = %d, %v", n, err)
	}
	if n, err := g.V().HasLabel("song").Count(); err != nil || n != 3 {
		t.Fatalf("V().HasLabel().Count() = %d, %v", n, err)
	}
	if n, err := g.V().HasLabel("missing").Count(); err != nil || n != 0 {
		t.Fatalf("missing label count = %d, %v", n, err)
	}
	if n, err := g.V().HasLabel("song").Limit(2).Count(); err != nil || n != 2 {
		t.Fatalf("filter+limit count = %d, %v", n, err)
	}
	if n, err := g.V().Limit(0).Count(); err != nil || n != 0 {
		t.Fatalf("limit 0 count = %d, %v", n, err)
	}

	ids, err := g.V().HasLabel("song").IDs()
	if err != nil || len(ids) != 3 {
		t.Fatalf("fused IDs = %v, %v", ids, err)
	}
	fast, err := g.VertexIDs("song")
	if err != nil || len(fast) != len(ids) {
		t.Fatalf("VertexIDs = %v, %v", fast, err)
	}

	els, err := g.V().HasLabel("song").Limit(2).ToList()
	if err != nil || len(els) != 2 {
		t.Fatalf("fused ToList = %d, %v", len(els), err)
	}
	for _, el := range els {
		if el.Kind != VertexElement || el.Label != "song" || len(el.Props) == 0 {
			t.Fatalf("fused element = %+v", el)
		}
	}

	// Applying a non-fusable stage after a limit must still materialize.
	if n, err := g.V().Limit(2).Out("rel").Count(); err != nil || n != 0 {
		t.Fatalf("limit+out count = %d, %v", n, err)
	}

	// Skip fuses only before labels/limit; the deterministic cases are:
	if n, err := g.V().Skip(2).Count(); err != nil || n != 3 {
		t.Fatalf("skip count = %d, %v", n, err)
	}
	if n, err := g.V().Skip(2).Limit(1).Count(); err != nil || n != 1 {
		t.Fatalf("skip+limit count = %d, %v", n, err)
	}
	if n, err := g.V().Skip(5).Count(); err != nil || n != 0 {
		t.Fatalf("skip past end = %d, %v", n, err)
	}
	if n, err := g.V().HasLabel("song").Skip(1).Count(); err != nil || n != 2 {
		t.Fatalf("filter+skip count = %d, %v", n, err)
	}
	if n, err := g.V().Limit(3).Skip(1).Count(); err != nil || n != 2 {
		t.Fatalf("limit+skip count = %d, %v", n, err)
	}
	if els, err := g.V().Skip(3).Limit(2).ToList(); err != nil || len(els) != 2 {
		t.Fatalf("skip+limit ToList = %d, %v", len(els), err)
	}
}
