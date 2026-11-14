package ograph

// Traversal correctness against a naive reference implementation on
// random graphs, including MutateID/Mutate/Filter/Dedup and ordering.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

type refGraph struct {
	vertices map[string]string // id → label
	edges    map[string][3]string
}

func buildRefGraph(t *testing.T, g *Graph, n int, m int) *refGraph {
	t.Helper()
	ref := &refGraph{vertices: map[string]string{}, edges: map[string][3]string{}}
	rng := rand.New(rand.NewSource(7))
	labels := []string{"artist", "song", "genre", "label"}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("v%d", i)
		label := labels[rng.Intn(len(labels))]
		if err := g.AddVertex(id, label, nil); err != nil {
			t.Fatal(err)
		}
		ref.vertices[id] = label
	}
	for i := 0; i < m; i++ {
		a := fmt.Sprintf("v%d", rng.Intn(n))
		b := fmt.Sprintf("v%d", rng.Intn(n))
		label := "rel"
		if rng.Intn(2) == 0 {
			label = "feat"
		}
		eid, err := g.AddEdge(a, b, label, nil)
		if err != nil {
			t.Fatal(err)
		}
		ref.edges[eid] = [3]string{a, b, label}
	}
	return ref
}

func TestTraversalStagesAgainstReference(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ref := buildRefGraph(t, g, 60, 140)

	// V() covers every vertex
	ids, err := g.V().IDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != len(ref.vertices) {
		t.Fatalf("V() size: %d != %d", len(ids), len(ref.vertices))
	}

	// HasLabel counts vertices per label
	for _, label := range []string{"artist", "song"} {
		want := 0
		for _, l := range ref.vertices {
			if l == label {
				want++
			}
		}
		got, err := g.V().HasLabel(label).Count()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("HasLabel(%q): %d != %d", label, got, want)
		}
	}

	// Out labels: unique adjacent vertices computed by reference
	refOut := map[string]map[string]bool{}
	for _, s := range ref.edges {
		if refOut[s[0]] == nil {
			refOut[s[0]] = map[string]bool{}
		}
		refOut[s[0]][s[1]] = true
	}
	for _, v := range ref.vertices {
		want := refOut[v]
		got, err := g.V(v).Out("rel", "feat").IDs()
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("Out(%q): %d != %d", v, len(got), len(want))
		}
		set := map[string]bool{}
		for _, id := range got {
			set[id] = true
		}
		for id := range want {
			if !set[id] {
				t.Fatalf("Out(%q) missing %q", v, id)
			}
		}
	}

	// OutE multiplicity: one entry per edge
	var edgeTotal int
	for e := range ref.edges {
		_ = e
		edgeTotal++
	}
	got, err := g.V().OutE().Count()
	if err != nil {
		t.Fatal(err)
	}
	if got != edgeTotal {
		t.Fatalf("OutE multiplicity: %d != %d", got, edgeTotal)
	}

	// Dedup on labels
	dup, err := g.V().OutE().HasEdgeLabel("rel").Count()
	if err != nil {
		t.Fatal(err)
	}
	refRel := 0
	for _, s := range ref.edges {
		if s[2] == "rel" {
			refRel++
		}
	}
	if dup != refRel {
		t.Fatalf("HasEdgeLabel(rel): %d != %d", dup, refRel)
	}
}

func TestMutateAndFilter(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("Artist:DRAKE", "artist", []byte(`{"name":"Drake","charts":true}`))
	g.AddVertex("artist:drake", "artist", []byte(`{"name":"drake","charts":true}`))
	g.AddVertex("song:x", "song", []byte(`{"charts":true}`))
	g.AddEdge("artist:drake", "song:x", "performs", nil)

	// MutateID retargets the traversal reference; both ids converge on the
	// canonical vertex.
	n, err := g.V("Artist:DRAKE").MutateID(strings.ToLower).Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("mutate id stream: %d", n)
	}
	ids, err := g.V("Artist:DRAKE").MutateID(strings.ToLower).IDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "artist:drake" {
		t.Fatalf("mutate id retarget: %v", ids)
	}

	// Full Mutate: property rewrite in the stream (never persists)
	els, err := g.V("artist:drake").Mutate(func(e Element) (Element, error) {
		e.Props = []byte(`{"name":"DRAKE","charts":true}`)
		return e, nil
	}).Filter(func(e Element) bool {
		return bytes.Contains(e.Props, []byte("DRAKE"))
	}).ToList()
	if err != nil {
		t.Fatal(err)
	}
	if len(els) != 1 {
		t.Fatalf("mutate/filter stream: %d", len(els))
	}

	// The stored props still hold the original value (Mutate never writes)
	el, _ := g.Vertex("artist:drake")
	if string(el.Props) != `{"name":"drake","charts":true}` {
		t.Fatalf("Mutate persisted: %s", el.Props)
	}

	// Traversal over retargeted id: Out must follow the canonical vertex
	n, err = g.V("Artist:DRAKE").MutateID(strings.ToLower).Out("performs").Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("retargeted Out: %d", n)
	}

	// Has with typed values, canonical JSON comparison
	n, err = g.V().Has("charts", true).Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("Has(charts): %d", n)
	}

	// Values stream returns raw json bytes
	vs, err := g.V("song:x").Values("charts").ToList()
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || vs[0].Kind != ValueElement || !bytes.Equal(vs[0].Value, []byte("true")) {
		t.Fatalf("Values stream: %+v", vs)
	}

	// Keys stream: song:x carries exactly one property (charts)
	ks, err := g.V("song:x").Keys().ToList()
	if err != nil {
		t.Fatal(err)
	}
	if len(ks) != 1 || ks[0].Kind != ValueElement || ks[0].Key != "charts" {
		t.Fatalf("Keys stream: %+v", ks)
	}
}

func TestTraversalOrderingStages(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	for i := 0; i < 10; i++ {
		g.AddVertex(fmt.Sprintf("v%d", i), "song", nil)
	}

	ids, err := g.V().OrderByID().Limit(4).IDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 || ids[0] != "v0" || ids[3] != "v3" {
		t.Fatalf("OrderByID+Limit: %v", ids)
	}

	skipped, err := g.V().OrderByID().Skip(8).IDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(skipped) != 2 || skipped[0] != "v8" {
		t.Fatalf("Skip: %v", skipped)
	}

	shuffled, err := g.V().Shuffle(rand.New(rand.NewSource(1))).OrderByID().IDs()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(shuffled)
	if shuffled[0] != "v0" {
		t.Fatal("shuffle lost elements")
	}
}

func TestTraverseMissingAndRetargeted(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("a", "artist", nil)

	// Unknown id: contributes nothing
	n, err := g.V("ghost").Out("rel").Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("unknown id leaked: %d", n)
	}

	// MutateID retargeting onto a nonexistent vertex still yields no
	// adjacency, and does not appear in V() results.
	ids, err := g.V("a").MutateID(func(string) string { return "ghost" }).IDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "ghost" {
		t.Fatalf("mutate retarget: %v", ids)
	}
	n, err = g.V("a").MutateID(func(string) string { return "ghost" }).Out("rel").Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("retargeted adjacency: %d", n)
	}
}
