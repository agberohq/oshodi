package ograph

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"testing"
)

// A corrupt varint length in a stored record must be rejected, not panic the
// process on Open (rebuild decodes every stored record with no recover).
func TestDecodeCorruptRecordNoPanic(t *testing.T) {
	vertex := []byte{recMagic, recVersion}
	vertex = appendField(vertex, []byte("v"))
	vertex = binary.AppendUvarint(vertex, math.MaxUint64)
	if _, _, err := decodeVertexRecord(vertex); err == nil {
		t.Fatal("expected foreign-record error for hostile vertex length")
	}

	edge := []byte{recMagic, recVersion}
	edge = appendField(edge, []byte("s"))
	edge = binary.AppendUvarint(edge, math.MaxUint64)
	if _, _, _, _, err := decodeEdgeRecord(edge); err == nil {
		t.Fatal("expected foreign-record error for hostile edge length")
	}
}

// KHop with n=0 must return the start vertex when IncludeStart is set.
func TestKHopZeroIncludesStart(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.AddVertex("v", "x", nil); err != nil {
		t.Fatal(err)
	}

	ids, err := g.KHop("v", 0, KHopOptions{IncludeStart: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "v" {
		t.Fatalf("KHop(n=0, IncludeStart) = %v, want [v]", ids)
	}
}

// The dedup key must be injective: a key containing a separator must not
// collide with a different key/value pair.
func TestDedupKeyUnambiguous(t *testing.T) {
	if dedupKey("a", []byte("|3:x")) == dedupKey("a|1", []byte("x")) {
		t.Fatal("dedupKey collision between distinct key/value pairs")
	}
}

// A NaN damping factor fails both range comparisons and would poison every
// rank; it must fall back to the default.
func TestPageRankRejectsNaN(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.AddVertex("a", "x", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.AddVertex("b", "x", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdgeID("e1", "a", "b", "link", nil); err != nil {
		t.Fatal(err)
	}

	ranks, err := g.PageRank(math.NaN(), 5)
	if err != nil {
		t.Fatal(err)
	}
	for id, r := range ranks {
		if math.IsNaN(r) {
			t.Fatalf("NaN rank for %s", id)
		}
	}
}

// Traversal terminals must surface corrupt stored records instead of
// silently emitting propertyless elements.
func TestToListSurfacesCorruptRecord(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.AddVertex("a", "x", []byte(`{"k":1}`)); err != nil {
		t.Fatal(err)
	}
	// Corrupt the stored record behind the traversal's back.
	if err := g.vertices.Set([]byte("a"), []byte("junk")); err != nil {
		t.Fatal(err)
	}

	if _, err := g.V("a").ToList(); err == nil {
		t.Fatal("ToList ignored a corrupt record")
	}
}

// Close racing in-flight traversals must not trip the WaitGroup reuse race.
func TestGraphCloseDuringTraversal(t *testing.T) {
	g, err := Open(testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.AddVertex("a", "x", nil); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_, err := g.V("a").IDs()
				if errors.Is(err, ErrGraphClosed) {
					return
				}
				if err != nil {
					t.Errorf("traversal: %v", err)
					return
				}
			}
		}()
	}

	// Close while traversals are in flight.
	if err := g.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	wg.Wait()
}
