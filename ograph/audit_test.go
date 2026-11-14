package ograph

// Liveness and contract tests for the findings in lab/report.md §12
// (panic-safety, lock discipline, lifecycle, multi-handle, dirty gating).

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	oshodi "github.com/agberohq/oshodi"
)

// openTestDB is a distinct helper because testDB already registers cleanup.
func openTestDB(t *testing.T) *oshodi.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := oshodi.Open(filepath.Join(dir, "g.db"))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// A panic inside a traversal callback must not wedge the graph.
func TestPanicInStageDoesNotWedge(t *testing.T) {
	g, err := Open(openTestDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.AddVertex("a", "song", nil)

	func() {
		defer func() { _ = recover() }()
		g.V("a").Filter(func(Element) bool { panic("boom") }).Count()
	}()

	done := make(chan struct{})
	go func() {
		// Any subsequent op must return (liveness), not block.
		_, _ = g.V("a").IDs()
		_ = g.AddVertex("b", "song", nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("graph wedged after panic in a stage callback")
	}
}

// Limit rejects negative counts without panicking.
func TestLimitNegative(t *testing.T) {
	g, err := Open(openTestDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.AddVertex("a", "song", nil)

	n, err := g.V().Limit(-1).Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("Limit(-1): %d", n)
	}
}

// Concurrent traversals over the edge bucket while Stats reads the
// counters — race-detector bait for the corrupt counter.
func TestConcurrentTraversalAgainstStats(t *testing.T) {
	g, err := Open(openTestDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("a", "song", nil)
	g.AddVertex("b", "song", nil)
	if _, err := g.AddEdge("a", "b", "rel", nil); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 50; n++ {
				if _, err := g.E().IDs(); err != nil {
					t.Errorf("E: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 50; n++ {
				g.Stats()
			}
		}()
	}
	wg.Wait()
}

// Terminals refuse a closed handle.
func TestTerminalsAfterClose(t *testing.T) {
	g, err := Open(openTestDB(t))
	if err != nil {
		t.Fatal(err)
	}
	g.AddVertex("a", "song", nil)
	tr := g.V("a")
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.ToList(); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("ToList after close: %v", err)
	}
	if _, err := tr.Count(); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Count after close: %v", err)
	}
	if _, err := tr.IDs(); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("IDs after close: %v", err)
	}
	if err := tr.Iterate(func(Element) error { return nil }); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Iterate after close: %v", err)
	}
}

// A second graph handle over the same database is refused.
func TestSingleHandlePerDB(t *testing.T) {
	db := openTestDB(t)
	g1, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer g1.Close()
	_, err = Open(db)
	if !errors.Is(err, ErrGraphAlreadyOpen) {
		t.Fatalf("second handle accepted: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatal(err)
	}

	// After close, a new handle is allowed.
	g2, err := Open(db)
	if err != nil {
		t.Fatalf("handle after close: %v", err)
	}
	g2.Close()
}

// ErrIndexDirty is reachable — a panicking mutation marks the index
// dirty and gates subsequent operations.
func TestDirtyGateReachable(t *testing.T) {
	g, err := Open(openTestDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddVertex("a", "song", nil)
	err = g.runMutation(func() error { panic(" boom ") })
	if err == nil {
		t.Fatal("mutation panic not surfaced")
	}

	if _, err := g.V("a").IDs(); !errors.Is(err, ErrIndexDirty) {
		t.Fatalf("traversals allowed after panic mutation: %v", err)
	}
	if err := g.AddVertex("b", "song", nil); !errors.Is(err, ErrIndexDirty) {
		t.Fatalf("mutations allowed after panic mutation: %v", err)
	}
}

// Mutations survive a panic in runMutation.
func TestMutationPanicDoesNotWedge(t *testing.T) {
	g, err := Open(openTestDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	done := make(chan struct{})
	go func() {
		defer func() { _ = recover() }()
		_ = g.runMutation(func() error { panic("mutation panic") })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runMutation wedged")
	}
}
