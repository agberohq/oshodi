package graph

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/agberohq/oshodi"
)

var errForeignRecord = errors.New("graph: foreign or corrupt record")

// Live-handle registry: one graph handle per *oshodi.DB. Two handles would
// silently maintain divergent adjacency indexes over the same buckets.
var (
	handleMu    sync.Mutex
	liveHandles = map[*oshodi.DB]*Graph{}
)

// edgeRef is one edge as the adjacency index sees it.
type edgeRef struct {
	src, dst, label, id string
}

// adjEdge is one edge endpoint as the adjacency index stores it: the
// neighbour's id and the edge id. For the incoming index, dst holds the source.
type adjEdge struct {
	dst string
	id  string
}

// adjacency maps, guarded by coord. out: src → label → [neighbour, edgeID];
// in mirrors it from the destination side. Keeping one slice per
// (source,label) avoids a map and a slice allocation per destination, which
// was the dominant retained allocation in the heap profile.
type adjacency struct {
	out map[string]map[string][]adjEdge
	in  map[string]map[string][]adjEdge
}

func newAdjacency() *adjacency {
	return &adjacency{
		out: map[string]map[string][]adjEdge{},
		in:  map[string]map[string][]adjEdge{},
	}
}

func addDir(root map[string]map[string][]adjEdge, a, label, b, id string) {
	lm, ok := root[a]
	if !ok {
		lm = map[string][]adjEdge{}
		root[a] = lm
	}
	lm[label] = append(lm[label], adjEdge{dst: b, id: id})
}

func (a *adjacency) add(src, dst, label, id string) {
	addDir(a.out, src, label, dst, id)
	addDir(a.in, dst, label, src, id)
}

func removeDir(root map[string]map[string][]adjEdge, a, label, b, id string) {
	lm, ok := root[a]
	if !ok {
		return
	}
	list, ok := lm[label]
	if !ok {
		return
	}
	for i := range list {
		if list[i].id != id {
			continue
		}
		list = append(list[:i], list[i+1:]...)
		break
	}
	if len(list) > 0 {
		lm[label] = list
		return
	}
	delete(lm, label)
	if len(lm) == 0 {
		delete(root, a)
	}
}

func (a *adjacency) remove(src, dst, label, id string) {
	removeDir(a.out, src, label, dst, id)
	removeDir(a.in, dst, label, src, id)
}

// vertexMeta is the in-memory view of one vertex. valid is false when the
// stored record could not be decoded: the vertex still exists for endpoint
// checks, but its label is unknown.
type vertexMeta struct {
	label string
	valid bool
}

// Graph is a property-graph layer over an Oshodi database. It owns two
// buckets (graph:v, graph:e); direct bucket writes to those buckets are
// unsupported while the handle is open.
type Graph struct {
	db       *oshodi.DB
	vertices *oshodi.Bucket
	edges    *oshodi.Bucket

	// coord serializes mutations, guards the adjacency and vertex indexes,
	// and lifecycle. Traversal stages and algorithms take RLock.
	coord sync.RWMutex
	adj   *adjacency
	// vertexIndex maps vertex id to its immutable label. Vertex labels are
	// immutable, so rebuilding it once at Open removes a point read from
	// every existence check, edge insert and label filter.
	vertexIndex map[string]vertexMeta

	closing  bool
	dirty    atomic.Bool
	corrupt  atomic.Int64 // corrupt/foreign records skipped/counted
	dangling atomic.Int64 // dangling edges dropped during rebuild

	ops sync.WaitGroup
}

// Open builds a graph handle over db. The adjacency index is rebuilt from
// the edge bucket in one scan; graph mutations are refused until that scan
// completes. One graph handle per database: a second Open over the same
// *oshodi.DB is ErrGraphAlreadyOpen (two handles would silently maintain
// divergent adjacency indexes).
func Open(db *oshodi.DB) (*Graph, error) {
	handleMu.Lock()
	defer handleMu.Unlock()
	if _, ok := liveHandles[db]; ok {
		return nil, ErrGraphAlreadyOpen
	}
	g := &Graph{
		db:          db,
		vertices:    db.Bucket(vertexBucket),
		edges:       db.Bucket(edgeBucket),
		adj:         newAdjacency(),
		vertexIndex: map[string]vertexMeta{},
	}
	if err := g.rebuild(); err != nil {
		return nil, err
	}
	liveHandles[db] = g
	return g, nil
}

// Close prevents new operations, waits for in-flight ones and releases the
// adjacency index. It never closes the underlying database.
// Close prevents new operations, waits for in-flight ones and releases the
// adjacency index. It never closes the underlying database.
func (g *Graph) Close() error {
	g.coord.Lock()
	if g.closing {
		g.coord.Unlock()
		return ErrGraphClosed
	}
	g.closing = true
	g.adj = nil
	g.vertexIndex = nil
	g.coord.Unlock()

	handleMu.Lock()
	delete(liveHandles, g.db)
	handleMu.Unlock()

	g.ops.Wait()
	return nil
}

// runMutation serializes all graph mutations.
func (g *Graph) runMutation(op func() error) (err error) {
	g.ops.Add(1)
	defer g.ops.Done()
	g.coord.Lock()
	defer g.coord.Unlock()
	defer func() {
		// A panic inside a mutation can break the committed-edge
		// invariant. Mark dirty and surface it as an error.
		if r := recover(); r != nil {
			g.markDirty()
			err = fmt.Errorf("graph: mutation failed and index marked dirty: %v", r)
		}
	}()
	if g.closing {
		return ErrGraphClosed
	}
	if g.dirty.Load() {
		return ErrIndexDirty
	}
	return op()
}

// runTraversal runs a traversal stage or algorithm under a shared lock.
func (g *Graph) runTraversal(fn func() error) error {
	g.ops.Add(1)
	defer g.ops.Done()
	g.coord.RLock()
	defer g.coord.RUnlock()
	if g.closing {
		return ErrGraphClosed
	}
	if g.dirty.Load() {
		return ErrIndexDirty
	}
	return fn()
}

// VertexCount returns the number of vertices whose label is one of labels, or
// every vertex when no labels are given. It reads the in-memory label index,
// so it does not materialize Elements and does not allocate per vertex.
//
// A label-filtered count fails with errForeignRecord when a corrupt vertex
// record is present, matching V().HasLabel(...).Count().
func (g *Graph) VertexCount(labels ...string) (int, error) {
	var n int
	err := g.runTraversal(func() error {
		filter := len(labels) > 0
		for _, meta := range g.vertexIndex {
			if filter {
				if !meta.valid {
					return errForeignRecord
				}
				if !matchesAnyLabel(meta.label, labels) {
					continue
				}
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// VertexIDs returns the ids of vertices whose label is one of labels, or every
// vertex id when no labels are given. Order is unspecified, like V().IDs().
// The result is a fresh slice owned by the caller.
func (g *Graph) VertexIDs(labels ...string) ([]string, error) {
	var ids []string
	err := g.runTraversal(func() error {
		filter := len(labels) > 0
		ids = make([]string, 0, len(g.vertexIndex))
		for id, meta := range g.vertexIndex {
			if filter {
				if !meta.valid {
					return errForeignRecord
				}
				if !matchesAnyLabel(meta.label, labels) {
					continue
				}
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func matchesAnyLabel(label string, wanted []string) bool {
	for _, w := range wanted {
		if w == label {
			return true
		}
	}
	return false
}
