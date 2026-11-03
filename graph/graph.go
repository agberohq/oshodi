package graph

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/agberohq/oshodi"
)

// Bucket names owned by the graph layer.
const (
	vertexBucket = "graph:v"
	edgeBucket   = "graph:e"
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

// adjacency maps, guarded by coord. out: src → label → dst → {edgeID};
// in mirrors it from the destination side.
type adjacency struct {
	out map[string]map[string]map[string]map[string]struct{}
	in  map[string]map[string]map[string]map[string]struct{}
}

func newAdjacency() *adjacency {
	return &adjacency{
		out: map[string]map[string]map[string]map[string]struct{}{},
		in:  map[string]map[string]map[string]map[string]struct{}{},
	}
}

func addDir(root map[string]map[string]map[string]map[string]struct{}, a, label, b, id string) {
	lv, ok := root[a]
	if !ok {
		lv = map[string]map[string]map[string]struct{}{}
		root[a] = lv
	}
	lm, ok := lv[label]
	if !ok {
		lm = map[string]map[string]struct{}{}
		lv[label] = lm
	}
	s, ok := lm[b]
	if !ok {
		s = map[string]struct{}{}
		lm[b] = s
	}
	s[id] = struct{}{}
}

func (a *adjacency) add(src, dst, label, id string) {
	addDir(a.out, src, label, dst, id)
	addDir(a.in, dst, label, src, id)
}

func removeDir(root map[string]map[string]map[string]map[string]struct{}, a, label, b, id string) {
	lv, ok := root[a]
	if !ok {
		return
	}
	lm, ok := lv[label]
	if !ok {
		return
	}
	s, ok := lm[b]
	if !ok {
		return
	}
	delete(s, id)
	if len(s) == 0 {
		delete(lm, b)
		if len(lm) == 0 {
			delete(root, a)
		}
	}
}

func (a *adjacency) remove(src, dst, label, id string) {
	removeDir(a.out, src, label, dst, id)
	removeDir(a.in, dst, label, src, id)
}

// Graph is a property-graph layer over an Oshodi database. It owns two
// buckets (graph:v, graph:e); direct bucket writes to those buckets are
// unsupported while the handle is open.
type Graph struct {
	db       *oshodi.DB
	vertices *oshodi.Bucket
	edges    *oshodi.Bucket

	// coord serializes mutations, guards the adjacency index and lifecycle.
	// Traversal stages and algorithms take RLock.
	coord sync.RWMutex
	adj   *adjacency

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
		db:       db,
		vertices: db.Bucket(vertexBucket),
		edges:    db.Bucket(edgeBucket),
		adj:      newAdjacency(),
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
