package ovector

import (
	"container/heap"
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agberohq/oshodi"
)

// Live-handle registry: one vector handle per *oshodi.DB.
var (
	handleMu    sync.Mutex
	liveHandles = map[*oshodi.DB]*Index{}
)

// hnswNode is a node in one layer of the HNSW graph.
type hnswNode struct {
	key   string
	value Vec
	// neighbors maps neighbor keys to neighbor nodes at this layer.
	neighbors map[string]*hnswNode
}

// addNeighbor adds a neighbor, replacing the worst if full.
func (n *hnswNode) addNeighbor(newNode *hnswNode, m int, dist DistanceFunc) {
	if n.neighbors == nil {
		n.neighbors = make(map[string]*hnswNode, m)
	}
	n.neighbors[newNode.key] = newNode
	if len(n.neighbors) <= m {
		return
	}
	var worstDist float32 = float32(math.Inf(-1))
	var worst *hnswNode
	for _, nb := range n.neighbors {
		d := dist(nb.value, n.value)
		if d > worstDist || worst == nil {
			worstDist = d
			worst = nb
		}
	}
	delete(n.neighbors, worst.key)
	delete(worst.neighbors, n.key)
	worst.replenish(m, dist)
}

func (n *hnswNode) replenish(m int, dist DistanceFunc) {
	if len(n.neighbors) >= m {
		return
	}
	for _, nb := range n.neighbors {
		for key, candidate := range nb.neighbors {
			if _, ok := n.neighbors[key]; ok {
				continue
			}
			if candidate == n {
				continue
			}
			n.addNeighbor(candidate, m, dist)
			if len(n.neighbors) >= m {
				return
			}
		}
	}
}

func (n *hnswNode) isolate(m int, dist DistanceFunc) {
	// Remove all backlinks before replenishing so replenish cannot reach
	// n again.
	for _, nb := range n.neighbors {
		delete(nb.neighbors, n.key)
	}
	for _, nb := range n.neighbors {
		nb.replenish(m, dist)
	}
}

// searchCandidate for the heap-based search.
type searchCandidate struct {
	node *hnswNode
	dist float32
}

// candidateHeap orders search candidates by distance: a min-heap by
// default, or a max-heap when max is set (the result set uses max so
// heap.Pop evicts the worst candidate).
type candidateHeap struct {
	items []*searchCandidate
	max   bool
}

func (h candidateHeap) Len() int { return len(h.items) }
func (h candidateHeap) Less(i, j int) bool {
	if h.max {
		return h.items[i].dist > h.items[j].dist
	}
	return h.items[i].dist < h.items[j].dist
}
func (h candidateHeap) Swap(i, j int)       { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *candidateHeap) Push(x interface{}) { h.items = append(h.items, x.(*searchCandidate)) }
func (h *candidateHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}

// worst returns the farthest candidate (the root of a max-mode heap).
func (h *candidateHeap) worst() *searchCandidate { return h.items[0] }

// search returns the k closest nodes within the same layer.
// It uses the standard HNSW early-termination condition: stop when the
// closest unvisited candidate is worse than the worst result in the
// efSearch-bounded result set.
func (n *hnswNode) search(k, efSearch int, target Vec, dist DistanceFunc) []*searchCandidate {
	// The exploration budget must be at least k, or the result set can't
	// hold all k neighbors.
	if efSearch < k {
		efSearch = k
	}
	// candidates is a min-heap ordered by distance (closest first).
	candidates := &candidateHeap{}
	entryDist := dist(n.value, target)
	candidates.items = append(candidates.items, &searchCandidate{node: n, dist: entryDist})

	// result is a max-heap bounded by efSearch; the root is the worst held.
	result := &candidateHeap{max: true}
	visited := make(map[string]bool)
	visited[n.key] = true
	heap.Push(result, &searchCandidate{node: n, dist: entryDist})

	for candidates.Len() > 0 {
		currentCand := heap.Pop(candidates).(*searchCandidate)
		current := currentCand.node

		// Standard HNSW termination: if the closest unvisited node is worse
		// than the worst in our result set, no better candidate exists.
		if result.Len() >= efSearch && currentCand.dist > result.worst().dist {
			break
		}

		neighborKeys := make([]string, 0, len(current.neighbors))
		for nk := range current.neighbors {
			neighborKeys = append(neighborKeys, nk)
		}
		slices.Sort(neighborKeys)

		for _, nk := range neighborKeys {
			neighbor := current.neighbors[nk]
			if visited[nk] {
				continue
			}
			visited[nk] = true
			d := dist(neighbor.value, target)

			// Only candidates that make it into the result set are explored
			// further (HNSW Algorithm 2); worse ones are pruned.
			if result.Len() < efSearch {
				sc := &searchCandidate{node: neighbor, dist: d}
				heap.Push(result, sc)
				heap.Push(candidates, sc)
			} else if d < result.worst().dist {
				heap.Pop(result)
				sc := &searchCandidate{node: neighbor, dist: d}
				heap.Push(result, sc)
				heap.Push(candidates, sc)
			}
		}
	}

	// Return only the top-k results from the efSearch-bounded set.
	out := make([]*searchCandidate, 0, result.Len())
	for result.Len() > 0 {
		out = append(out, heap.Pop(result).(*searchCandidate))
	}
	// The max-heap pops worst-first; reverse to best-first.
	slices.Reverse(out)
	// Trim to k if we have more.
	if len(out) > k {
		out = out[:k]
	}
	return out
}

// hnswLayer is one layer of the HNSW graph.
type hnswLayer struct {
	nodes map[string]*hnswNode
}

func (l *hnswLayer) entry() *hnswNode {
	if l == nil {
		return nil
	}
	for _, n := range l.nodes {
		return n
	}
	return nil
}

func (l *hnswLayer) size() int {
	if l == nil {
		return 0
	}
	return len(l.nodes)
}

// Index is a Hierarchical Navigable Small World (HNSW) vector index over an
// Oshodi database. It owns two buckets (vector:v, vector:e); direct bucket
// writes to those buckets are unsupported while the handle is open.
type Index struct {
	db      *oshodi.DB
	vectors *oshodi.Bucket
	edges   *oshodi.Bucket

	// HNSW parameters — must be set before Add.
	Distance DistanceFunc
	M        int
	Ml       float64
	EfSearch int
	Rng      *rand.Rand

	// coord serializes mutations, guards the in-memory graph, and lifecycle.
	coord sync.RWMutex
	// layers is the multi-layer HNSW graph structure.
	layers []*hnswLayer
	// dims caches the embedding dimension from the first vector added.
	dims int

	closing  bool
	dirty    atomic.Bool
	corrupt  atomic.Int64
	mismatch atomic.Int64

	ops sync.WaitGroup
}

// Open builds a vector index handle over db. The in-memory graph is rebuilt
// from the buckets in one scan; mutations are refused until that scan
// completes. One handle per database.
func Open(db *oshodi.DB) (*Index, error) {
	handleMu.Lock()
	defer handleMu.Unlock()
	if _, ok := liveHandles[db]; ok {
		return nil, ErrVectorAlreadyOpen
	}
	idx := &Index{
		db:       db,
		vectors:  db.Bucket(vectorBucket),
		edges:    db.Bucket(edgeBucket),
		Distance: CosineDistance,
		M:        DefaultM,
		Ml:       DefaultMl,
		EfSearch: DefaultEfSearch,
		Rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if err := idx.rebuild(); err != nil {
		return nil, err
	}
	liveHandles[db] = idx
	return idx, nil
}

// Close prevents new operations, waits for in-flight ones and releases the
// in-memory graph. It never closes the underlying database.
func (idx *Index) Close() error {
	idx.coord.Lock()
	if idx.closing {
		idx.coord.Unlock()
		return ErrVectorClosed
	}
	idx.closing = true
	idx.layers = nil
	idx.coord.Unlock()

	handleMu.Lock()
	delete(liveHandles, idx.db)
	handleMu.Unlock()

	idx.ops.Wait()
	return nil
}

func (idx *Index) runMutation(op func() error) (err error) {
	// closing must be checked before ops.Add: Close takes the write lock
	// before ops.Wait, so anything registered under the lock has finished
	// by then, and ops refused here never register.
	idx.coord.Lock()
	defer idx.coord.Unlock()
	if idx.closing {
		return ErrVectorClosed
	}
	idx.ops.Add(1)
	defer idx.ops.Done()
	if idx.dirty.Load() {
		return ErrIndexDirty
	}
	return op()
}

func (idx *Index) runTraversal(fn func() error) error {
	// See runMutation for the closing/ops.Add ordering.
	idx.coord.RLock()
	defer idx.coord.RUnlock()
	if idx.closing {
		return ErrVectorClosed
	}
	idx.ops.Add(1)
	defer idx.ops.Done()
	if idx.dirty.Load() {
		return ErrIndexDirty
	}
	return fn()
}

// randomLevel generates a random layer for a new node.
func (idx *Index) randomLevel() int {
	max := 1
	if len(idx.layers) > 0 {
		max = maxLevel(idx.Ml, idx.layers[0].size())
	}
	for level := 0; level < max; level++ {
		if idx.Rng.Float64() > idx.Ml {
			return level
		}
	}
	return max
}

func maxLevel(ml float64, numNodes int) int {
	if ml == 0 || numNodes == 0 {
		return 1
	}
	l := math.Log(float64(numNodes))
	l /= math.Log(1 / ml)
	return int(math.Round(l)) + 1
}

// AddMany inserts multiple vectors into the index in a single mutation.
// If a vector with an existing key is included, it is replaced.
func (idx *Index) AddMany(pairs ...struct {
	Key string
	Vec Vec
}) error {
	if len(pairs) == 0 {
		return nil
	}
	return idx.runMutation(func() error {
		for _, p := range pairs {
			if p.Key == "" {
				return ErrInvalidIdentity
			}
			if idx.Distance == nil {
				return ErrDimMismatch
			}
			if len(idx.layers) == 0 {
				idx.dims = len(p.Vec)
			} else if idx.dims != len(p.Vec) {
				return ErrDimMismatch
			}

			insertLevel := idx.randomLevel()
			for insertLevel >= len(idx.layers) {
				idx.layers = append(idx.layers, &hnswLayer{nodes: make(map[string]*hnswNode)})
			}

			// Replace if exists.
			if idx.layers[0].nodes[p.Key] != nil {
				idx.deleteLocked(p.Key)
			}

			var elevator *string
			for i := len(idx.layers) - 1; i >= 0; i-- {
				layer := idx.layers[i]
				newNode := &hnswNode{key: p.Key, value: p.Vec}

				if layer.entry() == nil {
					layer.nodes[p.Key] = newNode
					continue
				}

				searchPoint := layer.entry()
				if elevator != nil {
					searchPoint = layer.nodes[*elevator]
					if searchPoint == nil {
						searchPoint = layer.entry()
					}
				}
				if searchPoint == nil {
					layer.nodes[p.Key] = newNode
					continue
				}

				neighborhood := searchPoint.search(idx.M, idx.EfSearch, p.Vec, idx.Distance)
				if len(neighborhood) == 0 {
					continue
				}
				elevator = &neighborhood[0].node.key

				if insertLevel >= i {
					layer.nodes[p.Key] = newNode
					for _, nb := range neighborhood {
						nb.node.addNeighbor(newNode, idx.M, idx.Distance)
						newNode.addNeighbor(nb.node, idx.M, idx.Distance)
					}
				}
			}

			if err := idx.vectors.Set([]byte(p.Key), encodeVectorRecord(p.Vec)); err != nil {
				idx.dirty.Store(true)
				return err
			}
		}
		return nil
	})
}

// Add inserts a vector into the index. If another vector with the same key
// exists, it is replaced. The key is persisted in the bucket; the embedding
// is stored in the value.
func (idx *Index) Add(key string, vec Vec) error {
	if key == "" {
		return ErrInvalidIdentity
	}
	return idx.runMutation(func() error {
		if idx.Distance == nil {
			return ErrDimMismatch
		}
		if len(idx.layers) == 0 {
			idx.dims = len(vec)
		} else if idx.dims != len(vec) {
			return ErrDimMismatch
		}

		insertLevel := idx.randomLevel()
		for insertLevel >= len(idx.layers) {
			idx.layers = append(idx.layers, &hnswLayer{nodes: make(map[string]*hnswNode)})
		}

		// Replace if exists.
		if idx.layers[0].nodes[key] != nil {
			idx.deleteLocked(key)
		}

		var elevator *string
		for i := len(idx.layers) - 1; i >= 0; i-- {
			layer := idx.layers[i]
			newNode := &hnswNode{key: key, value: vec}

			if layer.entry() == nil {
				layer.nodes[key] = newNode
				continue
			}

			searchPoint := layer.entry()
			if elevator != nil {
				searchPoint = layer.nodes[*elevator]
				// After deleteLocked, the elevator node may no longer exist in this layer.
				// Fall back to any entry point.
				if searchPoint == nil {
					searchPoint = layer.entry()
				}
			}
			if searchPoint == nil {
				layer.nodes[key] = newNode
				continue
			}

			neighborhood := searchPoint.search(idx.M, idx.EfSearch, vec, idx.Distance)
			if len(neighborhood) == 0 {
				continue
			}
			elevator = &neighborhood[0].node.key

			if insertLevel >= i {
				layer.nodes[key] = newNode
				for _, nb := range neighborhood {
					nb.node.addNeighbor(newNode, idx.M, idx.Distance)
					newNode.addNeighbor(nb.node, idx.M, idx.Distance)
				}
			}
		}

		// Persist the vector to the bucket.
		if err := idx.vectors.Set([]byte(key), encodeVectorRecord(vec)); err != nil {
			idx.dirty.Store(true)
			return err
		}
		return nil
	})
}

// SearchResult contains a neighbor key and its distance from the query.
type SearchResult struct {
	Key      string
	Distance float32
}

// Search finds the k nearest neighbors to the query vector.
func (idx *Index) Search(near Vec, k int) ([]string, error) {
	results, err := idx.SearchWithDistance(near, k)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(results))
	for i, r := range results {
		keys[i] = r.Key
	}
	return keys, nil
}

// SearchWithDistance finds the k nearest neighbors to the query vector,
// returning both keys and distances. It returns no results if k is not
// positive.
func (idx *Index) SearchWithDistance(near Vec, k int) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}
	var results []SearchResult
	err := idx.runTraversal(func() error {
		if len(idx.layers) == 0 {
			return nil
		}
		if idx.dims != len(near) {
			return ErrDimMismatch
		}

		var elevator *string
		for layer := len(idx.layers) - 1; layer >= 0; layer-- {
			searchPoint := idx.layers[layer].entry()
			if elevator != nil {
				searchPoint = idx.layers[layer].nodes[*elevator]
			}
			if searchPoint == nil {
				continue
			}

			if layer > 0 {
				nodes := searchPoint.search(1, idx.EfSearch, near, idx.Distance)
				if len(nodes) > 0 {
					elevator = &nodes[0].node.key
				}
				continue
			}
			nodes := searchPoint.search(k, idx.EfSearch, near, idx.Distance)
			results = make([]SearchResult, 0, len(nodes))
			for _, n := range nodes {
				results = append(results, SearchResult{Key: n.node.key, Distance: n.dist})
			}
		}
		return nil
	})
	return results, err
}

// Delete removes a vector from the index by key.
func (idx *Index) Delete(key string) error {
	return idx.runMutation(func() error {
		deleted := idx.deleteLocked(key)
		if !deleted {
			return ErrVectorNotFound
		}
		return idx.vectors.Delete([]byte(key))
	})
}

func (idx *Index) deleteLocked(key string) bool {
	deleted := false
	for _, layer := range idx.layers {
		node, ok := layer.nodes[key]
		if !ok {
			continue
		}
		delete(layer.nodes, key)
		node.isolate(idx.M, idx.Distance)
		deleted = true
	}
	// Remove empty layers from the top, keeping at least layer 0.
	for i := len(idx.layers) - 1; i > 0; i-- {
		if len(idx.layers[i].nodes) == 0 {
			idx.layers = idx.layers[:i]
		} else {
			break
		}
	}
	return deleted
}

// Lookup returns the stored vector for a key.
func (idx *Index) Lookup(key string) (Vec, error) {
	var vec Vec
	err := idx.runTraversal(func() error {
		if len(idx.layers) == 0 {
			return ErrVectorNotFound
		}
		node, ok := idx.layers[0].nodes[key]
		if !ok {
			return ErrVectorNotFound
		}
		vec = node.value
		return nil
	})
	return vec, err
}

// Len returns the number of vectors in the index.
func (idx *Index) Len() int {
	idx.coord.RLock()
	defer idx.coord.RUnlock()
	if len(idx.layers) == 0 {
		return 0
	}
	return idx.layers[0].size()
}

// Dims returns the embedding dimension, or 0 if empty.
func (idx *Index) Dims() int {
	idx.coord.RLock()
	defer idx.coord.RUnlock()
	return idx.dims
}
