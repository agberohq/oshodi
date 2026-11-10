package graph

// Graph-pure analytics over the adjacency index. PageRank, BFS and
// Components hold the graph's shared read lock for their entire
// computation, so topology is stable while they run; mutations are excluded
// for the duration (documented contract).

// PageRank computes PageRank over directed edges for a fixed number of
// iterations. Every vertex participates, including isolated ones; vertices
// without outgoing edges (dangling) redistribute their rank uniformly.
func (g *Graph) PageRank(damping float64, iterations int) (map[string]float64, error) {
	if damping <= 0 || damping >= 1 {
		damping = pageRankDamping
	}
	if iterations <= 0 {
		iterations = pageRankIterations
	}
	var result map[string]float64
	err := g.runTraversal(func() error {
		ids, err := g.allVertexIDs()
		if err != nil {
			return err
		}
		n := float64(len(ids))
		if n == 0 {
			result = map[string]float64{}
			return nil
		}

		// Out-degree counts edges; parallel edges are distinct, matching
		// standard PageRank. (The previous dst-map representation counted a
		// parallel pair once.)
		degree := make(map[string]int, len(g.adj.out))
		for id, lm := range g.adj.out {
			for _, list := range lm {
				degree[id] += len(list)
			}
		}

		rank := make(map[string]float64, len(ids))
		for _, id := range ids {
			rank[id] = 1 / n
		}
		next := make(map[string]float64, len(ids))

		for it := 0; it < iterations; it++ {
			clear(next)
			danglingSum := 0.0
			for _, id := range ids {
				d := degree[id]
				r := rank[id]
				if d == 0 {
					danglingSum += r
					continue
				}
				share := damping * r / float64(d)
				for _, list := range g.adj.out[id] {
					for _, e := range list {
						next[e.dst] += share
					}
				}
			}
			base := (1-damping)/n + damping*danglingSum/n
			for _, id := range ids {
				next[id] += base
			}
			rank, next = next, rank
		}

		result = rank
		return nil
	})
	return result, err
}

// Direction selects which edges an algorithm or traversal follows.
type Direction uint8

const (
	Outgoing Direction = iota
	Incoming
	BothDirections
)

// BFS returns distances from start along edges in the given direction.
// Vertices unreachable from start are absent; start itself is 0.
func (g *Graph) BFS(start string, direction Direction) (map[string]int, error) {
	var result map[string]int
	err := g.runTraversal(func() error {
		if !g.hasVertexLocked(start) {
			return ErrVertexNotFound
		}
		result = g.bfsLocked(start, direction, unboundedDepth, true)
		return nil
	})
	return result, err
}

// KHopOptions configures KHop.
type KHopOptions struct {
	Direction    Direction
	IncludeStart bool // include the starting vertex in the result
}

// KHop returns the vertices within maximum distance n (inclusive) of start.
func (g *Graph) KHop(start string, n int, opts KHopOptions) ([]string, error) {
	var out []string
	err := g.runTraversal(func() error {
		if !g.hasVertexLocked(start) {
			return ErrVertexNotFound
		}
		if n < 0 {
			n = 0
		}
		dist := g.bfsLocked(start, opts.Direction, n, true)
		ids := make([]string, 0, len(dist))
		for id, d := range dist {
			if !opts.IncludeStart && id == start && d == 0 {
				continue
			}
			ids = append(ids, id)
		}
		out = ids
		return nil
	})
	return out, err
}

// bfsLocked walks the adjacency up to maxDepth (-1 = unbounded).
func (g *Graph) bfsLocked(start string, direction Direction, maxDepth int, includeStart bool) map[string]int {
	dist := map[string]int{}
	if maxDepth == 0 {
		return dist
	}
	if includeStart {
		dist[start] = 0
	}
	frontier := []string{start}
	depth := 0
	for len(frontier) > 0 && (maxDepth < 0 || depth < maxDepth) {
		var next []string
		for _, id := range frontier {
			neighbors := g.neighborsLocked(id, direction)
			for _, nb := range neighbors {
				if _, ok := dist[nb]; !ok {
					dist[nb] = depth + 1
					next = append(next, nb)
				}
			}
		}
		frontier = next
		depth++
	}
	return dist
}

func (g *Graph) neighborsLocked(id string, direction Direction) []string {
	var out []string
	if lm, ok := g.adj.out[id]; ok && (direction == Outgoing || direction == BothDirections) {
		for _, list := range lm {
			for _, e := range list {
				out = append(out, e.dst)
			}
		}
	}
	if lm, ok := g.adj.in[id]; ok && (direction == Incoming || direction == BothDirections) {
		for _, list := range lm {
			for _, e := range list {
				out = append(out, e.dst)
			}
		}
	}
	return out
}

// Components labels each vertex with the id of the smallest vertex in its
// weakly connected component.
func (g *Graph) Components() (map[string]string, error) {
	result := make(map[string]string)
	err := g.runTraversal(func() error {
		ids, err := g.allVertexIDs()
		if err != nil {
			return err
		}
		// union-find over undirected connectivity
		parent := make(map[string]string, len(ids))
		for _, id := range ids {
			parent[id] = id
		}
		var find func(string) string
		find = func(x string) string {
			for parent[x] != x {
				x = parent[x]
			}
			return x
		}
		union := func(a, b string) {
			pa, pb := find(a), find(b)
			if pa == pb {
				return
			}
			if pa < pb {
				parent[pb] = pa
			} else {
				parent[pa] = pb
			}
		}
		for id, lm := range g.adj.out {
			for _, list := range lm {
				for _, e := range list {
					union(id, e.dst)
				}
			}
		}
		out := make(map[string]string, len(ids))
		for _, id := range ids {
			out[id] = find(id)
		}
		result = out
		return nil
	})
	return result, err
}
