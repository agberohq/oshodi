package graph

import (
	"errors"
	"fmt"
)

// rebuild reconstructs the adjacency index from the edge bucket. Mutations
// are refused until it finishes; corrupt/foreign records are skipped and
// counted, never silently accepted; dangling edges (missing endpoint) are
// defensively dropped.
func (g *Graph) rebuild() error {
	g.adj = newAdjacency()
	var dangling []string

	err := g.edges.ScanE(nil, func(key []byte, value []byte) error {
		src, dst, label, _, derr := decodeEdgeRecord(value)
		if derr != nil {
			g.corrupt.Add(1)
			return nil
		}
		id := string(key)
		// Endpoints checked inside the scan callback: the rebuild streams
		// and never buffers the whole edge set.
		if err := g.mustExist(src); err != nil {
			if errors.Is(err, ErrVertexNotFound) {
				dangling = append(dangling, id)
				return nil
			}
			return err
		}
		if err := g.mustExist(dst); err != nil {
			if errors.Is(err, ErrVertexNotFound) {
				dangling = append(dangling, id)
				return nil
			}
			return err
		}
		g.adj.add(src, dst, label, id)
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: rebuild scan: %w", err)
	}

	for _, id := range dangling {
		if err := g.edges.Delete([]byte(id)); err != nil {
			return err
		}
	}
	g.dangling.Store(int64(len(dangling)))
	return nil
}

// allVertexIDs enumerates every vertex id (isolated vertices included).
func (g *Graph) allVertexIDs() ([]string, error) {
	var ids []string
	err := g.vertices.ScanE(nil, func(key []byte, _ []byte) error {
		ids = append(ids, string(key))
		return nil
	})
	return ids, err
}

// markDirtyInvalidates: any runtime failure of the committed-edge invariant
// blocks traversals until the graph is reopened.
func (g *Graph) markDirty() {
	g.dirty.Store(true)
}

// stats snapshot for diagnostics.
func (g *Graph) Stats() map[string]int {
	g.coord.RLock()
	defer g.coord.RUnlock()
	return map[string]int{
		"corrupt_records": int(g.corrupt.Load()),
		"dangling_edges":  int(g.dangling.Load()),
	}
}

// graph-side helpers used by traversal (call with coord held)

func (g *Graph) hasVertexLocked(id string) bool {
	_, err := g.vertices.Get([]byte(id))
	return err == nil
}

func (g *Graph) vertexLabelLocked(id string) (string, error) {
	raw, err := g.vertices.Get([]byte(id))
	if err != nil {
		return "", err
	}
	label, _, err := decodeVertexRecord(raw)
	return label, err
}

// allEdgeElements decodes every edge into an element (props included).
// Corrupt/foreign records are skipped and counted, never surfaced as scan
// errors — they are not adjacency data.
func (g *Graph) allEdgeElements() ([]Element, error) {
	var out []Element
	err := g.edges.ScanE(nil, func(key []byte, value []byte) error {
		src, dst, label, props, derr := decodeEdgeRecord(value)
		if derr != nil {
			g.corrupt.Add(1)
			return nil
		}
		out = append(out, Element{
			Kind:      EdgeElement,
			ID:        string(key),
			Src:       src,
			Dst:       dst,
			EdgeLabel: label,
			Props:     append([]byte(nil), props...),
		})
		return nil
	})
	return out, err
}
