package ograph

import (
	"fmt"
)

// rebuild reconstructs the adjacency index from the edge bucket. Mutations
// are refused until it finishes; corrupt/foreign records are skipped and
// counted, never silently accepted; dangling edges (missing endpoint) are
// defensively dropped.
func (g *Graph) rebuild() error {
	g.adj = newAdjacency()
	// Resolve vertices and their immutable labels in one scan. This replaces
	// both the 2-per-edge endpoint point reads and the per-vertex label reads
	// used by HasLabel.
	index, err := g.loadVertexIndex()
	if err != nil {
		return fmt.Errorf("ograph: rebuild vertices: %w", err)
	}
	g.vertexIndex = index
	var dangling []string

	err = g.edges.ScannerE(nil, func(key []byte, value []byte) error {
		src, dst, label, _, derr := decodeEdgeRecord(value)
		if derr != nil {
			g.corrupt.Add(1)
			return nil
		}
		id := string(key)
		if _, ok := g.vertexIndex[src]; !ok {
			dangling = append(dangling, id)
			return nil
		}
		if _, ok := g.vertexIndex[dst]; !ok {
			dangling = append(dangling, id)
			return nil
		}
		g.adj.add(src, dst, label, id)
		return nil
	})
	if err != nil {
		return fmt.Errorf("ograph: rebuild scan: %w", err)
	}

	for _, id := range dangling {
		if err := g.edges.Delete([]byte(id)); err != nil {
			return err
		}
	}
	g.dangling.Store(int64(len(dangling)))
	return nil
}

// loadVertexIndex scans the vertex bucket once, decoding labels and counting
// corrupt records. A corrupt vertex remains addressable (so its edges are not
// treated as dangling) but reports no label.
func (g *Graph) loadVertexIndex() (map[string]vertexMeta, error) {
	index := make(map[string]vertexMeta)
	err := g.vertices.ScannerE(nil, func(key []byte, value []byte) error {
		label, _, derr := decodeVertexRecord(value)
		if derr != nil {
			g.corrupt.Add(1)
			index[string(key)] = vertexMeta{}
			return nil
		}
		index[string(key)] = vertexMeta{label: label, valid: true}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return index, nil
}

// allVertexIDs enumerates every vertex id (isolated vertices included).
func (g *Graph) allVertexIDs() ([]string, error) {
	ids := make([]string, 0, len(g.vertexIndex))
	for id := range g.vertexIndex {
		ids = append(ids, id)
	}
	return ids, nil
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
	_, ok := g.vertexIndex[id]
	return ok
}

func (g *Graph) vertexLabelLocked(id string) (string, error) {
	meta, ok := g.vertexIndex[id]
	if !ok {
		return "", ErrVertexNotFound
	}
	if !meta.valid {
		return "", errForeignRecord
	}
	return meta.label, nil
}

// allEdgeElements decodes every edge into an element (props included).
// Corrupt/foreign records are skipped and counted, never surfaced as scan
// errors — they are not adjacency data.
func (g *Graph) allEdgeElements() ([]Element, error) {
	var out []Element
	err := g.edges.ScannerE(nil, func(key []byte, value []byte) error {
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
