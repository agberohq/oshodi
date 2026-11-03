package graph

import (
	"fmt"

	"github.com/agberohq/oshodi"
	ulid "github.com/oklog/ulid/v2"
)

// AddVertex creates or updates a vertex. The label is explicit; on an
// existing vertex the properties are updated but the stored label is
// immutable — changing it requires RemoveVertex + AddVertex.
func (g *Graph) AddVertex(id, label string, props []byte) error {
	if id == "" {
		return ErrInvalidIdentity
	}
	return g.runMutation(func() error {
		raw, err := g.vertices.Get([]byte(id))
		switch {
		case err == oshodi.ErrKeyNotFound:
			return g.vertices.Set([]byte(id), encodeVertexRecord(label, props))
		case err != nil:
			return err
		}
		oldLabel, _, err := decodeVertexRecord(raw)
		if err != nil {
			return err
		}
		if oldLabel != label {
			return fmt.Errorf("vertex %q has label %q: %w", id, oldLabel, ErrLabelConflict)
		}
		return g.vertices.Set([]byte(id), encodeVertexRecord(oldLabel, props))
	})
}

// SetVertex updates only the properties; the label is preserved.
func (g *Graph) SetVertex(id string, props []byte) error {
	return g.runMutation(func() error {
		raw, err := g.vertices.Get([]byte(id))
		if err == oshodi.ErrKeyNotFound {
			return ErrVertexNotFound
		}
		if err != nil {
			return err
		}
		label, _, err := decodeVertexRecord(raw)
		if err != nil {
			return err
		}
		return g.vertices.Set([]byte(id), encodeVertexRecord(label, props))
	})
}

// RemoveVertex removes the vertex and all incident edges. It is not an
// atomic transaction: a crash mid-cascade may leave the vertex present with
// a partial edge set, which is a benign state (no edge points at a missing
// vertex).
func (g *Graph) RemoveVertex(id string) error {
	return g.runMutation(func() error {
		if _, err := g.vertices.Get([]byte(id)); err == oshodi.ErrKeyNotFound {
			return ErrVertexNotFound
		} else if err != nil {
			return err
		}
		edges := g.collectIncidentLocked(id)
		for _, e := range edges {
			if err := g.edges.Delete([]byte(e.id)); err != nil {
				return err
			}
			g.adj.remove(e.src, e.dst, e.label, e.id)
		}
		return g.vertices.Delete([]byte(id))
	})
}

// Vertex fetches one vertex as an element (props loaded).
func (g *Graph) Vertex(id string) (Element, error) {
	var out Element
	err := g.runTraversal(func() error {
		raw, err := g.vertices.Get([]byte(id))
		if err == oshodi.ErrKeyNotFound {
			return ErrVertexNotFound
		}
		if err != nil {
			return err
		}
		label, props, err := decodeVertexRecord(raw)
		if err != nil {
			return err
		}
		out = Element{Kind: VertexElement, ID: id, Label: label, Props: append([]byte(nil), props...)}
		return nil
	})
	return out, err
}

// Edge fetches one edge as an element (props loaded).
func (g *Graph) Edge(id string) (Element, error) {
	var out Element
	err := g.runTraversal(func() error {
		el, err := g.loadEdgeLocked(id)
		if err != nil {
			return err
		}
		out = el
		return nil
	})
	return out, err
}

// AddEdge creates an edge with an auto-generated ULID identity.
func (g *Graph) AddEdge(src, dst, label string, props []byte) (string, error) {
	id := newULID()
	if err := g.AddEdgeID(id, src, dst, label, props); err != nil {
		return "", err
	}
	return id, nil
}

// AddEdgeID creates or updates an edge under an explicit globally unique
// identity. An existing id with identical src/dst/label is a property
// upsert; a different identity is ErrEdgeExists.
func (g *Graph) AddEdgeID(id, src, dst, label string, props []byte) error {
	if id == "" || src == "" || dst == "" {
		return ErrInvalidIdentity
	}
	return g.runMutation(func() error {
		raw, err := g.edges.Get([]byte(id))
		switch {
		case err == oshodi.ErrKeyNotFound:
			// fall through to create
		case err != nil:
			return err
		default:
			os, od, oldLabel, _, derr := decodeEdgeRecord(raw)
			if derr != nil {
				return derr
			}
			if os != src || od != dst || oldLabel != label {
				return fmt.Errorf("edge %q exists as (%q)-[%s]->(%q): %w", id, os, oldLabel, od, ErrEdgeExists)
			}
			return g.edges.Set([]byte(id), encodeEdgeRecord(src, dst, label, props))
		}

		// Endpoints must exist before the edge is committed.
		if err := g.mustExist(src); err != nil {
			return err
		}
		if err := g.mustExist(dst); err != nil {
			return err
		}
		if err := g.edges.Set([]byte(id), encodeEdgeRecord(src, dst, label, props)); err != nil {
			return err
		}
		g.adj.add(src, dst, label, id)
		return nil
	})
}

// RemoveEdge deletes an edge by its globally unique id.
func (g *Graph) RemoveEdge(id string) error {
	return g.runMutation(func() error {
		raw, err := g.edges.Get([]byte(id))
		if err == oshodi.ErrKeyNotFound {
			return ErrEdgeNotFound
		}
		if err != nil {
			return err
		}
		src, dst, label, _, derr := decodeEdgeRecord(raw)
		if derr != nil {
			return derr
		}
		if err := g.edges.Delete([]byte(id)); err != nil {
			return err
		}
		g.adj.remove(src, dst, label, id)
		return nil
	})
}

// --- helpers (call with coord held) ---

func (g *Graph) mustExist(id string) error {
	if _, err := g.vertices.Get([]byte(id)); err == oshodi.ErrKeyNotFound {
		return fmt.Errorf("vertex %q: %w", id, ErrVertexNotFound)
	} else if err != nil {
		return err
	}
	return nil
}

// loadEdgeLocked decodes one edge into an Element.
func (g *Graph) loadEdgeLocked(id string) (Element, error) {
	raw, err := g.edges.Get([]byte(id))
	if err == oshodi.ErrKeyNotFound {
		return Element{}, ErrEdgeNotFound
	}
	if err != nil {
		return Element{}, err
	}
	src, dst, label, props, derr := decodeEdgeRecord(raw)
	if derr != nil {
		return Element{}, derr
	}
	return Element{
		Kind:      EdgeElement,
		ID:        id,
		Src:       src,
		Dst:       dst,
		EdgeLabel: label,
		Props:     append([]byte(nil), props...),
	}, nil
}

// collectIncidentLocked returns every edge touching the vertex.
func (g *Graph) collectIncidentLocked(id string) []edgeRef {
	var refs []edgeRef
	if lm, ok := g.adj.out[id]; ok {
		for label, targets := range lm {
			for dst, ids := range targets {
				for eid := range ids {
					refs = append(refs, edgeRef{src: id, dst: dst, label: label, id: eid})
				}
			}
		}
	}
	if lm, ok := g.adj.in[id]; ok {
		for label, sources := range lm {
			for src, ids := range sources {
				for eid := range ids {
					refs = append(refs, edgeRef{src: src, dst: id, label: label, id: eid})
				}
			}
		}
	}
	return refs
}

func newULID() string {
	return ulid.Make().String()
}

// EdgeSpec is one edge for AddEdges.
type EdgeSpec struct {
	ID    string // optional; empty = auto ULID
	Src   string
	Dst   string
	Label string
	Props []byte
}

// AddEdges ingests edges in bulk: endpoints are validated once per touched
// vertex, records are written through one batched write and the adjacency
// index is mirrored in a single pass. Partial states are possible on write
// errors (documented non-transactional); a failed batch marks the index
// dirty so no traversal runs against a torn state.
func (g *Graph) AddEdges(specs []EdgeSpec) ([]string, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	ids := make([]string, len(specs))
	return ids, g.runMutation(func() error {
		// Validate endpoints once per unique vertex and identity map.
		seenVertex := map[string]struct{}{}
		seenID := map[string]struct{}{}
		entries := make([]oshodi.KV, 0, len(specs))
		for i := range specs {
			sp := specs[i]
			if sp.Src == "" || sp.Dst == "" {
				return ErrInvalidIdentity
			}
			if sp.ID == "" {
				sp.ID = newULID()
			} else if _, dup := seenID[sp.ID]; dup {
				return fmt.Errorf("edge id %q duplicated within batch: %w", sp.ID, ErrEdgeExists)
			}
			seenID[sp.ID] = struct{}{}
			for _, end := range []string{sp.Src, sp.Dst} {
				if _, ok := seenVertex[end]; ok {
					continue
				}
				seenVertex[end] = struct{}{}
				if _, err := g.vertices.Get([]byte(end)); err != nil {
					if err == oshodi.ErrKeyNotFound {
						return fmt.Errorf("vertex %q: %w", end, ErrVertexNotFound)
					}
					return err
				}
			}
			ids[i] = sp.ID
			entries = append(entries, oshodi.KV{
				Key:   []byte(sp.ID),
				Value: encodeEdgeRecord(sp.Src, sp.Dst, sp.Label, sp.Props),
			})
		}
		if err := g.edges.BatchSet(entries); err != nil {
			return err
		}
		for i := range specs {
			g.adj.add(specs[i].Src, specs[i].Dst, specs[i].Label, ids[i])
		}
		return nil
	})
}
