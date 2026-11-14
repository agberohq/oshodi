package ograph

import (
	"bytes"
	"errors"
	"math/rand"
	"sort"

	oshodi "github.com/agberohq/oshodi"
)

// Traversal is a chainable graph traversal. Results are **live,
// weakly-consistent views**: a mutation may occur between two stages and be
// observed by the later one. Any error surfaces from the terminal operation.
//
// Vertex-only stages (V with no ids, HasLabel, Skip, Limit) are fused into a
// deferred plan and evaluated when a terminal or a non-fusable stage needs
// the concrete set. This lets Count/IDs and Limit-terminated projections
// avoid materialising every vertex. Other stages materialise and run
// immediately under the graph's shared read lock.
type Traversal struct {
	g     *Graph
	elems []Element
	err   error

	// Deferred vertex-only source: V() with no ids does not build Elements
	// until a stage or terminal needs them. HasLabel and Limit extend this
	// plan, which lets Count/IDs and Limit-terminated projections avoid
	// materializing the whole vertex set.
	lazyAll    bool
	lazyLabels []string
	lazySkip   int
	lazyLimit  int
}

// V starts a traversal from specific vertices, or from every vertex when no
// ids are given.
func (g *Graph) V(ids ...string) *Traversal {
	t := &Traversal{g: g, lazyLimit: -1}
	t.step(func() error {
		if len(ids) == 0 {
			// Defer building Elements until a stage or terminal needs the
			// concrete set. Count/IDs and Limit-terminated projections then
			// avoid a full 20k-element materialization.
			t.lazyAll = true
			return nil
		}
		out := make([]Element, 0, len(ids))
		for _, id := range ids {
			if id == "" {
				return ErrInvalidIdentity
			}
			if !g.hasVertexLocked(id) {
				continue // unknown ids simply contribute nothing
			}
			out = append(out, Element{Kind: VertexElement, ID: id})
		}
		t.elems = out
		return nil
	})
	return t
}

// E starts a traversal from specific edges, or from every edge.
func (g *Graph) E(ids ...string) *Traversal {
	t := &Traversal{g: g, lazyLimit: -1}
	t.step(func() error {
		var out []Element
		if len(ids) == 0 {
			all, err := g.allEdgeElements()
			if err != nil {
				return err
			}
			out = all
		} else {
			for _, id := range ids {
				el, err := g.loadEdgeLocked(id)
				if err != nil {
					if err == ErrEdgeNotFound {
						continue
					}
					return err
				}
				out = append(out, el)
			}
		}
		t.elems = out
		return nil
	})
	return t
}

// Out follows outgoing edges (optionally filtered by label) and yields the
// unique adjacent vertices.
func (t *Traversal) Out(labels ...string) *Traversal {
	return t.traverseVertices(directionOut, labels...)
}

// In follows incoming edges and yields unique adjacent vertices.
func (t *Traversal) In(labels ...string) *Traversal {
	return t.traverseVertices(directionIn, labels...)
}

// Both follows outgoing and incoming edges, yielding unique adjacent
// vertices (the starting vertex itself may appear if reachable through an
// edge).
func (t *Traversal) Both(labels ...string) *Traversal {
	return t.traverseVertices(directionBoth, labels...)
}

// OutE yields the outgoing edge stream (multiplicity preserved).
func (t *Traversal) OutE(labels ...string) *Traversal {
	return t.traverseEdges(directionOut, labels...)
}

// InE yields the incoming edge stream.
func (t *Traversal) InE(labels ...string) *Traversal {
	return t.traverseEdges(directionIn, labels...)
}

// BothE yields the incident edge stream.
func (t *Traversal) BothE(labels ...string) *Traversal {
	return t.traverseEdges(directionBoth, labels...)
}

// HasLabel filters vertices by their stored label (read from the record
// envelope, no property decode).
func (t *Traversal) HasLabel(label string) *Traversal {
	t.step(func() error {
		if t.lazyAll && t.lazyLimit < 0 {
			t.lazyLabels = append(t.lazyLabels, label)
			return nil
		}
		t.materialize()
		out := make([]Element, 0, len(t.elems))
		for i := range t.elems {
			if t.elems[i].Kind != VertexElement {
				continue
			}
			l, err := t.g.vertexLabelLocked(t.elems[i].ID)
			if err != nil {
				if errors.Is(err, oshodi.ErrKeyNotFound) {
					continue // deleted between stages: no match
				}
				return err
			}
			if l == label {
				t.elems[i].Label = l
				out = append(out, t.elems[i])
			}
		}
		t.elems = out
		return nil
	})
	return t
}

// HasEdgeLabel filters edges by label (no I/O — labels come from the
// adjacency index).
func (t *Traversal) HasEdgeLabel(label string) *Traversal {
	t.step(func() error {
		t.materialize()
		out := make([]Element, 0, len(t.elems))
		for i := range t.elems {
			if t.elems[i].Kind == EdgeElement && t.elems[i].EdgeLabel == label {
				out = append(out, t.elems[i])
			}
		}
		t.elems = out
		return nil
	})
	return t
}

// Has filters elements by a JSON-object property value. Decoded values are
// compared by their canonical JSON encoding, so `Has("year", 2020)` and the
// stored number match regardless of Go type.
func (t *Traversal) Has(key string, value any) *Traversal {
	t.step(func() error {
		t.materialize()
		out := make([]Element, 0, len(t.elems))
		expected, err := jsonMarshal(value)
		if err != nil {
			return err
		}
		for i := range t.elems {
			el := t.elems[i]
			props, ok, err := t.elementProps(el)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			obj, derr := decodeJSONObject(props)
			if derr != nil {
				// props without the JSON convention simply do not match
				continue
			}
			got, ok := obj[key]
			if !ok {
				continue
			}
			gb, merr := jsonMarshal(got)
			if merr != nil {
				return merr
			}
			if bytes.Equal(gb, expected) {
				out = append(out, el)
			}
		}
		t.elems = out
		return nil
	})
	return t
}

// Values projects one property per vertex/edge element as a value stream.
// The raw JSON-encoded bytes of the property are yielded; vertices without
// that property contribute nothing.
func (t *Traversal) Values(key string) *Traversal {
	t.step(func() error {
		t.materialize()
		var out []Element
		for i := range t.elems {
			props, _, err := t.elementProps(t.elems[i])
			if err != nil {
				return err
			}
			raw, ok, derr := rawPropertyValue(props, key)
			if derr != nil {
				continue // non-JSON props never match the convention
			}
			if !ok {
				continue
			}
			out = append(out, Element{
				Kind:  ValueElement,
				Key:   key,
				Value: raw,
			})
		}
		t.elems = out
		return nil
	})
	return t
}

// Keys yields one value element per property key of each vertex/edge.
func (t *Traversal) Keys() *Traversal {
	t.step(func() error {
		t.materialize()
		var out []Element
		for i := range t.elems {
			props, ok, err := t.elementProps(t.elems[i])
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			obj, derr := decodeJSONObject(props)
			if derr != nil {
				continue
			}
			for k, v := range obj {
				raw, merr := jsonMarshal(v)
				if merr != nil {
					return merr
				}
				out = append(out, Element{Kind: ValueElement, Key: k, Value: raw})
			}
		}
		t.elems = out
		return nil
	})
	return t
}

// MutateID remaps element ids in the stream (traversal-reference redirect).
// It is pure: storage is never touched, and a nil result keeps the id.
func (t *Traversal) MutateID(fn func(id string) string) *Traversal {
	t.step(func() error {
		t.materialize()
		for i := range t.elems {
			if next := fn(t.elems[i].ID); next != "" {
				t.elems[i].ID = next
			}
		}
		return nil
	})
	return t
}

// Mutate is the generic user-defined map over the element stream. Element
// properties are loaded on demand before the callback runs; the callback
// may retarget the id and/or rewrite properties. Mutate never persists —
// use the write API for that.
func (t *Traversal) Mutate(fn func(Element) (Element, error)) *Traversal {
	t.step(func() error {
		t.materialize()
		out := make([]Element, 0, len(t.elems))
		for i := range t.elems {
			el := t.elems[i]
			props, ok, err := t.elementProps(el)
			if err != nil {
				return err
			}
			if ok {
				el.Props = props
			}
			next, err := fn(el)
			if err != nil {
				return err
			}
			out = append(out, next)
		}
		t.elems = out
		return nil
	})
	return t
}

// Filter keeps only elements the callback approves.
func (t *Traversal) Filter(fn func(Element) bool) *Traversal {
	t.step(func() error {
		t.materialize()
		out := make([]Element, 0, len(t.elems))
		for i := range t.elems {
			if fn(t.elems[i]) {
				out = append(out, t.elems[i])
			}
		}
		t.elems = out
		return nil
	})
	return t
}

// Dedup removes repeated ids from the stream.
func (t *Traversal) Dedup() *Traversal {
	t.step(func() error {
		t.materialize()
		seen := make(map[string]struct{}, len(t.elems))
		out := make([]Element, 0, len(t.elems))
		for i := range t.elems {
			tk := t.elems[i].Kind
			id := t.elems[i].ID
			if tk == ValueElement {
				// value streams dedup on key+value; length-prefixed so the
				// composite key is unambiguous
				id = dedupKey(t.elems[i].Key, t.elems[i].Value)
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, t.elems[i])
		}
		t.elems = out
		return nil
	})
	return t
}

// Limit keeps the first n elements (position in the stream, not sorted
// order — traversal order is unspecified unless OrderByID is used).
func (t *Traversal) Limit(n int) *Traversal {
	t.step(func() error {
		if n < 0 {
			n = 0
		}
		if t.lazyAll && t.lazyLimit < 0 {
			t.lazyLimit = n
			return nil
		}
		t.materialize()
		if n < len(t.elems) {
			t.elems = t.elems[:n]
		}
		return nil
	})
	return t
}

// Skip drops the first n elements.
func (t *Traversal) Skip(n int) *Traversal {
	t.step(func() error {
		if n <= 0 {
			return nil
		}
		if t.lazyAll && t.lazySkip == 0 && t.lazyLimit < 0 && len(t.lazyLabels) == 0 {
			t.lazySkip = n
			return nil
		}
		t.materialize()
		if n > 0 {
			if n > len(t.elems) {
				n = len(t.elems)
			}
			t.elems = t.elems[n:]
		}
		return nil
	})
	return t
}

// Shuffle randomizes the current stream order with an explicit source.
func (t *Traversal) Shuffle(rng *rand.Rand) *Traversal {
	t.step(func() error {
		t.materialize()
		if rng != nil {
			rng.Shuffle(len(t.elems), func(i, j int) {
				t.elems[i], t.elems[j] = t.elems[j], t.elems[i]
			})
		}
		return nil
	})
	return t
}

// OrderByID sorts the stream by element id (or property key for value
// streams), giving predictable pagination-style behavior.
func (t *Traversal) OrderByID() *Traversal {
	t.step(func() error {
		t.materialize()
		sort.SliceStable(t.elems, func(i, j int) bool {
			if t.elems[i].Kind != t.elems[j].Kind {
				return t.elems[i].Kind < t.elems[j].Kind
			}
			id1, id2 := t.elems[i].ID, t.elems[j].ID
			if t.elems[i].Kind == ValueElement {
				id1, id2 = t.elems[i].Key, t.elems[j].Key
			}
			return id1 < id2
		})
		return nil
	})
	return t
}

// terminals

// ToList materializes the stream with properties loaded.
func (t *Traversal) ToList() ([]Element, error) {
	if t.err != nil {
		return nil, t.err
	}
	t.g.coord.RLock()
	defer t.g.coord.RUnlock()
	if t.g.closing {
		return nil, ErrGraphClosed
	}
	t.materialize()
	if t.err != nil {
		return nil, t.err
	}
	out := make([]Element, 0, len(t.elems))
	for i := range t.elems {
		el := t.elems[i]
		props, _, err := t.elementProps(el)
		if err == nil {
			el.Props = props
		}
		out = append(out, el)
	}
	return out, nil
}

// IDs returns the ids (or keys, for value streams).
func (t *Traversal) IDs() ([]string, error) {
	if t.err != nil {
		return nil, t.err
	}
	t.g.coord.RLock()
	defer t.g.coord.RUnlock()
	if t.g.closing {
		return nil, ErrGraphClosed
	}
	if t.lazyAll {
		return t.lazyIDs()
	}
	out := make([]string, 0, len(t.elems))
	for i := range t.elems {
		if t.elems[i].Kind == ValueElement {
			out = append(out, t.elems[i].Key)
			continue
		}
		out = append(out, t.elems[i].ID)
	}
	return out, nil
}

// Count returns the number of elements in the stream.
func (t *Traversal) Count() (int, error) {
	if t.err != nil {
		return 0, t.err
	}
	t.g.coord.RLock()
	defer t.g.coord.RUnlock()
	if t.g.closing {
		return 0, ErrGraphClosed
	}
	if t.lazyAll {
		return t.countLazy()
	}
	return len(t.elems), nil
}

// Exists reports whether the stream is non-empty.
func (t *Traversal) Exists() (bool, error) {
	n, err := t.Count()
	return n > 0, err
}

// Iterate calls fn for every element in the stream.
func (t *Traversal) Iterate(fn func(Element) error) error {
	if t.err != nil {
		return t.err
	}
	t.g.coord.RLock()
	defer t.g.coord.RUnlock()
	if t.g.closing {
		return ErrGraphClosed
	}
	t.materialize()
	if t.err != nil {
		return t.err
	}
	for i := range t.elems {
		el := t.elems[i]
		props, _, err := t.elementProps(el)
		if err == nil {
			el.Props = props
		}
		if err := fn(el); err != nil {
			return err
		}
	}
	return nil
}

// internals

func matchesAllLabels(label string, wanted []string) bool {
	for _, w := range wanted {
		if w != label {
			return false
		}
	}
	return true
}

// materialize expands a deferred vertex-only source into Elements.
func (t *Traversal) materialize() {
	if !t.lazyAll {
		return
	}
	if t.g.dirty.Load() {
		t.err = ErrIndexDirty
		return
	}
	filter := len(t.lazyLabels) > 0
	capHint := len(t.g.vertexIndex) - t.lazySkip
	if capHint < 0 {
		capHint = 0
	}
	if t.lazyLimit >= 0 && t.lazyLimit < capHint {
		capHint = t.lazyLimit
	}
	out := make([]Element, 0, capHint)
	skipped := 0
	for id, meta := range t.g.vertexIndex {
		if skipped < t.lazySkip {
			skipped++
			continue
		}
		if t.lazyLimit >= 0 && len(out) >= t.lazyLimit {
			break
		}
		if filter {
			if !meta.valid {
				t.err = errForeignRecord
				break
			}
			if !matchesAllLabels(meta.label, t.lazyLabels) {
				continue
			}
		}
		el := Element{Kind: VertexElement, ID: id}
		if filter {
			el.Label = meta.label
		}
		out = append(out, el)
	}
	t.elems = out
	t.lazyAll = false
	t.lazyLabels = nil
	t.lazySkip = 0
	t.lazyLimit = -1
}

func (t *Traversal) countLazy() (int, error) {
	if t.g.dirty.Load() {
		return 0, ErrIndexDirty
	}
	filter := len(t.lazyLabels) > 0
	n := 0
	skipped := 0
	for _, meta := range t.g.vertexIndex {
		if skipped < t.lazySkip {
			skipped++
			continue
		}
		if t.lazyLimit >= 0 && n >= t.lazyLimit {
			break
		}
		if filter {
			if !meta.valid {
				return 0, errForeignRecord
			}
			if !matchesAllLabels(meta.label, t.lazyLabels) {
				continue
			}
		}
		n++
	}
	return n, nil
}

func (t *Traversal) lazyIDs() ([]string, error) {
	if t.g.dirty.Load() {
		return nil, ErrIndexDirty
	}
	filter := len(t.lazyLabels) > 0
	capHint := len(t.g.vertexIndex) - t.lazySkip
	if capHint < 0 {
		capHint = 0
	}
	if t.lazyLimit >= 0 && t.lazyLimit < capHint {
		capHint = t.lazyLimit
	}
	out := make([]string, 0, capHint)
	skipped := 0
	for id, meta := range t.g.vertexIndex {
		if skipped < t.lazySkip {
			skipped++
			continue
		}
		if t.lazyLimit >= 0 && len(out) >= t.lazyLimit {
			break
		}
		if filter {
			if !meta.valid {
				return nil, errForeignRecord
			}
			if !matchesAllLabels(meta.label, t.lazyLabels) {
				continue
			}
		}
		out = append(out, id)
	}
	return out, nil
}

// step runs a stage body under the graph's shared read lock. Stages execute
// immediately: the set for each stage is materialized as the chain is
// built, and errors are surfaced by the terminal op.
func (t *Traversal) step(fn func() error) {
	t.g.ops.Add(1)
	defer t.g.ops.Done()
	t.g.coord.RLock()
	defer t.g.coord.RUnlock()
	if t.g.closing {
		if t.err == nil {
			t.err = ErrGraphClosed
		}
		return
	}
	if t.g.dirty.Load() {
		if t.err == nil {
			t.err = ErrIndexDirty
		}
		return
	}
	if err := fn(); err != nil && t.err == nil {
		t.err = err
	}
}

// elementProps loads properties for a stream element on demand. Returns
// ok=false when the backing record no longer exists.
func (t *Traversal) elementProps(el Element) ([]byte, bool, error) {
	switch el.Kind {
	case VertexElement:
		raw, err := t.g.vertices.Get([]byte(el.ID))
		if err != nil {
			return nil, false, nil // deleted mid-traversal: no props
		}
		_, props, err := decodeVertexRecord(raw)
		if err != nil {
			return nil, false, err
		}
		return props, true, nil
	case EdgeElement:
		raw, err := t.g.edges.Get([]byte(el.ID))
		if err != nil {
			return nil, false, nil
		}
		_, _, _, props, err := decodeEdgeRecord(raw)
		if err != nil {
			return nil, false, err
		}
		return props, true, nil
	default:
		return el.Value, true, nil
	}
}

// traversal internals

type direction uint8

const (
	directionOut direction = iota
	directionIn
	directionBoth
)

func labelSet(labels []string) map[string]struct{} {
	if len(labels) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(labels))
	for _, l := range labels {
		m[l] = struct{}{}
	}
	return m
}

func (t *Traversal) traverseVertices(dir direction, labels ...string) *Traversal {
	t.step(func() error {
		t.materialize()
		wanted := labelSet(labels)
		seen := make(map[string]struct{})
		out := make([]Element, 0, len(t.elems))
		for i := range t.elems {
			if t.elems[i].Kind != VertexElement {
				continue
			}
			id := t.elems[i].ID
			if lm, ok := t.g.adj.out[id]; ok && (dir == directionOut || dir == directionBoth) {
				for label, list := range lm {
					if wanted != nil {
						if _, ok := wanted[label]; !ok {
							continue
						}
					}
					for _, e := range list {
						if _, ok := seen[e.dst]; !ok {
							seen[e.dst] = struct{}{}
							out = append(out, Element{Kind: VertexElement, ID: e.dst})
						}
					}
				}
			}
			if lm, ok := t.g.adj.in[id]; ok && (dir == directionIn || dir == directionBoth) {
				for label, list := range lm {
					if wanted != nil {
						if _, ok := wanted[label]; !ok {
							continue
						}
					}
					for _, e := range list {
						if _, ok := seen[e.dst]; !ok {
							seen[e.dst] = struct{}{}
							out = append(out, Element{Kind: VertexElement, ID: e.dst})
						}
					}
				}
			}
		}
		t.elems = out
		return nil
	})
	return t
}

func (t *Traversal) traverseEdges(dir direction, labels ...string) *Traversal {
	t.step(func() error {
		t.materialize()
		wanted := labelSet(labels)
		out := make([]Element, 0, len(t.elems))
		for i := range t.elems {
			if t.elems[i].Kind != VertexElement {
				continue
			}
			id := t.elems[i].ID
			if lm, ok := t.g.adj.out[id]; ok && (dir == directionOut || dir == directionBoth) {
				for label, list := range lm {
					if wanted != nil {
						if _, ok := wanted[label]; !ok {
							continue
						}
					}
					for _, e := range list {
						out = append(out, Element{
							Kind: EdgeElement, ID: e.id,
							Src: id, Dst: e.dst, EdgeLabel: label,
						})
					}
				}
			}
			if lm, ok := t.g.adj.in[id]; ok && (dir == directionIn || dir == directionBoth) {
				for label, list := range lm {
					if wanted != nil {
						if _, ok := wanted[label]; !ok {
							continue
						}
					}
					for _, e := range list {
						out = append(out, Element{
							Kind: EdgeElement, ID: e.id,
							Src: e.dst, Dst: id, EdgeLabel: label,
						})
					}
				}
			}
		}
		t.elems = out
		return nil
	})
	return t
}
