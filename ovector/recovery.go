package ovector

import (
	"math/rand"
	"time"
)

// rebuild reconstructs the in-memory HNSW graph from the vector bucket.
// Mutations are refused until it finishes; corrupt records are skipped and
// counted, never silently accepted.
func (idx *Index) rebuild() error {
	idx.layers = nil

	// Scan all vectors and build a single-layer graph for reconstruction.
	// We rebuild the graph structure by re-adding vectors, which reconstructs
	// the HNSW layers and neighbor relationships.
	var vectors []struct {
		key string
		vec Vec
	}

	err := idx.vectors.ScannerE(nil, func(key []byte, value []byte) error {
		vec, err := decodeVectorRecord(value)
		if err != nil {
			idx.corrupt.Add(1)
			return nil
		}
		vectors = append(vectors, struct {
			key string
			vec Vec
		}{key: string(key), vec: vec})
		return nil
	})
	if err != nil {
		return err
	}

	if len(vectors) == 0 {
		return nil
	}

	// Use a deterministic RNG for rebuild to get consistent graph structure.
	idx.Rng = rand.New(rand.NewSource(42))

	// Re-add all vectors to reconstruct the HNSW graph.
	// We bypass persistence since vectors are already in the bucket.
	for _, v := range vectors {
		if len(idx.layers) == 0 {
			idx.dims = len(v.vec)
		} else if idx.dims != len(v.vec) {
			// Mixed dimensions would poison distance computations; skip
			// and count like corrupt records.
			idx.mismatch.Add(1)
			continue
		}

		insertLevel := idx.randomLevel()
		for insertLevel >= len(idx.layers) {
			idx.layers = append(idx.layers, &hnswLayer{nodes: make(map[string]*hnswNode)})
		}

		var elevator *string
		for i := len(idx.layers) - 1; i >= 0; i-- {
			layer := idx.layers[i]
			newNode := &hnswNode{key: v.key, value: v.vec}

			if layer.entry() == nil {
				layer.nodes[v.key] = newNode
				continue
			}

			searchPoint := layer.entry()
			if elevator != nil {
				searchPoint = layer.nodes[*elevator]
			}

			neighborhood := searchPoint.search(idx.M, idx.EfSearch, v.vec, idx.Distance)
			if len(neighborhood) == 0 {
				continue
			}
			elevator = &neighborhood[0].node.key

			if insertLevel >= i {
				layer.nodes[v.key] = newNode
				for _, nb := range neighborhood {
					nb.node.addNeighbor(newNode, idx.M, idx.Distance)
					newNode.addNeighbor(nb.node, idx.M, idx.Distance)
				}
			}
		}
	}

	// Time-seed so level assignment after rebuild differs across processes.
	idx.Rng = rand.New(rand.NewSource(time.Now().UnixNano()))

	return nil
}

// Stats returns diagnostic information about the index.
func (idx *Index) Stats() map[string]int {
	idx.coord.RLock()
	defer idx.coord.RUnlock()
	return map[string]int{
		"vectors":         idx.Len(),
		"corrupt_records": int(idx.corrupt.Load()),
		"dim_mismatches":  int(idx.mismatch.Load()),
	}
}
