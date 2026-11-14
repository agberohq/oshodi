package vector

import "github.com/agberohq/oshodi/internal/consts"

// Vector-layer constants live here so bucket names, record framing and
// algorithm defaults have a single definition.
const (
	vectorBucket = consts.VectorBucketVectors
	edgeBucket   = consts.VectorBucketEdges

	recMagic   byte = consts.VectorRecordMagic
	recVersion byte = consts.VectorRecordVersion

	// HNSW defaults tuned for face embeddings (128–512 dims).
	DefaultM        = consts.VectorM
	DefaultMl       = consts.VectorMl
	DefaultEfSearch = consts.VectorEfSearch
)
