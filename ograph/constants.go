package ograph

import "github.com/agberohq/oshodi/internal/consts"

// Graph-layer constants live here so bucket names, record framing and
// algorithm defaults have a single definition.
const (
	vertexBucket = consts.GraphBucketVertices
	edgeBucket   = consts.GraphBucketEdges

	recMagic   byte = consts.GraphRecordMagic
	recVersion byte = consts.GraphRecordVersion

	pageRankDamping    = consts.GraphPageRankDamping
	pageRankIterations = consts.GraphPageRankIters

	dedupKeySep = consts.GraphDedupKeySep
	dedupLenSep = consts.GraphDedupLenSep

	unboundedDepth = -1
)
