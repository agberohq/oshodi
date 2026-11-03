package graph

import "errors"

var (
	ErrVertexNotFound   = errors.New("graph: vertex not found")
	ErrEdgeNotFound     = errors.New("graph: edge not found")
	ErrEdgeExists       = errors.New("graph: edge id already bound to a different identity")
	ErrLabelConflict    = errors.New("graph: vertex exists with a different label")
	ErrIndexDirty       = errors.New("graph: adjacency index is dirty; reopen required")
	ErrInvalidIdentity  = errors.New("graph: vertex or edge id must not be empty")
	ErrGraphClosed      = errors.New("graph: graph handle closed")
	ErrGraphAlreadyOpen = errors.New("graph: another graph handle is open on this database")
)
