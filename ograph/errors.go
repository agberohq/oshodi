package ograph

import "errors"

var (
	ErrVertexNotFound    = errors.New("ograph: vertex not found")
	ErrEdgeNotFound      = errors.New("ograph: edge not found")
	ErrEdgeExists        = errors.New("ograph: edge id already bound to a different identity")
	ErrLabelConflict     = errors.New("ograph: vertex exists with a different label")
	ErrIndexDirty        = errors.New("ograph: adjacency index is dirty; reopen required")
	ErrInvalidIdentity   = errors.New("ograph: vertex or edge id must not be empty")
	ErrDuplicateIdentity = errors.New("ograph: identity duplicated within a batch")
	ErrGraphClosed       = errors.New("ograph: graph handle closed")
	ErrGraphAlreadyOpen  = errors.New("ograph: another graph handle is open on this database")
)
