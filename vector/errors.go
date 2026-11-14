package vector

import "errors"

var (
	ErrVectorNotFound    = errors.New("vector: vector not found")
	ErrIndexDirty        = errors.New("vector: adjacency index is dirty; reopen required")
	ErrInvalidIdentity   = errors.New("vector: vector id must not be empty")
	ErrVectorClosed      = errors.New("vector: vector handle closed")
	ErrVectorAlreadyOpen = errors.New("vector: another vector handle is open on this database")
	ErrDimMismatch       = errors.New("vector: embedding dimension mismatch")
)
