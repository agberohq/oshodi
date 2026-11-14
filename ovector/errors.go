package ovector

import "errors"

var (
	ErrVectorNotFound    = errors.New("ovector: vector not found")
	ErrIndexDirty        = errors.New("ovector: adjacency index is dirty; reopen required")
	ErrInvalidIdentity   = errors.New("ovector: vector id must not be empty")
	ErrVectorClosed      = errors.New("ovector: vector handle closed")
	ErrVectorAlreadyOpen = errors.New("ovector: another vector handle is open on this database")
	ErrDimMismatch       = errors.New("ovector: embedding dimension mismatch")
)
