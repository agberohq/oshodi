package pipeline

// HookContext carries contextual data into every hook call
type HookContext struct {
	Bucket string
	Key    []byte
	Value  []byte
}

// Hooks is a collection of interceptor functions that can be used to
// add middleware-like behavior to database operations.
type Hooks struct {
	OnWrite       func(ctx HookContext) ([]byte, error)
	OnRead        func(ctx HookContext) ([]byte, error)
	OnDelete      func(ctx HookContext) error
	OnPublish     func(bucket, channel string, payload []byte) ([]byte, error)
	OnSubscribe   func(bucket, channel string, subID int64)
	OnUnsubscribe func(bucket, channel string, subID int64)
}

// WriteRequest represents a single write operation.
// This is kept here as it's a "pipeline" concept, even though the batcher is gone.
// It could be used by other middleware or logging hooks.
type WriteRequest struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}
