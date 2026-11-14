package messaging

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/agberohq/oshodi/internal/consts"
	"github.com/olekukonko/mappo"
)

// HandlerFunc is the signature every REQ/RESP handler must satisfy. The
// context is cancelled when the caller stops waiting (timeout or parent
// cancellation), so a handler stuck on a downstream call can observe the
// abandonment and exit instead of leaking its goroutine.
type HandlerFunc func(ctx context.Context, req []byte) ([]byte, error)

// callResult holds the outcome of a single Call invocation so the channel
// carries a single typed value instead of an anonymous struct literal.
type callResult struct {
	data []byte
	err  error
}

// resultPool avoids allocating a buffered channel on every Call.
var resultPool = sync.Pool{
	New: func() any { return make(chan callResult, 1) },
}

// ReqResp manages named request/response handlers using a sharded map.
type ReqResp struct {
	handlers *mappo.Sharded[string, HandlerFunc]
	timeout  time.Duration
}

// NewReqResp creates a new ReqResp instance.
func NewReqResp(timeout time.Duration) *ReqResp {
	if timeout == 0 {
		timeout = consts.ReqRespDefaultTimeout
	}
	return &ReqResp{
		handlers: mappo.NewSharded[string, HandlerFunc](),
		timeout:  timeout,
	}
}

// Register binds name → handler. A nil handler removes any existing
// registration instead of storing a value that would panic on call.
func (r *ReqResp) Register(name string, fn HandlerFunc) {
	if fn == nil {
		r.handlers.Delete(name)
		return
	}
	r.handlers.Set(name, fn)
}

// Unregister removes the named handler.
func (r *ReqResp) Unregister(name string) {
	r.handlers.Delete(name)
}

// Has returns true if a handler exists.
func (r *ReqResp) Has(name string) bool {
	_, ok := r.handlers.Get(name)
	return ok
}

// Handlers returns a snapshot of all registered handler names.
func (r *ReqResp) Handlers() []string {
	return r.handlers.Keys()
}

// runHandler is a package-level goroutine body: passing the values as
// arguments avoids the heap-allocated closure a `go func(){...}()` would
// capture on every Call.
func runHandler(ctx context.Context, fn HandlerFunc, payload []byte, ch chan callResult) {
	data, err := fn(ctx, payload)
	select {
	case ch <- callResult{data: data, err: err}:
	default:
	}
}

// Call invokes the named handler with timeout protection. It runs the handler
// in its own goroutine so a blocking handler cannot block the caller past the
// timeout; the handler's context is cancelled when the caller stops waiting,
// whether by timeout or parent cancellation. Use CallDirect for handlers known
// to return promptly, as it avoids the goroutine and channel round trip.
func (r *ReqResp) Call(ctx context.Context, name string, payload []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	fn, ok := r.handlers.Get(name)
	if !ok || fn == nil {
		return nil, fmt.Errorf("no handler registered for %q", name)
	}

	ch := resultPool.Get().(chan callResult)

	// The handler observes callCtx, so a stuck handler exits once the
	// caller stops waiting instead of leaking its goroutine.
	callCtx, cancel := context.WithTimeout(ctx, r.timeout)
	go runHandler(callCtx, fn, payload, ch)

	select {
	case res := <-ch:
		cancel()
		// The handler has sent and returned; the channel is safe to reuse.
		resultPool.Put(ch)
		return res.data, res.err
	case <-callCtx.Done():
		cancel()
		// The handler may still be running and will send its result later.
		// Never recycle ch here: reusing it would deliver that stale result
		// to an unrelated caller. Dropping it lets the buffered send finish
		// and the channel be collected with the handler.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("call to %q timed out", name)
	}
}

// CallDirect invokes the handler in the current goroutine.
func (r *ReqResp) CallDirect(ctx context.Context, name string, payload []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	fn, ok := r.handlers.Get(name)
	if !ok || fn == nil {
		return nil, fmt.Errorf("no handler registered for %q", name)
	}
	return fn(ctx, payload)
}
