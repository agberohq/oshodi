package messaging

import (
	"fmt"
	"sync"
	"time"

	"github.com/agberohq/oshodi/internal/consts"
	"github.com/olekukonko/mappo"
)

// HandlerFunc is the signature every REQ/RESP handler must satisfy.
type HandlerFunc func(req []byte) ([]byte, error)

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

// timerPool reuses time.Timer values to avoid per-call heap allocations.
var timerPool = sync.Pool{
	New: func() any { return time.NewTimer(consts.TimerIdle) },
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

// Register binds name → handler.
func (r *ReqResp) Register(name string, fn HandlerFunc) {
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
func runHandler(fn HandlerFunc, payload []byte, ch chan callResult) {
	data, err := fn(payload)
	select {
	case ch <- callResult{data: data, err: err}:
	default:
	}
}

// Call invokes the named handler with timeout protection. It runs the handler
// in its own goroutine so a blocking handler cannot block the caller past the
// timeout; use CallDirect for handlers known to return promptly, as it avoids
// the goroutine and channel round trip.
func (r *ReqResp) Call(name string, payload []byte) ([]byte, error) {
	fn, ok := r.handlers.Get(name)
	if !ok {
		return nil, fmt.Errorf("no handler registered for %q", name)
	}

	ch := resultPool.Get().(chan callResult)

	go runHandler(fn, payload, ch)

	// Reuse a pooled timer to avoid heap allocation.
	timer := timerPool.Get().(*time.Timer)
	timer.Reset(r.timeout)

	select {
	case res := <-ch:
		// A fired-but-undrained channel would deliver a stale firing to
		// the next pool user as an instant bogus timeout — drain or drop.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerPool.Put(timer)
		// The handler has sent and returned; the channel is safe to reuse.
		resultPool.Put(ch)
		return res.data, res.err
	case <-timer.C:
		// The firing was consumed by this select, so the timer is drained.
		timerPool.Put(timer)
		// Never recycle ch here: the handler may still be running and will
		// send its result later. Reusing it would deliver that stale result
		// to an unrelated caller. Dropping it lets the buffered send finish
		// and the channel be collected with the handler.
		return nil, fmt.Errorf("call to %q timed out", name)
	}
}

// CallDirect invokes the handler in the current goroutine.
func (r *ReqResp) CallDirect(name string, payload []byte) ([]byte, error) {
	fn, ok := r.handlers.Get(name)
	if !ok {
		return nil, fmt.Errorf("no handler registered for %q", name)
	}
	return fn(payload)
}
