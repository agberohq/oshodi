package messaging

import (
	"fmt"
	"sync"
	"time"

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
	New: func() any { return time.NewTimer(time.Hour) },
}

// ReqResp manages named request/response handlers using a sharded map.
type ReqResp struct {
	handlers *mappo.Sharded[string, HandlerFunc]
	timeout  time.Duration
}

// NewReqResp creates a new ReqResp instance.
func NewReqResp(timeout time.Duration) *ReqResp {
	if timeout == 0 {
		timeout = 5 * time.Second
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

// Call invokes the named handler with timeout protection.
// Uses pooled channels and timers to eliminate per-call allocations.
func (r *ReqResp) Call(name string, payload []byte) ([]byte, error) {
	fn, ok := r.handlers.Get(name)
	if !ok {
		return nil, fmt.Errorf("no handler registered for %q", name)
	}

	ch := resultPool.Get().(chan callResult)
	defer resultPool.Put(ch)

	go func() {
		data, err := fn(payload)
		select {
		case ch <- callResult{data: data, err: err}:
		default:
		}
	}()

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
		return res.data, res.err
	case <-timer.C:
		// The firing was consumed by this select, so the timer is drained.
		timerPool.Put(timer)
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
