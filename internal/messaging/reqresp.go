package messaging

import (
	"fmt"
	"time"

	"github.com/olekukonko/mappo"
)

// HandlerFunc is the signature every REQ/RESP handler must satisfy.
type HandlerFunc func(req []byte) ([]byte, error)

// ReqResp manages named request/response handlers using a lock‑free map.
type ReqResp struct {
	handlers *mappo.Concurrent[string, HandlerFunc]
	timeout  time.Duration
}

// NewReqResp creates a new ReqResp instance.
func NewReqResp(timeout time.Duration) *ReqResp {
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &ReqResp{
		handlers: mappo.NewConcurrent[string, HandlerFunc](),
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
func (r *ReqResp) Call(name string, payload []byte) ([]byte, error) {
	fn, ok := r.handlers.Get(name)
	if !ok {
		return nil, fmt.Errorf("no handler registered for %q", name)
	}

	type result struct {
		data []byte
		err  error
	}

	ch := make(chan result, 1)
	go func() {
		data, err := fn(payload)
		ch <- result{data, err}
	}()

	select {
	case res := <-ch:
		return res.data, res.err
	case <-time.After(r.timeout):
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
