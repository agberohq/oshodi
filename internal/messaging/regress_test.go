package messaging

import (
	"testing"
	"time"

	"github.com/olekukonko/jack"
)

// Registering a nil handler must not store a value that panics on the first
// call from a background goroutine.
func TestRegisterNilHandler(t *testing.T) {
	rr := NewReqResp(50 * time.Millisecond)

	rr.Register("x", nil)
	if rr.Has("x") {
		t.Fatal("nil handler must not register")
	}
	if _, err := rr.Call("x", nil); err == nil {
		t.Fatal("expected error for unregistered handler")
	}
	if _, err := rr.CallDirect("x", nil); err == nil {
		t.Fatal("expected error for unregistered handler")
	}
}

// Subscribers registered without a callback must not turn every publish into
// a recovered panic.
func TestPublishNilSubscriber(t *testing.T) {
	pool := jack.NewPool(4)
	ps := NewPubSub(pool, nil, nil, false)

	ps.Subscribe("", "ch", nil)
	if _, err := ps.Publish("", "ch", []byte("payload")); err != nil {
		t.Fatalf("publish with nil subscriber: %v", err)
	}
}
