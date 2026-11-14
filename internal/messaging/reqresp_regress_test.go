package messaging

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// A handler stuck on a downstream call must observe the caller's timeout and
// exit, instead of leaking its goroutine forever.
func TestCallCancelsHandlerContext(t *testing.T) {
	rr := NewReqResp(50 * time.Millisecond)
	exited := make(chan struct{})
	rr.Register("stuck", func(ctx context.Context, req []byte) ([]byte, error) {
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	})

	if _, err := rr.Call(context.Background(), "stuck", nil); err == nil {
		t.Fatal("expected timeout")
	}

	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("handler goroutine did not observe cancellation")
	}
}

// Cancelling the parent context must abandon the call, not just the timeout.
func TestCallParentCancellation(t *testing.T) {
	rr := NewReqResp(time.Second)
	rr.Register("slow", func(ctx context.Context, req []byte) ([]byte, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return []byte("late"), nil
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	if _, err := rr.Call(ctx, "slow", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// A handler goroutine that finishes after an abandoned Call must not deliver
// its stale result to a later, unrelated caller.
func TestStaleResultNotDelivered(t *testing.T) {
	rr := NewReqResp(20 * time.Millisecond)

	release := make(chan struct{})
	rr.Register("late", func(ctx context.Context, req []byte) ([]byte, error) {
		<-release
		return []byte("stale"), nil
	})
	rr.Register("fresh", func(ctx context.Context, req []byte) ([]byte, error) {
		return []byte("fresh"), nil
	})

	if _, err := rr.Call(context.Background(), "late", nil); err == nil {
		t.Fatal("expected timeout")
	}
	close(release)
	time.Sleep(20 * time.Millisecond) // let the abandoned handler finish

	got, err := rr.Call(context.Background(), "fresh", nil)
	if err != nil {
		t.Fatalf("fresh call failed: %v", err)
	}
	if string(got) != "fresh" {
		t.Fatalf("stale result leaked across calls: %q", got)
	}
}

// The handler goroutine count must return to baseline after a timed-out
// call: no leak.
func TestNoGoroutineLeakOnTimeout(t *testing.T) {
	rr := NewReqResp(20 * time.Millisecond)
	rr.Register("stuck", func(ctx context.Context, req []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		if _, err := rr.Call(context.Background(), "stuck", nil); err == nil {
			t.Fatal("expected timeout")
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines leaked: before %d, after %d", before, n)
	}
}
