package messaging

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewReqResp(t *testing.T) {
	rr := NewReqResp(5 * time.Second)
	if rr == nil {
		t.Fatal("reqresp is nil")
	}
	if rr.handlers == nil {
		t.Fatal("handlers is nil")
	}
}

func TestReqRespRegisterAndCall(t *testing.T) {
	rr := NewReqResp(5 * time.Second)

	name := "test-handler"
	expected := []byte("response")

	rr.Register(name, func(ctx context.Context, req []byte) ([]byte, error) {
		return expected, nil
	})

	resp, err := rr.Call(context.Background(), name, []byte("request"))
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}
	if !bytes.Equal(resp, expected) {
		t.Errorf("expected %s, got %s", expected, resp)
	}
}

func TestReqRespCallWithError(t *testing.T) {
	rr := NewReqResp(5 * time.Second)

	name := "error-handler"
	expectedErr := errors.New("handler error")

	rr.Register(name, func(ctx context.Context, req []byte) ([]byte, error) {
		return nil, expectedErr
	})

	_, err := rr.Call(context.Background(), name, []byte("request"))
	if err != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestReqRespCallNotFound(t *testing.T) {
	rr := NewReqResp(5 * time.Second)

	_, err := rr.Call(context.Background(), "nonexistent", []byte("request"))
	if err == nil {
		t.Error("expected error for non-existent handler")
	}
}

func TestReqRespUnregister(t *testing.T) {
	rr := NewReqResp(5 * time.Second)

	name := "test-handler"
	rr.Register(name, func(ctx context.Context, req []byte) ([]byte, error) {
		return nil, nil
	})

	if !rr.Has(name) {
		t.Error("handler should exist")
	}

	rr.Unregister(name)

	if rr.Has(name) {
		t.Error("handler should not exist after unregister")
	}
}

func TestReqRespCallDirect(t *testing.T) {
	rr := NewReqResp(5 * time.Second)

	name := "direct-handler"
	expected := []byte("direct-response")

	rr.Register(name, func(ctx context.Context, req []byte) ([]byte, error) {
		return expected, nil
	})

	resp, err := rr.CallDirect(context.Background(), name, []byte("request"))
	if err != nil {
		t.Fatalf("call direct failed: %v", err)
	}
	if !bytes.Equal(resp, expected) {
		t.Errorf("expected %s, got %s", expected, resp)
	}
}

func TestReqRespTimeout(t *testing.T) {
	rr := NewReqResp(100 * time.Millisecond)

	name := "slow-handler"
	rr.Register(name, func(ctx context.Context, req []byte) ([]byte, error) {
		time.Sleep(200 * time.Millisecond)
		return []byte("response"), nil
	})

	_, err := rr.Call(context.Background(), name, []byte("request"))
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestReqRespHandlersList(t *testing.T) {
	rr := NewReqResp(5 * time.Second)

	handlers := []string{"handler1", "handler2", "handler3"}
	for _, h := range handlers {
		rr.Register(h, func(ctx context.Context, req []byte) ([]byte, error) {
			return nil, nil
		})
	}

	list := rr.Handlers()
	if len(list) != len(handlers) {
		t.Errorf("expected %d handlers, got %d", len(handlers), len(list))
	}
}

func TestReqRespConcurrentRegistration(t *testing.T) {
	rr := NewReqResp(5 * time.Second)

	var wg sync.WaitGroup
	numGoroutines := 10
	numHandlers := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numHandlers; j++ {
				name := "handler" + string(rune(id)) + "-" + string(rune(j))
				rr.Register(name, func(ctx context.Context, req []byte) ([]byte, error) {
					return nil, nil
				})
			}
		}(i)
	}

	wg.Wait()

	if rr.Handlers() == nil {
		t.Error("handlers should not be nil")
	}
}

func BenchmarkReqRespCall(b *testing.B) {
	rr := NewReqResp(5 * time.Second)
	name := "bench-handler"
	rr.Register(name, func(ctx context.Context, req []byte) ([]byte, error) {
		return []byte("response"), nil
	})

	payload := []byte("request")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr.Call(context.Background(), name, payload)
	}
}

func BenchmarkReqRespCallDirect(b *testing.B) {
	rr := NewReqResp(5 * time.Second)
	name := "bench-handler"
	rr.Register(name, func(ctx context.Context, req []byte) ([]byte, error) {
		return []byte("response"), nil
	})

	payload := []byte("request")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr.CallDirect(context.Background(), name, payload)
	}
}

// TestCallTimeoutDoesNotDeliverStaleResult pins the pooled-channel bug: after
// a timeout the handler goroutine may still send, and a recycled channel would
// deliver that stale result to the next caller.
func TestCallTimeoutDoesNotDeliverStaleResult(t *testing.T) {
	rr := NewReqResp(50 * time.Millisecond)

	var slowOnce, fastOnce sync.Once
	releaseSlow := make(chan struct{})
	releaseFast := make(chan struct{})
	defer slowOnce.Do(func() { close(releaseSlow) })
	defer fastOnce.Do(func() { close(releaseFast) })

	rr.Register("slow", func(ctx context.Context, req []byte) ([]byte, error) {
		<-releaseSlow
		return []byte("slow"), nil
	})
	if _, err := rr.Call(context.Background(), "slow", nil); err == nil {
		t.Fatal("slow call did not time out")
	}

	fastStarted := make(chan struct{})
	rr.Register("fast", func(ctx context.Context, req []byte) ([]byte, error) {
		close(fastStarted)
		<-releaseFast
		return []byte("fast"), nil
	})

	go func() {
		<-fastStarted
		slowOnce.Do(func() { close(releaseSlow) })
		time.Sleep(10 * time.Millisecond)
		fastOnce.Do(func() { close(releaseFast) })
	}()

	got, err := rr.Call(context.Background(), "fast", nil)
	if err != nil {
		t.Fatalf("fast call: %v", err)
	}
	if string(got) != "fast" {
		t.Fatalf("fast call got %q; a timed-out call's result was delivered", got)
	}
}
