package messaging

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olekukonko/jack"
)

func TestNewPubSub(t *testing.T) {
	pool := jack.NewPool(10)
	ps := NewPubSub(pool, nil, nil)
	if ps == nil {
		t.Fatal("pubsub is nil")
	}
	if ps.channels == nil {
		t.Fatal("channels is nil")
	}
}

func TestPubSubSubscribeAndPublish(t *testing.T) {
	pool := jack.NewPool(10)
	ps := NewPubSub(pool, nil, nil)

	channel := "test-channel"
	received := make(chan []byte, 1)

	_, unsub := ps.Subscribe("", channel, func(payload []byte) {
		received <- payload
	})
	defer unsub()

	payload := []byte("test-message")
	count, err := ps.Publish("", channel, payload)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 subscriber, got %d", count)
	}

	select {
	case msg := <-received:
		if !bytes.Equal(msg, payload) {
			t.Errorf("expected %s, got %s", payload, msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestPubSubMultipleSubscribers(t *testing.T) {
	pool := jack.NewPool(10)
	ps := NewPubSub(pool, nil, nil)

	channel := "test-channel"
	var wg sync.WaitGroup
	var receivedCount atomic.Int32

	for i := 0; i < 5; i++ {
		wg.Add(1)
		_, unsub := ps.Subscribe("", channel, func(payload []byte) {
			receivedCount.Add(1)
			wg.Done()
		})
		defer unsub()
	}

	payload := []byte("test-message")
	count, err := ps.Publish("", channel, payload)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if count != 5 {
		t.Errorf("expected 5 subscribers, got %d", count)
	}

	// Wait for all subscribers to receive
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if receivedCount.Load() != 5 {
			t.Errorf("expected 5 messages received, got %d", receivedCount.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for messages")
	}
}

func TestPubSubUnsubscribe(t *testing.T) {
	pool := jack.NewPool(10)
	ps := NewPubSub(pool, nil, nil)

	channel := "test-channel"
	var receivedCount int32

	_, unsub := ps.Subscribe("", channel, func(payload []byte) {
		atomic.AddInt32(&receivedCount, 1)
	})

	unsub()

	payload := []byte("test-message")
	count, err := ps.Publish("", channel, payload)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if count != 0 {
		t.Errorf("expected 0 subscribers, got %d", count)
	}

	// Give time for any potential messages
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&receivedCount) != 0 {
		t.Errorf("expected 0 messages, got %d", receivedCount)
	}
}

func TestPubSubUnsubscribeAll(t *testing.T) {
	pool := jack.NewPool(10)
	ps := NewPubSub(pool, nil, nil)

	channel := "test-channel"
	var receivedCount int32

	for i := 0; i < 3; i++ {
		ps.SubscribeSimple("", channel, func(payload []byte) {
			atomic.AddInt32(&receivedCount, 1)
		})
	}

	ps.UnsubscribeAll("", channel)

	payload := []byte("test-message")
	count, err := ps.Publish("", channel, payload)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if count != 0 {
		t.Errorf("expected 0 subscribers, got %d", count)
	}

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&receivedCount) != 0 {
		t.Errorf("expected 0 messages, got %d", receivedCount)
	}
}

func TestPubSubCount(t *testing.T) {
	pool := jack.NewPool(10)
	ps := NewPubSub(pool, nil, nil)

	channel := "test-channel"

	if ps.Count("", channel) != 0 {
		t.Error("expected 0 subscribers")
	}

	_, unsub1 := ps.Subscribe("", channel, nil)
	_, unsub2 := ps.Subscribe("", channel, nil)
	defer unsub1()
	defer unsub2()

	if ps.Count("", channel) != 2 {
		t.Errorf("expected 2 subscribers, got %d", ps.Count("", channel))
	}
}

// internal/messaging/pubsub_test.go - Fix the IDs test
func TestPubSubIDs(t *testing.T) {
	pool := jack.NewPool(10)
	ps := NewPubSub(pool, nil, nil)

	channel := "test-channel"

	ids := ps.IDs("", channel)
	if len(ids) != 0 {
		t.Error("expected empty IDs")
	}

	_, unsub1 := ps.Subscribe("", channel, nil)
	_, unsub2 := ps.Subscribe("", channel, nil)
	defer unsub1()
	defer unsub2()

	ids = ps.IDs("", channel)
	if len(ids) != 2 {
		t.Errorf("expected 2 IDs, got %d", len(ids))
	}
}

func TestPubSubDifferentBuckets(t *testing.T) {
	pool := jack.NewPool(10)
	ps := NewPubSub(pool, nil, nil)

	bucket1 := "bucket1"
	bucket2 := "bucket2"
	channel := "channel"

	var received1, received2 int32

	ps.SubscribeSimple(bucket1, channel, func(payload []byte) {
		atomic.AddInt32(&received1, 1)
	})
	ps.SubscribeSimple(bucket2, channel, func(payload []byte) {
		atomic.AddInt32(&received2, 1)
	})

	payload := []byte("test")
	ps.Publish(bucket1, channel, payload)
	ps.Publish(bucket2, channel, payload)

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&received1) != 1 {
		t.Errorf("bucket1 expected 1 message, got %d", received1)
	}
	if atomic.LoadInt32(&received2) != 1 {
		t.Errorf("bucket2 expected 1 message, got %d", received2)
	}
}

func TestPubSubPanicRecovery(t *testing.T) {
	pool := jack.NewPool(10)
	ps := NewPubSub(pool, nil, nil)

	channel := "test-channel"

	ps.SubscribeSimple("", channel, func(payload []byte) {
		panic("test panic")
	})

	payload := []byte("test")
	_, err := ps.Publish("", channel, payload)
	if err != nil {
		t.Fatalf("publish should not return error on panic: %v", err)
	}

	// Give time for panic to be recovered
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkPubSubPublish(b *testing.B) {
	pool := jack.NewPool(100)
	ps := NewPubSub(pool, nil, nil)

	channel := "test-channel"
	payload := []byte("test")

	for i := 0; i < 100; i++ {
		ps.SubscribeSimple("", channel, func(p []byte) {})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.Publish("", channel, payload)
	}
}
