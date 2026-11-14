package bugs

import (
	"path/filepath"
	"testing"
	"time"

	oshodi "github.com/agberohq/oshodi"
)

// By default every subscriber must receive its own payload copy. With the
// shared default, one subscriber mutating the payload corrupted it for every
// other subscriber on the channel — silently and scheduling-dependent.
func TestPubSubPayloadCopiedByDefault(t *testing.T) {
	db, err := oshodi.Open(filepath.Join(t.TempDir(), "pubsub.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := mustBucket(t, db, "b")

	mutated := make(chan struct{})
	seen := make(chan []byte, 1)

	unsub1 := b.Subscribe("ch", func(p []byte) {
		p[0] = 'X'
		close(mutated)
	})
	defer unsub1()
	unsub2 := b.Subscribe("ch", func(p []byte) {
		<-mutated
		seen <- append([]byte(nil), p...)
	})
	defer unsub2()

	if _, err := b.Publish("ch", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case p := <-seen:
		if string(p) != "hello" {
			t.Fatalf("payload mutated across subscribers: %q", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second subscriber")
	}
}
