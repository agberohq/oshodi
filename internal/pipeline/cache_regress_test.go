package pipeline

import (
	"fmt"
	"testing"
)

// Cache misses must not create entries: on read-mostly workloads eviction
// only runs inside Set, so junk entries from misses grow without bound.
func TestCacheMissDoesNotInsertEntry(t *testing.T) {
	c := NewCache(10)

	for i := 0; i < 100; i++ {
		if _, ok := c.Get([]byte(fmt.Sprintf("missing-%d", i))); ok {
			t.Fatal("unexpected hit")
		}
		if ok, _ := c.View([]byte(fmt.Sprintf("vmiss-%d", i)), func([]byte) error { return nil }); ok {
			t.Fatal("unexpected view hit")
		}
	}

	if c.Len() != 0 {
		t.Fatalf("cache misses leaked %d junk entries", c.Len())
	}

	// A miss on a colliding hash slot must not evict or corrupt the real
	// entry either.
	c.Set([]byte("real"), []byte("value"))
	for i := 0; i < 10; i++ {
		c.Get([]byte(fmt.Sprintf("noise-%d", i)))
	}
	if v, ok := c.Get([]byte("real")); !ok || string(v) != "value" {
		t.Fatalf("real entry damaged by misses: %q, %v", v, ok)
	}
}
