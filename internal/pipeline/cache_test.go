package pipeline

import (
	"bytes"
	"sync"
	"testing"
)

func TestNewCache(t *testing.T) {
	cache := NewCache(10000)
	if cache == nil {
		t.Fatal("cache is nil")
	}
	if cache.data == nil {
		t.Fatal("data is nil")
	}
}

func TestCacheSetAndGet(t *testing.T) {
	cache := NewCache(10000)

	key := []byte("test-key")
	value := []byte("test-value")

	cache.Set(key, value)

	got, ok := cache.Get(key)
	if !ok {
		t.Fatal("key not found")
	}
	if !bytes.Equal(got, value) {
		t.Errorf("expected %s, got %s", value, got)
	}
}

func TestCacheGetNotFound(t *testing.T) {
	cache := NewCache(10000)

	_, ok := cache.Get([]byte("nonexistent"))
	if ok {
		t.Error("should not find nonexistent key")
	}
}

func TestCacheDelete(t *testing.T) {
	cache := NewCache(10000)

	key := []byte("test-key")
	value := []byte("test-value")

	cache.Set(key, value)
	cache.Delete(key)

	_, ok := cache.Get(key)
	if ok {
		t.Error("key should not exist after delete")
	}
}

func TestCacheLen(t *testing.T) {
	cache := NewCache(10000)

	if cache.Len() != 0 {
		t.Errorf("expected 0, got %d", cache.Len())
	}

	cache.Set([]byte("key1"), []byte("value1"))
	cache.Set([]byte("key2"), []byte("value2"))
	cache.Set([]byte("key3"), []byte("value3"))

	if cache.Len() != 3 {
		t.Errorf("expected 3, got %d", cache.Len())
	}
}

func TestCacheClear(t *testing.T) {
	cache := NewCache(10000)

	cache.Set([]byte("key1"), []byte("value1"))
	cache.Set([]byte("key2"), []byte("value2"))

	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("expected 0 after clear, got %d", cache.Len())
	}
}

func TestCacheStats(t *testing.T) {
	cache := NewCache(10000)

	stats := cache.Stats()
	if stats.Size < 0 {
		t.Errorf("stats.Size should be >= 0, got %d", stats.Size)
	}
}

func TestCacheClose(t *testing.T) {
	cache := NewCache(10000)
	if err := cache.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	cache := NewCache(10000)

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := []byte("key" + string(rune(id)) + "-" + string(rune(j)))
				value := []byte("value" + string(rune(id)) + "-" + string(rune(j)))
				cache.Set(key, value)
				got, ok := cache.Get(key)
				if !ok {
					t.Errorf("key not found: %s", key)
				}
				if !bytes.Equal(got, value) {
					t.Errorf("value mismatch for key %s", key)
				}
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkCacheSet(b *testing.B) {
	cache := NewCache(10000)
	key := []byte("test-key")
	value := []byte("test-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(key, value)
	}
}

func BenchmarkCacheGet(b *testing.B) {
	cache := NewCache(10000)
	key := []byte("test-key")
	value := []byte("test-value")
	cache.Set(key, value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(key)
	}
}
