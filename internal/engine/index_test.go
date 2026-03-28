package engine

import (
	"sync"
	"testing"
)

func TestNewIndex(t *testing.T) {
	idx := NewIndex(1000000, 0.01)
	if idx == nil {
		t.Fatal("index is nil")
	}
	if idx.shards == nil {
		t.Fatal("shards is nil")
	}
	if idx.bloom == nil {
		t.Fatal("bloom filter is nil")
	}
}

func TestIndexSetAndGet(t *testing.T) {
	idx := NewIndex(1000000, 0.01)

	key := []byte("test-key")
	offset := int64(12345)

	idx.Set(key, offset)

	got, ok := idx.Get(key)
	if !ok {
		t.Fatal("key not found")
	}
	if got != offset {
		t.Errorf("expected %d, got %d", offset, got)
	}
}

func TestIndexDelete(t *testing.T) {
	idx := NewIndex(1000000, 0.01)

	key := []byte("test-key")
	offset := int64(12345)

	idx.Set(key, offset)

	idx.Delete(key)

	_, ok := idx.Get(key)
	if ok {
		t.Error("key should not exist after delete")
	}
}

func TestIndexMaybeHas(t *testing.T) {
	idx := NewIndex(1000000, 0.01)

	key := []byte("test-key")
	offset := int64(12345)

	// Initially should return false
	if idx.MaybeHas(key) {
		t.Error("MaybeHas should return false for non-existent key")
	}

	idx.Set(key, offset)

	// After set should return true
	if !idx.MaybeHas(key) {
		t.Error("MaybeHas should return true for existing key")
	}
}

func TestIndexLen(t *testing.T) {
	idx := NewIndex(1000000, 0.01)

	if idx.Len() != 0 {
		t.Errorf("expected 0, got %d", idx.Len())
	}

	idx.Set([]byte("key1"), 1)
	idx.Set([]byte("key2"), 2)
	idx.Set([]byte("key3"), 3)

	if idx.Len() != 3 {
		t.Errorf("expected 3, got %d", idx.Len())
	}

	idx.Delete([]byte("key2"))

	if idx.Len() != 2 {
		t.Errorf("expected 2, got %d", idx.Len())
	}
}

func TestIndexBloomInsertions(t *testing.T) {
	idx := NewIndex(1000000, 0.01)

	if idx.BloomInsertions() != 0 {
		t.Errorf("expected 0 insertions, got %d", idx.BloomInsertions())
	}

	idx.Set([]byte("key1"), 1)
	idx.Set([]byte("key2"), 2)

	if idx.BloomInsertions() != 2 {
		t.Errorf("expected 2 insertions, got %d", idx.BloomInsertions())
	}
}

func TestIndexBloomFPR(t *testing.T) {
	idx := NewIndex(1000000, 0.01)

	fpr := idx.BloomFPR()
	if fpr < 0 || fpr > 1 {
		t.Errorf("invalid FPR: %f", fpr)
	}

	// Add many keys to get a realistic FPR
	for i := 0; i < 10000; i++ {
		key := []byte("key" + string(rune(i)))
		idx.Set(key, int64(i))
	}

	fpr = idx.BloomFPR()
	if fpr < 0 || fpr > 0.1 {
		t.Errorf("FPR after many inserts: %f", fpr)
	}
}

func TestIndexRange(t *testing.T) {
	idx := NewIndex(1000000, 0.01)

	testData := map[string]int64{
		"key1": 100,
		"key2": 200,
		"key3": 300,
	}

	for k, v := range testData {
		idx.Set([]byte(k), v)
	}

	count := 0
	idx.Range(func(key []byte, offset int64) bool {
		expected, ok := testData[string(key)]
		if !ok {
			t.Errorf("unexpected key: %s", key)
		}
		if offset != expected {
			t.Errorf("key %s: expected offset %d, got %d", key, expected, offset)
		}
		count++
		return true
	})

	if count != len(testData) {
		t.Errorf("expected %d items, got %d", len(testData), count)
	}
}

func TestIndexRangeStop(t *testing.T) {
	idx := NewIndex(1000000, 0.01)

	for i := 0; i < 10; i++ {
		key := []byte("key" + string(rune(i)))
		idx.Set(key, int64(i))
	}

	count := 0
	idx.Range(func(key []byte, offset int64) bool {
		count++
		return count < 5
	})

	if count != 5 {
		t.Errorf("expected 5 items, got %d", count)
	}
}

func TestIndexConcurrentOperations(t *testing.T) {
	idx := NewIndex(1000000, 0.01)

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := []byte("key" + string(rune(id)) + "-" + string(rune(j)))
				idx.Set(key, int64(id*j))
			}
		}(i)
	}

	wg.Wait()

	expected := numGoroutines * numOps
	if idx.Len() != expected {
		t.Errorf("expected %d keys, got %d", expected, idx.Len())
	}
}

func BenchmarkIndexSet(b *testing.B) {
	idx := NewIndex(1000000, 0.01)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + string(rune(i%10000)))
		idx.Set(key, int64(i))
	}
}

func BenchmarkIndexGet(b *testing.B) {
	idx := NewIndex(1000000, 0.01)

	for i := 0; i < 10000; i++ {
		key := []byte("key" + string(rune(i)))
		idx.Set(key, int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + string(rune(i%10000)))
		idx.Get(key)
	}
}
