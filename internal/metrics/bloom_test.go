package metrics

import (
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	bf := NewBloomFilter(1000000, 0.01)
	if bf == nil {
		t.Fatal("bloom filter is nil")
	}
	if bf.size == 0 {
		t.Error("size should be > 0")
	}
	if bf.k == 0 {
		t.Error("k should be > 0")
	}
	if len(bf.bits) == 0 {
		t.Error("bits should not be empty")
	}
}

func TestBloomFilterAddAndMaybeHas(t *testing.T) {
	bf := NewBloomFilter(1000000, 0.01)

	key := []byte("test-key")

	if bf.MaybeHas(key) {
		t.Error("MaybeHas should return false for non-existent key")
	}

	bf.Add(key)

	if !bf.MaybeHas(key) {
		t.Error("MaybeHas should return true for added key")
	}
}

func TestBloomFilterInsertions(t *testing.T) {
	bf := NewBloomFilter(1000000, 0.01)

	if bf.Insertions() != 0 {
		t.Errorf("expected 0 insertions, got %d", bf.Insertions())
	}

	bf.Add([]byte("key1"))
	bf.Add([]byte("key2"))

	if bf.Insertions() != 2 {
		t.Errorf("expected 2 insertions, got %d", bf.Insertions())
	}
}

func TestBloomFilterEstimatedFPR(t *testing.T) {
	bf := NewBloomFilter(1000000, 0.01)

	fpr := bf.EstimatedFPR()
	if fpr < 0 || fpr > 1 {
		t.Errorf("invalid FPR: %f", fpr)
	}

	// Add many keys
	for i := 0; i < 100000; i++ {
		key := []byte("key" + string(rune(i)))
		bf.Add(key)
	}

	fpr = bf.EstimatedFPR()
	if fpr > 0.05 {
		t.Errorf("FPR too high: %f", fpr)
	}
}

func TestBloomFilterReset(t *testing.T) {
	bf := NewBloomFilter(1000000, 0.01)

	key := []byte("test-key")
	bf.Add(key)

	if !bf.MaybeHas(key) {
		t.Error("key should be present before reset")
	}

	bf.Reset()

	if bf.MaybeHas(key) {
		t.Error("key should not be present after reset")
	}

	if bf.Insertions() != 0 {
		t.Errorf("expected 0 insertions after reset, got %d", bf.Insertions())
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Add some keys
	for i := 0; i < 500; i++ {
		key := []byte("key" + string(rune(i)))
		bf.Add(key)
	}

	// Test for false positives
	falsePositives := 0
	tests := 10000
	for i := 500; i < 500+tests; i++ {
		key := []byte("key" + string(rune(i)))
		if bf.MaybeHas(key) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / float64(tests)
	if rate > 0.05 { // Allow some margin
		t.Errorf("false positive rate too high: %f", rate)
	}
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(1000000, 0.01)
	key := []byte("test-key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(key)
	}
}

func BenchmarkBloomFilterMaybeHas(b *testing.B) {
	bf := NewBloomFilter(1000000, 0.01)
	key := []byte("test-key")
	bf.Add(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MaybeHas(key)
	}
}
