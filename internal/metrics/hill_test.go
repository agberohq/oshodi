package metrics

import (
	"strconv"
	"testing"
)

func TestNewHyperLogLog(t *testing.T) {
	hll := NewHyperLogLog(14)
	if hll == nil {
		t.Fatal("HLL is nil")
	}
	if hll.m != 16384 {
		t.Errorf("expected m=16384, got %d", hll.m)
	}
	if hll.precision != 14 {
		t.Errorf("expected precision=14, got %d", hll.precision)
	}
}

func TestHyperLogLogAdd(t *testing.T) {
	hll := NewHyperLogLog(14)

	key := []byte("test-key")
	hll.Add(key)

	estimate := hll.Estimate()
	if estimate < 1 {
		t.Errorf("expected estimate >= 1, got %f", estimate)
	}
}

func TestHyperLogLogEstimate(t *testing.T) {
	hll := NewHyperLogLog(14)

	// Add unique keys
	for i := 0; i < 1000; i++ {
		key := []byte("key" + strconv.Itoa(i))
		hll.Add(key)
	}

	estimate := hll.Estimate()
	if estimate < 800 || estimate > 1200 {
		t.Errorf("estimate %f out of range (800-1200)", estimate)
	}
}

func TestHyperLogLogDuplicateKeys(t *testing.T) {
	hll := NewHyperLogLog(14)

	key := []byte("test-key")
	for i := 0; i < 100; i++ {
		hll.Add(key)
	}

	estimate := hll.Estimate()
	if estimate < 0.5 || estimate > 1.5 {
		t.Errorf("estimate for duplicates should be ~1, got %f", estimate)
	}
}

func TestHyperLogLogMerge(t *testing.T) {
	hll1 := NewHyperLogLog(14)
	hll2 := NewHyperLogLog(14)

	for i := 0; i < 500; i++ {
		key := []byte("key1-" + strconv.Itoa(i))
		hll1.Add(key)
	}

	for i := 0; i < 500; i++ {
		key := []byte("key2-" + strconv.Itoa(i))
		hll2.Add(key)
	}

	hll1.Merge(hll2)

	estimate := hll1.Estimate()
	if estimate < 800 || estimate > 1200 {
		t.Errorf("merged estimate %f out of range (800-1200)", estimate)
	}
}

func TestHyperLogLogReset(t *testing.T) {
	hll := NewHyperLogLog(14)

	for i := 0; i < 100; i++ {
		key := []byte("key" + strconv.Itoa(i))
		hll.Add(key)
	}

	estimate := hll.Estimate()
	if estimate < 1 {
		t.Errorf("estimate should be > 0, got %f", estimate)
	}

	hll.Reset()

	estimate = hll.Estimate()
	if estimate < 0 || estimate > 10 {
		t.Errorf("after reset estimate should be near 0, got %f", estimate)
	}
}

func TestHyperLogLogPrecision(t *testing.T) {
	// Test different precisions
	for _, precision := range []uint8{10, 12, 14, 16} {
		hll := NewHyperLogLog(precision)
		for i := 0; i < 10000; i++ {
			key := []byte("key" + strconv.Itoa(i))
			hll.Add(key)
		}
		estimate := hll.Estimate()
		if estimate < 8000 || estimate > 12000 {
			t.Errorf("precision %d: estimate %f out of range (8000-12000)", precision, estimate)
		}
	}
}

func BenchmarkHyperLogLogAdd(b *testing.B) {
	hll := NewHyperLogLog(14)
	key := []byte("test-key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hll.Add(key)
	}
}

func BenchmarkHyperLogLogEstimate(b *testing.B) {
	hll := NewHyperLogLog(14)
	for i := 0; i < 10000; i++ {
		key := []byte("key" + strconv.Itoa(i))
		hll.Add(key)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hll.Estimate()
	}
}
