// internal/metrics/sketch_test.go
package metrics

import (
	"strconv"
	"testing"
)

func TestNewCountMinSketch(t *testing.T) {
	cms := NewCountMinSketch(0.01, 0.99)
	if cms == nil {
		t.Fatal("CMS is nil")
	}
	if cms.rows == 0 {
		t.Error("rows should be > 0")
	}
	if cms.cols == 0 {
		t.Error("cols should be > 0")
	}
	if len(cms.matrix) != int(cms.rows*cms.cols) {
		t.Error("matrix size mismatch")
	}
}

func TestCountMinSketchAddAndEstimate(t *testing.T) {
	cms := NewCountMinSketch(0.01, 0.99)

	key := []byte("test-key")

	estimate := cms.Estimate(key)
	if estimate != 0 {
		t.Errorf("expected 0, got %d", estimate)
	}

	cms.Add(key)
	cms.Add(key)

	estimate = cms.Estimate(key)
	if estimate < 2 {
		t.Errorf("expected >= 2, got %d", estimate)
	}
}

func TestCountMinSketchMultipleKeys(t *testing.T) {
	cms := NewCountMinSketch(0.01, 0.99)

	keys := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = []byte("key" + strconv.Itoa(i))
	}

	// Add each key multiple times
	for i := 0; i < 10; i++ {
		for j := 0; j <= i; j++ {
			cms.Add(keys[i])
		}
	}

	// Check estimates
	for i := 0; i < 10; i++ {
		estimate := cms.Estimate(keys[i])
		if estimate < uint64(i+1) {
			t.Errorf("key %d: expected >= %d, got %d", i, i+1, estimate)
		}
	}
}

func TestCountMinSketchMerge(t *testing.T) {
	cms1 := NewCountMinSketch(0.01, 0.99)
	cms2 := NewCountMinSketch(0.01, 0.99)

	key1 := []byte("key1")
	key2 := []byte("key2")

	cms1.Add(key1)
	cms1.Add(key1)

	cms2.Add(key2)
	cms2.Add(key2)

	cms1.Merge(cms2)

	estimate1 := cms1.Estimate(key1)
	estimate2 := cms1.Estimate(key2)

	if estimate1 < 2 {
		t.Errorf("expected key1 estimate >= 2, got %d", estimate1)
	}
	if estimate2 < 2 {
		t.Errorf("expected key2 estimate >= 2, got %d", estimate2)
	}
}

func TestCountMinSketchLargeValues(t *testing.T) {
	cms := NewCountMinSketch(0.01, 0.99)

	key := []byte("test-key")
	iterations := 10000

	for i := 0; i < iterations; i++ {
		cms.Add(key)
	}

	estimate := cms.Estimate(key)
	if estimate < uint64(iterations) {
		t.Errorf("expected estimate >= %d, got %d", iterations, estimate)
	}
}

func TestCountMinSketchAccuracy(t *testing.T) {
	cms := NewCountMinSketch(0.01, 0.99)

	key := []byte("test-key")
	actualCount := 5000

	for i := 0; i < actualCount; i++ {
		cms.Add(key)
	}

	estimate := cms.Estimate(key)
	errorRate := float64(estimate-uint64(actualCount)) / float64(actualCount)

	if errorRate > 0.05 { // Allow 5% error
		t.Errorf("error rate too high: %f%%", errorRate*100)
	}
}

func BenchmarkCountMinSketchAdd(b *testing.B) {
	cms := NewCountMinSketch(0.01, 0.99)
	key := []byte("test-key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cms.Add(key)
	}
}

func BenchmarkCountMinSketchEstimate(b *testing.B) {
	cms := NewCountMinSketch(0.01, 0.99)
	key := []byte("test-key")
	cms.Add(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cms.Estimate(key)
	}
}
