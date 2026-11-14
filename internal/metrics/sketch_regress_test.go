package metrics

import "testing"

// The depth parameter is a confidence: 99% confidence means five rows. One
// row removes the min-over-rows bound entirely and over-estimates on every
// colliding key.
func TestCountMinSketchRowsMatchConfidence(t *testing.T) {
	if cms := NewCountMinSketch(0.01, 0.99); cms.rows != 5 {
		t.Fatalf("expected 5 rows for 99%% confidence, got %d", cms.rows)
	}
}
