package vector

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestCosineDistance(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{1, 0, 0}
	d := CosineDistance(a, b)
	if d != 0 {
		t.Errorf("identical vectors: expected 0, got %f", d)
	}

	c := []float32{-1, 0, 0}
	d = CosineDistance(a, c)
	if d != 2 {
		t.Errorf("opposite vectors: expected 2, got %f", d)
	}

	zero := []float32{0, 0, 0}
	d = CosineDistance(a, zero)
	if d != 1 {
		t.Errorf("zero vector: expected 1, got %f", d)
	}

	// Non-unit magnitudes: cosine distance must be scale-invariant.
	scaled := []float32{2, 0, 0}
	d = CosineDistance(scaled, scaled)
	if d != 0 {
		t.Errorf("scaled identical vectors: expected 0, got %f", d)
	}

	d = CosineDistance(a, scaled)
	if d != 0 {
		t.Errorf("scaled parallel vectors: expected 0, got %f", d)
	}

	d = CosineDistance(scaled, []float32{-3, 0, 0})
	if d != 2 {
		t.Errorf("scaled opposite vectors: expected 2, got %f", d)
	}
}

func TestCosineSimilarity(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{1, 0, 0}
	s := CosineSimilarity(a, b)
	if s != 1 {
		t.Errorf("identical vectors: expected 1, got %f", s)
	}

	c := []float32{-1, 0, 0}
	s = CosineSimilarity(a, c)
	if s != -1 {
		t.Errorf("opposite vectors: expected -1, got %f", s)
	}

	d := []float32{0, 1, 0}
	s = CosineSimilarity(a, d)
	if s != 0 {
		t.Errorf("orthogonal vectors: expected 0, got %f", s)
	}

	// Non-unit magnitudes: cosine similarity must be scale-invariant.
	s = CosineSimilarity([]float32{2, 0, 0}, []float32{3, 0, 0})
	if s != 1 {
		t.Errorf("scaled parallel vectors: expected 1, got %f", s)
	}

	s = CosineSimilarity([]float32{2, 0, 0}, []float32{-3, 0, 0})
	if s != -1 {
		t.Errorf("scaled opposite vectors: expected -1, got %f", s)
	}
}

func TestEuclideanDistance(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{1, 2, 3}
	d := EuclideanDistance(a, b)
	if d != 0 {
		t.Errorf("identical vectors: expected 0, got %f", d)
	}

	c := []float32{4, 6, 8}
	d = EuclideanDistance(a, c)
	expected := float32(math.Sqrt(9 + 16 + 25))
	if d != expected {
		t.Errorf("expected %f, got %f", expected, d)
	}
}

func TestEncodeDecodeVector(t *testing.T) {
	original := []float32{0.1, 0.2, 0.3, 0.4, 0.5}
	encoded := encodeVectorRecord(original)

	decoded, err := decodeVectorRecord(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(decoded) != len(original) {
		t.Fatalf("length mismatch: expected %d, got %d", len(original), len(decoded))
	}

	for i := range original {
		if decoded[i] != original[i] {
			t.Errorf("index %d: expected %f, got %f", i, original[i], decoded[i])
		}
	}
}

func TestDecodeVectorCorrupt(t *testing.T) {
	corrupt := []byte{0xFF, 0xFF, 0, 0, 0, 0}
	_, err := decodeVectorRecord(corrupt)
	if err == nil {
		t.Fatal("expected error for corrupt record")
	}
}

func TestDecodeVectorTruncated(t *testing.T) {
	// Valid header but truncated data
	truncated := []byte{recMagic, recVersion, 0, 0, 0, 1, 0x3F} // claims 1 dim but only 3 bytes
	_, err := decodeVectorRecord(truncated)
	if err == nil {
		t.Fatal("expected error for truncated record")
	}
}

func TestEncodeDecodeEdge(t *testing.T) {
	src := "node1"
	dst := "node2"
	layer := 3

	encoded := encodeEdgeRecord(src, dst, layer)

	decodedSrc, decodedDst, decodedLayer, err := decodeEdgeRecord(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decodedSrc != src {
		t.Errorf("src mismatch: expected %s, got %s", src, decodedSrc)
	}
	if decodedDst != dst {
		t.Errorf("dst mismatch: expected %s, got %s", dst, decodedDst)
	}
	if decodedLayer != layer {
		t.Errorf("layer mismatch: expected %d, got %d", layer, decodedLayer)
	}
}

func TestDecodeEdgeHugeLength(t *testing.T) {
	// A varint length near 2^64 must be rejected, not wrap negative.
	encoded := []byte{recMagic, recVersion}
	encoded = binary.AppendUvarint(encoded, math.MaxUint64)
	if _, _, _, err := decodeEdgeRecord(encoded); err == nil {
		t.Fatal("expected error for huge field length")
	}
}

func TestDecodeVectorHugeDims(t *testing.T) {
	// Claims a huge dimension count; must be rejected without allocating.
	huge := []byte{recMagic, recVersion, 0xFF, 0xFF, 0xFF, 0xFF} // dims = 0xFFFFFFFF
	if _, err := decodeVectorRecord(huge); err == nil {
		t.Fatal("expected error for huge dims")
	}
}
