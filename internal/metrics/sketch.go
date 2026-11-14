package metrics

import (
	"math"
	"sync/atomic"

	"github.com/agberohq/oshodi/internal/consts"
	"github.com/zeebo/xxh3"
)

// CountMinSketch for high-speed frequency estimation.
type CountMinSketch struct {
	matrix []uint64 // Use regular uint64 with atomic operations
	rows   uint32
	cols   uint32
	mask   uint64
}

// NewCountMinSketch creates an optimized, lock-free Count-Min Sketch.
// epsilon bounds the relative over-estimation; confidence is the probability
// that an estimate stays within that bound (rows = ceil(ln(1/(1-conf)))).
func NewCountMinSketch(epsilon, confidence float64) *CountMinSketch {
	if epsilon <= 0 || epsilon >= math.E {
		epsilon = consts.CMSDefaultEpsilon
	}
	if confidence <= 0 || confidence >= 1 {
		confidence = consts.CMSDefaultConfidence
	}
	rows := uint32(math.Ceil(math.Log(1.0 / (1.0 - confidence))))

	cols := uint32(math.Ceil(math.E / epsilon))
	cols--
	cols |= cols >> 1
	cols |= cols >> 2
	cols |= cols >> 4
	cols |= cols >> 8
	cols |= cols >> 16
	cols++

	return &CountMinSketch{
		matrix: make([]uint64, rows*cols),
		rows:   rows,
		cols:   cols,
		mask:   uint64(cols - 1),
	}
}

// Add lock-free increments the count for a key.
func (cms *CountMinSketch) Add(key []byte) {
	for i := uint32(0); i < cms.rows; i++ {
		colIdx := xxh3.HashSeed(key, uint64(i)) & cms.mask
		offset := i*cms.cols + uint32(colIdx)
		atomic.AddUint64(&cms.matrix[offset], 1)
	}
}

// Estimate returns estimated frequency of a key via atomic loads.
func (cms *CountMinSketch) Estimate(key []byte) uint64 {
	min := uint64(^uint64(0))
	for i := uint32(0); i < cms.rows; i++ {
		colIdx := xxh3.HashSeed(key, uint64(i)) & cms.mask
		offset := i*cms.cols + uint32(colIdx)
		val := atomic.LoadUint64(&cms.matrix[offset])
		if val < min {
			min = val
		}
	}
	return min
}

// Merge lock-free combines two sketches.
func (cms *CountMinSketch) Merge(other *CountMinSketch) {
	if cms.rows != other.rows || cms.cols != other.cols {
		return
	}
	totalSize := cms.rows * cms.cols
	for i := uint32(0); i < totalSize; i++ {
		val := atomic.LoadUint64(&other.matrix[i])
		if val > 0 {
			atomic.AddUint64(&cms.matrix[i], val)
		}
	}
}
