package metrics

import (
	"math"
	"math/bits"
	"sync/atomic"

	"github.com/zeebo/xxh3"
)

// HyperLogLog for cardinality estimation.
type HyperLogLog struct {
	registers []uint32 // Use regular uint32 with atomic operations
	precision uint8
	m         uint64
	alpha     float64
}

// NewHyperLogLog creates a new lock-free HyperLogLog.
func NewHyperLogLog(precision uint8) *HyperLogLog {
	if precision < 4 {
		precision = 4
	}
	if precision > 16 {
		precision = 16
	}

	m := uint64(1) << precision

	var alpha float64
	switch m {
	case 16:
		alpha = 0.673
	case 32:
		alpha = 0.697
	case 64:
		alpha = 0.709
	default:
		alpha = 0.7213 / (1.0 + 1.079/float64(m))
	}

	return &HyperLogLog{
		registers: make([]uint32, m),
		precision: precision,
		m:         m,
		alpha:     alpha,
	}
}

// Add inserts a key lock-free.
func (hll *HyperLogLog) Add(key []byte) {
	hash := xxh3.Hash128(key)

	idx := hash.Lo & (hll.m - 1)
	val := uint32(bits.LeadingZeros64(hash.Hi)) + 1

	// Lock-free maximum update
	for {
		old := atomic.LoadUint32(&hll.registers[idx])
		if val <= old {
			break
		}
		if atomic.CompareAndSwapUint32(&hll.registers[idx], old, val) {
			break
		}
	}
}

// Estimate returns cardinality estimate using atomic reads.
func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	zeros := 0

	for i := uint64(0); i < hll.m; i++ {
		r := atomic.LoadUint32(&hll.registers[i])
		sum += math.Pow(2.0, -float64(r))
		if r == 0 {
			zeros++
		}
	}

	estimate := hll.alpha * float64(hll.m) * float64(hll.m) / sum

	if estimate <= 2.5*float64(hll.m) && zeros > 0 {
		estimate = float64(hll.m) * math.Log(float64(hll.m)/float64(zeros))
	}

	return estimate
}

// Merge lock-free combines two HLLs.
func (hll *HyperLogLog) Merge(other *HyperLogLog) {
	if hll.m != other.m {
		return
	}
	for i := uint64(0); i < hll.m; i++ {
		otherVal := atomic.LoadUint32(&other.registers[i])
		for {
			old := atomic.LoadUint32(&hll.registers[i])
			if otherVal <= old {
				break
			}
			if atomic.CompareAndSwapUint32(&hll.registers[i], old, otherVal) {
				break
			}
		}
	}
}

// Reset clears the HLL lock-free.
func (hll *HyperLogLog) Reset() {
	for i := uint64(0); i < hll.m; i++ {
		atomic.StoreUint32(&hll.registers[i], 0)
	}
}
