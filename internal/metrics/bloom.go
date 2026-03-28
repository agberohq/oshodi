package metrics

import (
	"math"
	"sync/atomic"

	"github.com/zeebo/xxh3"
)

// BloomFilter provides fast approximate membership testing using atomic operations.
type BloomFilter struct {
	bits       []uint64
	k          uint32
	size       uint64
	insertions atomic.Uint64
}

// NewBloomFilter creates a new lock-free Bloom filter.
func NewBloomFilter(expectedItems uint, falsePositiveRate float64) *BloomFilter {
	if expectedItems == 0 {
		expectedItems = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}

	m := uint64(math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)))
	k := uint32(math.Ceil(math.Ln2 * float64(m) / float64(expectedItems)))

	if m < 1 {
		m = 1
	}
	if k < 1 {
		k = 1
	}

	numWords := (m + 63) / 64
	bits := make([]uint64, numWords)

	return &BloomFilter{
		bits: bits,
		k:    k,
		size: m,
	}
}

// Add inserts a key into the filter using lock-free CAS operations.
func (bf *BloomFilter) Add(key []byte) {
	if bf.size == 0 {
		return
	}

	for i := uint32(0); i < bf.k; i++ {
		seed := uint64(i)
		h := xxh3.HashSeed(key, seed) % bf.size
		word, bit := h/64, h%64
		mask := uint64(1) << bit

		// Lock-free Compare-And-Swap loop
		for {
			old := atomic.LoadUint64(&bf.bits[word])
			if old&mask != 0 {
				break
			}
			if atomic.CompareAndSwapUint64(&bf.bits[word], old, old|mask) {
				break
			}
		}
	}
	bf.insertions.Add(1)
}

// MaybeHas checks if a key might exist using atomic loads.
func (bf *BloomFilter) MaybeHas(key []byte) bool {
	if bf.size == 0 {
		return false
	}

	for i := uint32(0); i < bf.k; i++ {
		seed := uint64(i)
		h := xxh3.HashSeed(key, seed) % bf.size
		word, bit := h/64, h%64
		if atomic.LoadUint64(&bf.bits[word])&(uint64(1)<<bit) == 0 {
			return false
		}
	}
	return true
}

// EstimatedFPR calculates the current false positive rate dynamically.
func (bf *BloomFilter) EstimatedFPR() float64 {
	if bf.size == 0 {
		return 1.0
	}
	n := float64(bf.insertions.Load())
	m := float64(bf.size)
	k := float64(bf.k)
	return math.Pow(1.0-math.Exp(-k*n/m), k)
}

// Insertions returns the number of items added (atomic read).
func (bf *BloomFilter) Insertions() uint64 {
	return bf.insertions.Load()
}

// Reset clears all bits safely.
func (bf *BloomFilter) Reset() {
	for i := range bf.bits {
		atomic.StoreUint64(&bf.bits[i], 0)
	}
	bf.insertions.Store(0)
}
