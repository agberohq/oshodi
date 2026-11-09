package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
)

// Benchmarks for the three scan workloads: walking the index only, reading and
// verifying every value, and reading, verifying and decoding every value.
// Values are consumed through scanBenchSink so the compiler cannot elide the
// work.

type scanBenchDoc struct {
	TS   int64   `json:"ts"`
	Req  float64 `json:"requests_sec"`
	P99  float64 `json:"p99_ms"`
	Err  float64 `json:"error_rate"`
	Blob string  `json:"blob"`
}

var scanBenchSink uint64

func scanBenchValue(size int) []byte {
	blob := make([]byte, 0)
	if size > 96 {
		blob = make([]byte, size-96)
		for i := range blob {
			blob[i] = 'x'
		}
	}
	v, err := json.Marshal(scanBenchDoc{TS: 1, Req: 2, P99: 1.2, Err: 0.1, Blob: string(blob)})
	if err != nil {
		panic(err)
	}
	return v
}

func scanBenchDB(b *testing.B, size int) (*DB, *Bucket) {
	b.Helper()
	db, err := NewDB(&Config{FilePath: filepath.Join(b.TempDir(), "bench.db"), DisableWAL: true})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	bk := db.GetBucket("s")
	val := scanBenchValue(size)
	key := make([]byte, 8)
	for i := 0; i < 10000; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := bk.Set(key, val); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	return db, bk
}

func BenchmarkScanKeyOnly(b *testing.B) {
	for _, size := range []int{64, 1024, 8192} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			db, _ := scanBenchDB(b, size)
			idx := db.index.Load()
			var sink uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.RangeString(func(k string, _ int64) bool {
					sink += uint64(len(k))
					return true
				})
			}
			scanBenchSink = sink
		})
	}
}

func BenchmarkScanValues(b *testing.B) {
	for _, size := range []int{64, 1024, 8192} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			_, bk := scanBenchDB(b, size)
			var sink uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bk.ScanView(func(k, v []byte) bool {
					sink += uint64(len(k)) + uint64(v[0])
					return true
				}); err != nil {
					b.Fatalf("scan: %v", err)
				}
			}
			scanBenchSink = sink
		})
	}
}

func BenchmarkScanDecode(b *testing.B) {
	for _, size := range []int{64, 1024, 8192} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			_, bk := scanBenchDB(b, size)
			var sink uint64
			var decodeErr error
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var doc scanBenchDoc
				if err := bk.ScanView(func(_, v []byte) bool {
					if decodeErr = json.Unmarshal(v, &doc); decodeErr != nil {
						return false
					}
					sink += uint64(doc.TS)
					return true
				}); err != nil {
					b.Fatalf("scan: %v", err)
				}
				if decodeErr != nil {
					b.Fatalf("decode: %v", decodeErr)
				}
			}
			scanBenchSink = sink
		})
	}
}
