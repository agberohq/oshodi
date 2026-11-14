package oshodi

import (
	"fmt"
	"path/filepath"
	"testing"
)

func benchOrderedBucket(b *testing.B, n int) *Bucket {
	b.Helper()
	db, err := Open(filepath.Join(b.TempDir(), "ordered-bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	bk := db.Bucket("b")
	entries := make([]KV, 0, 1000)
	for i := 0; i < n; i++ {
		entries = append(entries, KV{
			Key:   []byte(fmt.Sprintf("k%08d", i)),
			Value: []byte("value"),
		})
		if len(entries) == cap(entries) {
			if err := bk.BatchSet(entries); err != nil {
				b.Fatal(err)
			}
			entries = entries[:0]
		}
	}
	if len(entries) > 0 {
		if err := bk.BatchSet(entries); err != nil {
			b.Fatal(err)
		}
	}
	return bk
}

func BenchmarkOrderedScan250k(b *testing.B) {
	bk := benchOrderedBucket(b, 250000)
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		if err := bk.ScanRange(nil, nil, func(k, v []byte) bool { n++; return true }); err != nil {
			b.Fatal(err)
		}
	}
	_ = n
}

func BenchmarkPrefixScan250k(b *testing.B) {
	bk := benchOrderedBucket(b, 250000)
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		if err := bk.ScanPrefix([]byte("k00001"), func(k, v []byte) bool { n++; return true }); err != nil {
			b.Fatal(err)
		}
	}
	_ = n
}

func BenchmarkQueryCount250k(b *testing.B) {
	bk := benchOrderedBucket(b, 250000)
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		c, err := bk.Query().Count()
		if err != nil {
			b.Fatal(err)
		}
		n += c
	}
	_ = n
}

func BenchmarkUnorderedScan250k(b *testing.B) {
	bk := benchOrderedBucket(b, 250000)
	b.ReportAllocs()
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		if err := bk.Scanner(func(k, v []byte) bool { n++; return true }); err != nil {
			b.Fatal(err)
		}
	}
	_ = n
}

func fillBucket(b *testing.B, bk *Bucket, n int) {
	b.Helper()
	entries := make([]KV, 0, 1000)
	for i := 0; i < n; i++ {
		entries = append(entries, KV{
			Key:   []byte(fmt.Sprintf("k%08d", i)),
			Value: []byte("value"),
		})
		if len(entries) == cap(entries) {
			if err := bk.BatchSet(entries); err != nil {
				b.Fatal(err)
			}
			entries = entries[:0]
		}
	}
	if len(entries) > 0 {
		if err := bk.BatchSet(entries); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderedScanAfterOtherBucketWrite(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "ordered-multi.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	a, other := db.Bucket("a"), db.Bucket("o")
	fillBucket(b, a, 250000)
	fillBucket(b, other, 250000)

	// Warm a's snapshot.
	if err := a.ScanRangeKeys(nil, nil, func(k []byte) bool { return true }); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := other.Set([]byte(fmt.Sprintf("x%08d", i)), []byte("v")); err != nil {
			b.Fatal(err)
		}
		if err := a.ScanRange(nil, nil, func(k, v []byte) bool { return true }); err != nil {
			b.Fatal(err)
		}
	}
}
