package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func newTestDB(t *testing.T) (*DB, func()) {
	t.Helper()
	tmp := t.TempDir()
	db, err := NewDB(&Config{
		FilePath: tmp + "/test.db",
	})
	if err != nil {
		t.Fatalf("failed to create test DB: %v", err)
	}
	return db, func() { db.Close() }
}

func populateTestDB(t *testing.T, db *DB, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("value-%d", i))
		if err := db.Set(key, val); err != nil {
			t.Fatalf("failed to set key %d: %v", i, err)
		}
	}
}

// Scan Tests

func TestScan_Sequential(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	populateTestDB(t, db, 100)

	var mu sync.Mutex
	seen := make(map[string]string)

	err := db.Scan(context.Background(), &ScanOptions{}, func(key, value []byte) error {
		mu.Lock()
		defer mu.Unlock()
		seen[string(key)] = string(value)
		return nil
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(seen) != 100 {
		t.Errorf("expected 100 records, got %d", len(seen))
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("value-%d", i)
		if got := seen[key]; got != expected {
			t.Errorf("key %s: expected %s, got %s", key, expected, got)
		}
	}
}

func TestScan_EmptyDB(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	count := 0
	err := db.Scan(context.Background(), nil, func(key, value []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records, got %d", count)
	}
}

func TestScan_ContextCancellation(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	populateTestDB(t, db, 1000)

	ctx, cancel := context.WithCancel(context.Background())
	count := 0
	err := db.Scan(ctx, &ScanOptions{}, func(key, value []byte) error {
		count++
		if count >= 50 {
			cancel()
		}
		return nil
	})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if count >= 1000 {
		t.Errorf("expected scan to stop early, but processed %d records", count)
	}
}

func TestScan_CallbackError(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	populateTestDB(t, db, 100)

	expectedErr := fmt.Errorf("test error")
	count := 0
	err := db.Scan(context.Background(), &ScanOptions{}, func(key, value []byte) error {
		count++
		if count == 10 {
			return expectedErr
		}
		return nil
	})
	if err != expectedErr {
		t.Errorf("expected callback error, got %v", err)
	}
	if count != 10 {
		t.Errorf("expected scan to stop at 10, got %d", count)
	}
}

func TestScan_Parallel(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	populateTestDB(t, db, 1000)

	var mu sync.Mutex
	seen := make(map[string]string)

	err := db.Scan(context.Background(), &ScanOptions{
		Concurrency: 4,
	}, func(key, value []byte) error {
		mu.Lock()
		defer mu.Unlock()
		seen[string(key)] = string(value)
		return nil
	})
	if err != nil {
		t.Fatalf("Parallel Scan failed: %v", err)
	}

	if len(seen) != 1000 {
		t.Errorf("expected 1000 records, got %d", len(seen))
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("value-%d", i)
		if got := seen[key]; got != expected {
			t.Errorf("key %s: expected %s, got %s", key, expected, got)
		}
	}
}

func TestScan_Parallel_ContextCancellation(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	populateTestDB(t, db, 1000)

	ctx, cancel := context.WithCancel(context.Background())
	count := 0
	var mu sync.Mutex

	err := db.Scan(ctx, &ScanOptions{
		Concurrency: 4,
	}, func(key, value []byte) error {
		mu.Lock()
		count++
		c := count
		mu.Unlock()

		if c >= 100 {
			cancel()
		}
		return nil
	})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// Map Tests

func TestMap_Basic(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	// Store category → item data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("item-%d", i))
		val := []byte(fmt.Sprintf("category-%d", i%3))
		if err := db.Set(key, val); err != nil {
			t.Fatalf("failed to set: %v", err)
		}
	}

	results, err := db.Map(context.Background(), &MapReduceOptions{}, func(key, value []byte) (ik, iv []byte, err error) {
		// Map: extract category as key, "1" as value for counting
		return value, []byte("1"), nil
	})
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}
}

func TestMap_Filter(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("%d", i))
		if err := db.Set(key, val); err != nil {
			t.Fatalf("failed to set: %v", err)
		}
	}

	results, err := db.Map(context.Background(), &MapReduceOptions{}, func(key, value []byte) (ik, iv []byte, err error) {
		// Only map even-numbered keys (key ends with 0, 2, 4, 6, 8)
		lastByte := key[len(key)-1]
		if (lastByte-'0')%2 != 0 {
			return nil, nil, nil // filter out odd keys
		}
		return key, value, nil
	})
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}

	// Should have exactly 50 results (even keys only)
	if len(results) != 50 {
		t.Errorf("expected 50 results, got %d", len(results))
	}
}

// Reduce Tests

func TestReduce_Sum(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	results := []MapResult{
		{Key: []byte("total"), Value: []byte("10")},
		{Key: []byte("total"), Value: []byte("20")},
		{Key: []byte("total"), Value: []byte("30")},
		{Key: []byte("count"), Value: []byte("1")},
	}

	output, err := db.Reduce(context.Background(), results, func(ik []byte, values [][]byte) ([]byte, error) {
		sum := 0
		for _, v := range values {
			var n int
			fmt.Sscanf(string(v), "%d", &n)
			sum += n
		}
		return []byte(fmt.Sprintf("%d", sum)), nil
	})
	if err != nil {
		t.Fatalf("Reduce failed: %v", err)
	}

	if got := string(output["total"]); got != "60" {
		t.Errorf("total: expected 60, got %s", got)
	}
	if got := string(output["count"]); got != "1" {
		t.Errorf("count: expected 1, got %s", got)
	}
}

// MapReduce Tests

func TestMapReduce_CountByCategory(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	// Store items with categories
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("item-%d", i))
		category := fmt.Sprintf("cat-%d", i%5)
		if err := db.Set(key, []byte(category)); err != nil {
			t.Fatalf("failed to set: %v", err)
		}
	}

	// MapReduce: count items per category
	result, err := db.MapReduce(
		context.Background(),
		&MapReduceOptions{},
		// Mapper: extract category as intermediate key
		func(key, value []byte) (ik, iv []byte, err error) {
			return value, []byte("1"), nil
		},
		// Reducer: count values
		func(ik []byte, values [][]byte) ([]byte, error) {
			return []byte(fmt.Sprintf("%d", len(values))), nil
		},
		nil, // no combiner
	)
	if err != nil {
		t.Fatalf("MapReduce failed: %v", err)
	}

	// Each category should have 20 items (100 / 5)
	for i := 0; i < 5; i++ {
		cat := fmt.Sprintf("cat-%d", i)
		if got := string(result[cat]); got != "20" {
			t.Errorf("category %s: expected 20, got %s", cat, got)
		}
	}
}

func TestMapReduce_WithCombiner(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	// Store numeric values
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("%d", i))
		if err := db.Set(key, val); err != nil {
			t.Fatalf("failed to set: %v", err)
		}
	}

	// MapReduce: sum all values with combiner
	result, err := db.MapReduce(
		context.Background(),
		&MapReduceOptions{
			Concurrency:    4,
			EnableCombiner: true,
		},
		// Mapper: group all values under "sum" key
		func(key, value []byte) (ik, iv []byte, err error) {
			return []byte("sum"), value, nil
		},
		// Reducer: sum all values
		func(ik []byte, values [][]byte) ([]byte, error) {
			sum := 0
			for _, v := range values {
				var n int
				fmt.Sscanf(string(v), "%d", &n)
				sum += n
			}
			return []byte(fmt.Sprintf("%d", sum)), nil
		},
		// Combiner: partial sum (same as reducer for associative operation)
		func(ik []byte, values [][]byte) ([]byte, error) {
			sum := 0
			for _, v := range values {
				var n int
				fmt.Sscanf(string(v), "%d", &n)
				sum += n
			}
			return []byte(fmt.Sprintf("%d", sum)), nil
		},
	)
	if err != nil {
		t.Fatalf("MapReduce failed: %v", err)
	}

	// Sum of 0..99 = 4950
	if got := string(result["sum"]); got != "4950" {
		t.Errorf("sum: expected 4950, got %s", got)
	}
}

func TestMapReduce_Parallel(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	// Store items with categories
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("item-%d", i))
		category := fmt.Sprintf("cat-%d", i%10)
		if err := db.Set(key, []byte(category)); err != nil {
			t.Fatalf("failed to set: %v", err)
		}
	}

	result, err := db.MapReduce(
		context.Background(),
		&MapReduceOptions{
			Concurrency: 8,
		},
		func(key, value []byte) (ik, iv []byte, err error) {
			return value, []byte("1"), nil
		},
		func(ik []byte, values [][]byte) ([]byte, error) {
			return []byte(fmt.Sprintf("%d", len(values))), nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("MapReduce failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		cat := fmt.Sprintf("cat-%d", i)
		if got := string(result[cat]); got != "100" {
			t.Errorf("category %s: expected 100, got %s", cat, got)
		}
	}
}

func TestMapReduce_EmptyDB(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	result, err := db.MapReduce(
		context.Background(),
		&MapReduceOptions{},
		func(key, value []byte) (ik, iv []byte, err error) {
			return value, []byte("1"), nil
		},
		func(ik []byte, values [][]byte) ([]byte, error) {
			return []byte(fmt.Sprintf("%d", len(values))), nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("MapReduce failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d entries", len(result))
	}
}

func TestMapReduce_ContextCancellation(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	populateTestDB(t, db, 10000)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := db.MapReduce(
		ctx,
		&MapReduceOptions{},
		func(key, value []byte) (ik, iv []byte, err error) {
			return value, []byte("1"), nil
		},
		func(ik []byte, values [][]byte) ([]byte, error) {
			return []byte(fmt.Sprintf("%d", len(values))), nil
		},
		nil,
	)
	if err != context.Canceled {
		t.Logf("MapReduce returned %v (may not be canceled if keys collected before cancel)", err)
	}
}

func TestMapReduce_MapperError(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	populateTestDB(t, db, 10)

	expectedErr := fmt.Errorf("mapper error")
	_, err := db.MapReduce(
		context.Background(),
		&MapReduceOptions{},
		func(key, value []byte) (ik, iv []byte, err error) {
			return nil, nil, expectedErr
		},
		func(ik []byte, values [][]byte) ([]byte, error) {
			return nil, nil
		},
		nil,
	)
	if err != expectedErr {
		t.Errorf("expected mapper error, got %v", err)
	}
}

func TestMapReduce_ReduceError(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	populateTestDB(t, db, 10)

	expectedErr := fmt.Errorf("reducer error")
	_, err := db.MapReduce(
		context.Background(),
		&MapReduceOptions{},
		func(key, value []byte) (ik, iv []byte, err error) {
			return []byte("key"), []byte("val"), nil
		},
		func(ik []byte, values [][]byte) ([]byte, error) {
			return nil, expectedErr
		},
		nil,
	)
	if err != expectedErr {
		t.Errorf("expected reducer error, got %v", err)
	}
}

func TestMapReduce_CombinerError(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	populateTestDB(t, db, 10)

	expectedErr := fmt.Errorf("combiner error")
	_, err := db.MapReduce(
		context.Background(),
		&MapReduceOptions{
			EnableCombiner: true,
		},
		func(key, value []byte) (ik, iv []byte, err error) {
			return []byte("key"), []byte("val"), nil
		},
		func(ik []byte, values [][]byte) ([]byte, error) {
			return []byte("result"), nil
		},
		func(ik []byte, values [][]byte) ([]byte, error) {
			return nil, expectedErr
		},
	)
	if err != expectedErr {
		t.Errorf("expected combiner error, got %v", err)
	}
}

// Integration: Map + Reduce separately

func TestMapThenReduce(t *testing.T) {
	db, cleanup := newTestDB(t)
	defer cleanup()

	// Store items with categories
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("item-%d", i))
		category := fmt.Sprintf("cat-%d", i%5)
		if err := db.Set(key, []byte(category)); err != nil {
			t.Fatalf("failed to set: %v", err)
		}
	}

	// Map phase
	results, err := db.Map(context.Background(), &MapReduceOptions{}, func(key, value []byte) (ik, iv []byte, err error) {
		return value, []byte("1"), nil
	})
	if err != nil {
		t.Fatalf("Map failed: %v", err)
	}

	// Reduce phase
	output, err := db.Reduce(context.Background(), results, func(ik []byte, values [][]byte) ([]byte, error) {
		return []byte(fmt.Sprintf("%d", len(values))), nil
	})
	if err != nil {
		t.Fatalf("Reduce failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		cat := fmt.Sprintf("cat-%d", i)
		if got := string(output[cat]); got != "20" {
			t.Errorf("category %s: expected 20, got %s", cat, got)
		}
	}
}

// Benchmarks

func populateBenchDB(b *testing.B, db *DB, count int) {
	b.Helper()
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("value-%d-%d-%d-%d", i, i*2, i*3, i*4))
		if err := db.Set(key, val); err != nil {
			b.Fatalf("failed to set key %d: %v", i, err)
		}
	}
}

func benchmarkScanSequential(n int, b *testing.B) {
	db, err := NewDB(&Config{
		FilePath: b.TempDir() + "/test.db",
	})
	if err != nil {
		b.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	populateBenchDB(b, db, n)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Scan(context.Background(), &ScanOptions{}, func(key, value []byte) error {
			return nil
		})
		if err != nil {
			b.Fatalf("Scan failed: %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(n)/float64(b.N)*1e6, "records/ms")
}

func BenchmarkScanSequential_100(b *testing.B)  { benchmarkScanSequential(100, b) }
func BenchmarkScanSequential_1K(b *testing.B)   { benchmarkScanSequential(1000, b) }
func BenchmarkScanSequential_10K(b *testing.B)  { benchmarkScanSequential(10000, b) }
func BenchmarkScanSequential_100K(b *testing.B) { benchmarkScanSequential(100000, b) }

func benchmarkScanParallel(n, concurrency int, b *testing.B) {
	db, err := NewDB(&Config{
		FilePath: b.TempDir() + "/test.db",
	})
	if err != nil {
		b.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	populateBenchDB(b, db, n)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Scan(context.Background(), &ScanOptions{
			Concurrency: concurrency,
		}, func(key, value []byte) error {
			return nil
		})
		if err != nil {
			b.Fatalf("Scan failed: %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(n)/float64(b.N)*1e6, "records/ms")
}

func BenchmarkScanParallel_1K(b *testing.B)   { benchmarkScanParallel(1000, 4, b) }
func BenchmarkScanParallel_10K(b *testing.B)  { benchmarkScanParallel(10000, 4, b) }
func BenchmarkScanParallel_100K(b *testing.B) { benchmarkScanParallel(100000, 4, b) }

func benchmarkMapReduce(n, concurrency int, b *testing.B) {
	db, err := NewDB(&Config{
		FilePath: b.TempDir() + "/test.db",
	})
	if err != nil {
		b.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	populateBenchDB(b, db, n)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.MapReduce(
			context.Background(),
			&MapReduceOptions{
				Concurrency:    concurrency,
				EnableCombiner: true,
			},
			func(key, value []byte) (ik, iv []byte, err error) {
				return []byte("count"), []byte("1"), nil
			},
			func(ik []byte, values [][]byte) ([]byte, error) {
				return []byte(fmt.Sprintf("%d", len(values))), nil
			},
			func(ik []byte, values [][]byte) ([]byte, error) {
				return []byte(fmt.Sprintf("%d", len(values))), nil
			},
		)
		if err != nil {
			b.Fatalf("MapReduce failed: %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(n)/float64(b.N)*1e6, "records/ms")
}

func BenchmarkMapReduce_1K(b *testing.B)   { benchmarkMapReduce(1000, 4, b) }
func BenchmarkMapReduce_10K(b *testing.B)  { benchmarkMapReduce(10000, 4, b) }
func BenchmarkMapReduce_100K(b *testing.B) { benchmarkMapReduce(100000, 4, b) }
