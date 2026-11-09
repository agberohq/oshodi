package engine

import (
	"context"
	"sync"
)

// MapFunc transforms a key-value pair into an intermediate key-value pair.
// The intermediate key is used for grouping in the reduce phase.
type MapFunc func(key, value []byte) (ik, iv []byte, err error)

// ReduceFunc combines intermediate values for a given intermediate key.
// It receives all intermediate values for one intermediate key and produces
// a single output value.
type ReduceFunc func(ik []byte, values [][]byte) ([]byte, error)

// CombinerFunc is a worker-local aggregation function that can partially
// combine intermediate results before the final reduce phase.
// It has the same signature as ReduceFunc but operates on a subset of values.
// The combiner must be compatible with the reducer (i.e., combining partial
// results must produce the same final result as combining all values at once).
type CombinerFunc func(ik []byte, values [][]byte) ([]byte, error)

// MapReduceOptions configures the behavior of MapReduce operations.
type MapReduceOptions struct {
	// Concurrency controls the number of parallel map workers.
	// 0 or 1 means sequential processing. Values > 1 enable parallel maps.
	Concurrency int

	// BatchSize controls how many keys are collected before dispatching
	// to workers in parallel mode. Default is 1024.
	BatchSize int

	// EnableCombiner enables worker-local aggregation when a combiner
	// is provided. This reduces the amount of data shuffled to the
	// final reduce phase.
	EnableCombiner bool
}

// MapResult holds the results of a Map operation.
type MapResult struct {
	// Key is the intermediate key produced by the mapper.
	Key []byte
	// Value is the intermediate value produced by the mapper.
	Value []byte
}

// Map applies a transformation function to all key-value pairs in the database.
// It returns a slice of intermediate key-value pairs.
//
// For large datasets, consider using MapReduce with a combiner to avoid
// materializing all intermediate results in memory.
func (db *DB) Map(ctx context.Context, opts *MapReduceOptions, fn MapFunc) ([]MapResult, error) {
	if opts == nil {
		opts = &MapReduceOptions{}
	}

	var results []MapResult
	var mu sync.Mutex

	scanOpts := &ScanOptions{
		Concurrency: opts.Concurrency,
		BatchSize:   opts.BatchSize,
	}

	err := db.Scan(ctx, scanOpts, func(key, value []byte) error {
		ik, iv, err := fn(key, value)
		if err != nil {
			return err
		}

		// Skip nil results (used for filtering)
		if ik == nil {
			return nil
		}

		mu.Lock()
		defer mu.Unlock()
		results = append(results, MapResult{
			Key:   ik,
			Value: iv,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// Reduce combines intermediate values for each intermediate key.
// It groups all intermediate values by their key and applies the reduce
// function to each group.
func (db *DB) Reduce(ctx context.Context, results []MapResult, fn ReduceFunc) (map[string][]byte, error) {
	// Group intermediate values by key.
	groups := make(map[string][][]byte)
	for _, r := range results {
		key := string(r.Key)
		groups[key] = append(groups[key], r.Value)
	}

	// Apply reduce function to each group.
	output := make(map[string][]byte)
	for key, values := range groups {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		val, err := fn([]byte(key), values)
		if err != nil {
			return nil, err
		}
		output[key] = val
	}

	return output, nil
}

// MapReduce executes a complete MapReduce pipeline: scan → map → [combiner] → reduce.
// This is the primary API for data processing in Oshodi.
//
// The pipeline works as follows:
// Scan: Enumerate all keys from the index and read values from storage.
// Map: Transform each record into an intermediate key-value pair.
// Combiner (optional): Worker-local aggregation to reduce intermediate data.
// Reduce: Combine all intermediate results into final output.
//
// Consistency: MapReduce does NOT provide snapshot isolation. It observes the
// current state of the database as it iterates. For analytics workloads this
// is acceptable; for exact consistency, use a snapshot-based approach.
//
// Memory usage: The pipeline avoids materializing all intermediate results
// when a combiner is provided. Each worker maintains local partial results
// that are combined at the end.
func (db *DB) MapReduce(
	ctx context.Context,
	opts *MapReduceOptions,
	mapper MapFunc,
	reducer ReduceFunc,
	combiner CombinerFunc,
) (map[string][]byte, error) {
	if opts == nil {
		opts = &MapReduceOptions{}
	}

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	// Collect all keys first for partitioning.
	type kvPair struct {
		key    []byte
		offset int64
	}

	var allKeys []kvPair
	var mu sync.Mutex
	var collectErr error

	idx := db.index.Load()
	err := idx.Scan(func(key []byte, offset int64) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		mu.Lock()
		defer mu.Unlock()
		if collectErr != nil {
			return collectErr
		}
		k := make([]byte, len(key))
		copy(k, key)
		allKeys = append(allKeys, kvPair{key: k, offset: offset})
		return nil
	})
	if err != nil {
		return nil, err
	}

	store := db.storage.Load()

	// Worker-local intermediate results.
	type workerResult struct {
		groups map[string][][]byte // intermediate key → values
	}

	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	var resultsMu sync.Mutex
	var allResults []workerResult

	// Handle empty database case.
	if len(allKeys) == 0 {
		return make(map[string][]byte), nil
	}

	workers := concurrency
	if workers > len(allKeys) {
		workers = len(allKeys)
	}

	chunkSize := (len(allKeys) + workers - 1) / workers
	for w := 0; w < workers; w++ {
		start := w * chunkSize
		if start >= len(allKeys) {
			break
		}
		end := start + chunkSize
		if end > len(allKeys) {
			end = len(allKeys)
		}

		chunk := allKeys[start:end]
		wg.Add(1)
		go func(pairs []kvPair) {
			defer wg.Done()

			localGroups := make(map[string][][]byte)

			for _, pair := range pairs {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				default:
				}

				val, err := store.ReadAt(pair.offset, pair.key, nil)
				if err != nil {
					errCh <- err
					return
				}

				// Map phase
				ik, iv, err := mapper(pair.key, val)
				if err != nil {
					errCh <- err
					return
				}

				if ik == nil {
					continue
				}

				localGroups[string(ik)] = append(localGroups[string(ik)], iv)
			}

			// Local combiner phase (optional)
			if opts.EnableCombiner && combiner != nil {
				for ik, values := range localGroups {
					select {
					case <-ctx.Done():
						errCh <- ctx.Err()
						return
					default:
					}

					combined, err := combiner([]byte(ik), values)
					if err != nil {
						errCh <- err
						return
					}
					// Replace the group with the single combined value.
					localGroups[ik] = [][]byte{combined}
				}
			}

			resultsMu.Lock()
			allResults = append(allResults, workerResult{groups: localGroups})
			resultsMu.Unlock()
		}(chunk)
	}

	wg.Wait()
	close(errCh)

	// Check for errors from workers.
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}

	// Merge all worker-local results.
	merged := make(map[string][][]byte)
	for _, wr := range allResults {
		for ik, values := range wr.groups {
			merged[ik] = append(merged[ik], values...)
		}
	}

	// Final reduce phase.
	output := make(map[string][]byte)
	for ik, values := range merged {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		val, err := reducer([]byte(ik), values)
		if err != nil {
			return nil, err
		}
		output[ik] = val
	}

	return output, nil
}
