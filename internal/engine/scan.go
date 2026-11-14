package engine

import (
	"context"
	"errors"
	"sync"
)

// ScanOptions configures the behavior of Scan operations.
type ScanOptions struct {
	// Concurrency controls the number of parallel workers.
	// 0 or 1 means sequential scan. Values > 1 enable parallel scanning.
	Concurrency int

	// BatchSize controls how many keys are collected before dispatching
	// to workers in parallel mode. Default is 1024.
	BatchSize int
}

// Scan iterates over all key-value pairs in the database.
// It uses the index to enumerate keys and reads values from storage.
//
// Consistency: Scan does NOT provide snapshot isolation. It observes the
// current state of the database as it iterates. Concurrent writes may cause
// some records to be seen, missed, or seen twice. For analytics workloads
// this is acceptable; for exact consistency, use a snapshot-based approach.
//
// The callback receives copies of the key and value bytes. Modifying them
// does not affect the database. Return an error to abort the scan.
func (db *DB) Scan(ctx context.Context, opts *ScanOptions, fn func(key, value []byte) error) error {
	if opts == nil {
		opts = &ScanOptions{}
	}

	concurrency := opts.Concurrency
	if concurrency <= 1 {
		return db.scanSequential(ctx, fn)
	}

	return db.scanParallel(ctx, opts, fn)
}

// errScanStopped marks a parallel-scan enumeration that stopped because
// every worker had already exited; it is never surfaced to callers.
var errScanStopped = errors.New("scan stopped")

// scanSequential iterates all keys in index order, reading each value.
func (db *DB) scanSequential(ctx context.Context, fn func(key, value []byte) error) error {
	idx := db.index.Load()
	store := db.storage.Load()

	return idx.Scan(func(key []byte, offset int64) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		val, err := store.ReadAt(offset, key, nil)
		if err != nil {
			return err
		}

		// Copy key and value so the callback can hold references safely.
		cKey := make([]byte, len(key))
		copy(cKey, key)
		cVal := make([]byte, len(val))
		copy(cVal, val)

		return fn(cKey, cVal)
	})
}

// scanParallel streams index entries to workers through a bounded channel.
// Enumeration overlaps with value reads and nothing is materialized: the
// channel retains at most batchSize (key, offset) tasks at a time, so
// memory stays bounded regardless of dataset size.
func (db *DB) scanParallel(ctx context.Context, opts *ScanOptions, fn func(key, value []byte) error) error {
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultScanConcurrency
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultScanBatchSize
	}

	idx := db.index.Load()
	store := db.storage.Load()

	type task struct {
		key    []byte
		offset int64
	}

	work := make(chan task, batchSize)
	errCh := make(chan error, concurrency)
	stopped := make(chan struct{})
	prodDone := make(chan struct{})
	var werr error

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range work {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				default:
				}
				val, err := store.ReadAt(t.offset, t.key, nil)
				if err != nil {
					errCh <- err
					return
				}
				cVal := make([]byte, len(val))
				copy(cVal, val)
				if err := fn(t.key, cVal); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	go func() {
		defer close(prodDone)
		defer close(work)
		if e := idx.Scan(func(key []byte, offset int64) error {
			select {
			case <-ctx.Done():
				werr = ctx.Err()
				return werr
			case <-stopped:
				// Every worker exited on an error; no receiver is left to
				// drain the channel, so stop enumerating instead of
				// blocking forever.
				return errScanStopped
			case work <- task{key: key, offset: offset}:
				return nil
			}
		}); e != nil && e != errScanStopped && werr == nil {
			werr = e
		}
	}()

	wg.Wait()
	close(stopped)
	// The producer may still be running; it escapes via stopped above, and
	// joining it here makes the werr read below race-free.
	<-prodDone
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	if werr != nil {
		return werr
	}
	return nil
}
