package storage

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

// A failed flush write must re-queue its batch: writers already received
// these offsets, so dropping the batch silently loses acknowledged records.
func TestWALFlushFailureKeepsPending(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "f.wal")
	cfg := DefaultWALConfig(tmp)
	cfg.Mode = SyncMode

	w, err := NewWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Break the underlying file so the flush write fails.
	if err := w.file.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Write(context.Background(), []byte("k"), []byte("v")); err == nil {
		t.Fatal("expected flush error")
	}

	w.mu.Lock()
	n := len(w.pending)
	fp := w.flushPos.Load()
	w.mu.Unlock()

	if n == 0 {
		t.Fatal("failed batch was dropped from the pending queue")
	}
	if fp != 0 {
		t.Fatalf("flushPos advanced past the failed batch: %d", fp)
	}
}

// Concurrent writes racing Close must never be acknowledged after the final
// flush has run.
func TestWALWriteConcurrentWithClose(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "race.wal")
	cfg := DefaultWALConfig(tmp)
	cfg.Mode = SyncMode

	w, err := NewWAL(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, err := w.Write(context.Background(), []byte{byte('a' + i)}, []byte("v"))
				if err != nil && err != ErrWALClosed {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(i)
	}
	if err := w.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	wg.Wait()
}
