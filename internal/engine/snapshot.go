package engine

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/agberohq/oshodi/internal/storage"
)

// Snapshot support.
//
// A snapshot is an offset watermark: a record is visible to it iff its offset
// is below the watermark. Records are never rewritten in place, so an offset
// stays valid until compaction replaces the file, which is why compaction is
// deferred while a snapshot is open.
//
// Overwriting a key replaces its index entry, so older versions would become
// unreachable. The engine keeps an in-memory history of the offsets it
// displaced, pruned against the oldest live snapshot.

// ErrSnapshotsActive is returned by Compact while a snapshot is open.
var ErrSnapshotsActive = errSnapshotsActive{}

type errSnapshotsActive struct{}

func (errSnapshotsActive) Error() string {
	return "oshodi: compaction deferred while a snapshot is open"
}

// writeTracker tracks writes whose index publication has not landed yet.
//
// A snapshot watermark must not include an offset whose write is still in
// flight: the record may be on disk while the index still points at the older
// version, so a reader could resolve the new record mid-commit. The watermark is
// therefore the minimum start offset of any in-flight write, or the logical size
// when none are in flight. It is deliberately conservative: a stalled writer
// makes snapshots see an older state, never a torn one.
type writeTracker struct {
	mu       sync.Mutex
	inflight map[int64]int
	min      int64
}

func newWriteTracker() *writeTracker {
	return &writeTracker{inflight: make(map[int64]int, 8)}
}

// begin registers a write that will append at or after logical.
// It must be called before the record is reserved.
func (t *writeTracker) begin(logical int64) {
	t.mu.Lock()
	t.inflight[logical]++
	if t.min == 0 || logical < t.min {
		t.min = logical
	}
	t.mu.Unlock()
}

// done releases a write registered by begin, after its index publication.
func (t *writeTracker) done(logical int64) {
	t.mu.Lock()
	if n := t.inflight[logical]; n > 1 {
		t.inflight[logical] = n - 1
	} else {
		delete(t.inflight, logical)
	}
	if len(t.inflight) == 0 {
		t.min = 0
	} else if logical == t.min {
		min := int64(0)
		for k := range t.inflight {
			if min == 0 || k < min {
				min = k
			}
		}
		t.min = min
	}
	t.mu.Unlock()
}

// watermark returns the highest offset a snapshot may include.
func (t *writeTracker) watermark(logical int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.inflight) == 0 {
		return logical
	}
	return t.min
}

// keyHistory stores, per key, the offsets of versions displaced by later
// writes, in ascending order. The index always holds the newest version; this
// is only the tail.
type keyHistory struct {
	mu    sync.Mutex
	byKey map[string][]int64
}

func newKeyHistory() *keyHistory {
	return &keyHistory{byKey: make(map[string][]int64, 64)}
}

// record notes that prev was displaced by a newer write to key.
//
// bound is the lowest watermark a live or future snapshot can still have
// (min of the oldest open snapshot and the in-flight write window). Entries
// below it are unreachable, so they are dropped — except the newest one below
// the boundary, which is what a snapshot sitting exactly on the boundary still
// needs. A bound of 0 (file start) disables pruning.
func (h *keyHistory) record(key string, prev int64, bound int64) {
	if prev < 0 {
		return
	}
	h.mu.Lock()
	entries := append(h.byKey[key], prev)
	if bound > 0 {
		keepFrom := 0
		for i, off := range entries {
			if off < bound {
				keepFrom = i // the newest below the boundary so far
			}
		}
		if keepFrom > 0 {
			entries = entries[keepFrom:]
		}
	}
	h.byKey[key] = entries
	h.mu.Unlock()
}

// lookup returns the key's displaced offsets in ascending order.
func (h *keyHistory) lookup(key string) []int64 {
	h.mu.Lock()
	entries := h.byKey[key]
	var out []int64
	if len(entries) > 0 {
		out = make([]int64, len(entries))
		copy(out, entries)
	}
	h.mu.Unlock()
	return out
}

// keysInRange returns the history keys under prefix p in [lo, hi): keys that
// were overwritten or deleted, which the ordered index no longer lists.
func (h *keyHistory) keysInRange(p, lo, hi string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for k := range h.byKey {
		if !strings.HasPrefix(k, p) || k < lo || (hi != "" && k >= hi) {
			continue
		}
		out = append(out, k)
	}
	return out
}

func (h *keyHistory) clear() {
	h.mu.Lock()
	h.byKey = make(map[string][]int64, 64)
	h.mu.Unlock()
}

// snapshotRegistry tracks open snapshots so compaction can defer and history
// can prune to the oldest live watermark.
type snapshotRegistry struct {
	mu      sync.Mutex
	live    map[int64]int
	oldestW int64
}

func newSnapshotRegistry() *snapshotRegistry {
	return &snapshotRegistry{live: make(map[int64]int, 8)}
}

func (r *snapshotRegistry) add(watermark int64) {
	r.mu.Lock()
	r.live[watermark]++
	if r.oldestW == 0 || watermark < r.oldestW {
		r.oldestW = watermark
	}
	r.mu.Unlock()
}

func (r *snapshotRegistry) remove(watermark int64) {
	r.mu.Lock()
	if n := r.live[watermark]; n > 1 {
		r.live[watermark] = n - 1
	} else {
		delete(r.live, watermark)
	}
	if len(r.live) == 0 {
		r.oldestW = 0
	} else if watermark == r.oldestW {
		oldest := int64(0)
		for w := range r.live {
			if oldest == 0 || w < oldest {
				oldest = w
			}
		}
		r.oldestW = oldest
	}
	r.mu.Unlock()
}

// oldest returns the oldest live watermark, or 0 when no snapshot is open.
func (r *snapshotRegistry) oldest() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.oldestW
}

func (r *snapshotRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.live)
}

// historyBound returns the lowest offset a live or imminent snapshot can still
// need history for: the minimum of the oldest open snapshot watermark and the
// in-flight write window. Called from a write path, so the caller's own write
// is part of that window.
func (db *DB) historyBound() int64 {
	bound := db.writes.watermark(db.storage.Load().LogicalSize())
	if o := db.snaps.oldest(); o != 0 && (bound == 0 || o < bound) {
		bound = o
	}
	return bound
}

// Snapshot is a point-in-time read view of the database.
//
// Close must be called; an open snapshot defers compaction.
type Snapshot struct {
	db        *DB
	watermark int64
	closed    atomic.Bool
}

// Snapshot captures a read view. Creating one is O(1): it records the current
// write watermark, not a copy of the data.
func (db *DB) Snapshot() (*Snapshot, error) {
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	// A compaction swap changes every offset, so it must not straddle snapshot
	// creation. Compaction holds this for its whole run; it is rare, and it
	// refuses to start while snapshots are live anyway.
	db.compactMu.Lock()
	logical := db.storage.Load().LogicalSize()
	watermark := db.writes.watermark(logical)
	db.snaps.add(watermark)
	db.compactMu.Unlock()

	return &Snapshot{db: db, watermark: watermark}, nil
}

// Watermark returns the offset boundary this view reads at.
func (s *Snapshot) Watermark() int64 { return s.watermark }

// Close releases the view.
func (s *Snapshot) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	s.db.snaps.remove(s.watermark)
	return nil
}

// getFull resolves a fully namespaced key as of the snapshot.
func (s *Snapshot) getFull(full string) ([]byte, bool, error) {
	if s.closed.Load() {
		return nil, false, ErrDBClosed
	}
	// Readers take the publication lock shared so a write's index update and
	// its displaced-version record are observed as one step.
	s.db.readViewMu.RLock()
	defer s.db.readViewMu.RUnlock()

	idx := s.db.index.Load()
	store := s.db.storage.Load()

	head, ok := idx.GetRaw(full)
	if !ok {
		return nil, false, nil
	}

	// Candidates newest first: the index head, then displaced versions from
	// history (which is ascending, so walk it backwards).
	var offsets []int64
	if head < 0 {
		offsets = append(offsets, markToTombstone(head))
	} else {
		offsets = append(offsets, head)
	}
	hist := s.db.history.lookup(full)
	for i := len(hist) - 1; i >= 0; i-- {
		offsets = append(offsets, hist[i])
	}

	for _, off := range offsets {
		if off >= s.watermark {
			continue // written after this view
		}
		_, val, tombstone, _, err := store.ReadRecordAt(off)
		if err != nil {
			if errors.Is(err, storage.ErrCorrupt) || errors.Is(err, storage.ErrClosed) {
				// A concurrently closed/replaced generation; treat as absent
				// rather than surfacing a corruption error to the caller.
				return nil, false, nil
			}
			return nil, false, err
		}
		if tombstone {
			return nil, false, nil
		}
		return val, true, nil
	}
	return nil, false, nil
}
