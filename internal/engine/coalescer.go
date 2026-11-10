package engine

import (
	"fmt"
	"github.com/agberohq/oshodi/internal/consts"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
)

// pendingSet is one queued Set awaiting a leader's batch commit.
// Key/value bytes are copied at submission — callers legally reuse their
// buffers after Set returns, and the leader encodes them later.
type pendingSet struct {
	key   []byte
	value []byte
	done  chan error
}

// coalesceShard is one slice of the pending-queue map, keyed by key hash.
type coalesceShard struct {
	mu      sync.Mutex
	pending []*pendingSet
	spare   []*pendingSet
	leading bool
	notify  chan struct{}
}

// writeCoalescer implements leader-based group commit: the submitter that
// finds no leader on a shard runs the batch itself; everyone else waits on
// their done channel. Waiting occurs only while the shard is below
// BatchMinSize and still receiving entries.
type writeCoalescer struct {
	shards uint32
	mask   uint32
	shard  []coalesceShard

	batchMin      int           // grow the batch up to this size
	maxBatch      int           // wait-window target ceiling
	batchTimeout  time.Duration // window granted to reach batchMin
	queuePerShard int           // capacity; overflow falls back to direct write
}

func newWriteCoalescer(shards, batchMin, maxBatch, queueTotal int, batchTimeout time.Duration) *writeCoalescer {
	if shards < 1 {
		shards = 1
	}
	n := 1
	for n < shards {
		n <<= 1
	}
	perShard := queueTotal / shards
	if perShard < consts.MinCoalescePerShard {
		perShard = consts.MinCoalescePerShard
	}
	if batchMin <= 0 {
		batchMin = 1
	}
	if maxBatch < batchMin {
		maxBatch = batchMin
	}
	c := &writeCoalescer{
		shards:        uint32(n),
		mask:          uint32(n - 1),
		shard:         make([]coalesceShard, n),
		batchMin:      batchMin,
		maxBatch:      maxBatch,
		batchTimeout:  batchTimeout,
		queuePerShard: perShard,
	}
	for i := range c.shard {
		c.shard[i].notify = make(chan struct{}, 1)
	}
	return c
}

func hash64(key []byte) uint64 { return xxh3.Hash(key) }

// enqueue hands one Set to a shard. The final bool is false when the queue
// is full — the caller must fall back to the direct write path.
func (c *writeCoalescer) enqueue(key, value []byte) (*coalesceShard, *pendingSet, bool, bool) {
	sh := &c.shard[uint32(hash64(key))&c.mask]

	// Heap-allocated so the caller's slot stays valid regardless of later
	// queue swaps.
	ps := &pendingSet{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
		done:  make(chan error, 1),
	}

	sh.mu.Lock()
	if len(sh.pending) >= c.queuePerShard {
		sh.mu.Unlock()
		return nil, nil, false, false
	}
	sh.pending = append(sh.pending, ps)
	lead := !sh.leading
	if lead {
		sh.leading = true
	}
	sh.mu.Unlock()

	if !lead {
		// Wake the leader's optional batchMin growth window.
		select {
		case sh.notify <- struct{}{}:
		default:
		}
	}
	return sh, ps, lead, true
}

// take removes up to `want` queued entries from the front of the queue.
// A single array is kept per shard; the batch is copied out so newly
// arriving appends (which write into the same backing array) can never
// alias committed batch entries.
func (c *writeCoalescer) take(sh *coalesceShard, want int) []*pendingSet {
	sh.mu.Lock()
	n := len(sh.pending)
	if want >= 0 && want < n {
		n = want
	}
	batch := make([]*pendingSet, n)
	copy(batch, sh.pending[:n])
	sh.pending = sh.pending[n:]
	sh.mu.Unlock()
	return batch
}

// drain hands the current queue to the leader, optionally growing the
// window up to batchMin within batchTimeout while entries keep arriving.
// The returned batch always contains the leader's own entry (len >= 1).
func (c *writeCoalescer) drain(sh *coalesceShard) []*pendingSet {
	batch := c.take(sh, -1)
	if c.batchMin > 1 && len(batch) < c.batchMin {
		timer := time.NewTimer(c.batchTimeout)
		defer timer.Stop()
		for len(batch) < c.maxBatch {
			select {
			case <-sh.notify:
				batch = append(batch, c.take(sh, -1)...)
			case <-timer.C:
				batch = append(batch, c.take(sh, -1)...)
				return batch
			}
		}
	}
	return batch
}

// release hands leadership back when the queue is empty. Returns true
// when a late follower already joined — the leader must keep committing;
// returning false means leading is cleared and the leader may exit.
func (c *writeCoalescer) release(sh *coalesceShard) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if len(sh.pending) == 0 {
		sh.leading = false
		return false
	}
	return true
}

// coalesceSet runs one Set through the leader/queue machinery.
func (db *DB) coalesceSet(key, value []byte) error {
	sh, slot, lead, ok := db.coalescer.enqueue(key, value)
	if !ok {
		// Pending budget exhausted: never surface this as an error.
		return db.commitDirect(key, value)
	}
	if !lead {
		return <-slot.done
	}

	for {
		batch := db.coalescer.drain(sh)
		if len(batch) > 0 {
			db.commitBatch(batch)
			continue
		}
		// Hand leadership back; if a late follower joined mid-commit,
		// keep draining so its waiter is never stranded.
		if !db.coalescer.release(sh) {
			return nil
		}
	}
}

// commitBatch performs one batched physical write and index update and
// always completes every waiter — success, error, or leader panic.
func (db *DB) commitBatch(batch []*pendingSet) {
	defer func() {}()
	entries := make([]KV, len(batch))
	for i := range batch {
		entries[i] = KV{Key: batch[i].key, Value: batch[i].value}
	}

	var offsets []int64
	var werr error
	func() {
		db.writeCommitMu.RLock()
		defer db.writeCommitMu.RUnlock()
		offsets, werr = db.writer.BatchWriteRecord(entries)
	}()

	// Never mutate the shared *pendingSet fields: a follower may still be
	// dereferencing its own slot. The buffered done channel is the only
	// handoff after enqueue.
	finishAll := func(err error) {
		for i := range batch {
			if batch[i].done != nil {
				select {
				case batch[i].done <- err:
				default:
				}
			}
		}
	}

	defer func() {
		if r := recover(); r != nil {
			finishAll(fmt.Errorf("oshodi: batch commit failed: %v", r))
			panic(r)
		}
	}()

	if werr != nil {
		finishAll(werr)
		return
	}

	idx := db.index.Load()
	for i := range batch {
		db.metrics.Add(batch[i].key)
		idx.SetIfNewer(batch[i].key, offsets[i])
		db.cache.Delete(batch[i].key)
	}
	finishAll(nil)
}
