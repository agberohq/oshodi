package messaging

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/agberohq/oshodi/internal/consts"
	"github.com/agberohq/oshodi/internal/pipeline"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
	"github.com/olekukonko/mappo"
)

// pubsubJob is a reusable task to eliminate closure allocations during publish.
type pubsubJob struct {
	fn      func([]byte)
	payload []byte
	logger  *ll.Logger
	id      int64
	channel string
}

func (j *pubsubJob) Do() error {
	defer func() {
		if r := recover(); r != nil {
			if j.logger != nil {
				j.logger.Fields(
					"subscriber_id", j.id,
					"channel", j.channel,
					"panic", r,
					"stack", string(debug.Stack()),
				).Error("pubsub panic")
			}
		}
		j.fn = nil
		j.payload = nil
		pubsubJobPool.Put(j)
	}()

	if j.fn != nil {
		j.fn(j.payload)
	}
	return nil
}

var pubsubJobPool = sync.Pool{
	New: func() any {
		return &pubsubJob{}
	},
}

// subEntry caches subscriber id+fn pairs collected during Range to avoid
// repeated pool gets inside the iteration loop.
type subEntry struct {
	id int64
	fn func([]byte)
}

// subEntryPool reuses the slice header for subscriber snapshots.
var subEntryPool = sync.Pool{
	New: func() any {
		b := make([]subEntry, 0, consts.PubSubSnapshotCap)
		return &b
	},
}

// Channel holds subscribers for a specific bucket+channel
type Channel struct {
	subs *mappo.Sharded[int64, func([]byte)]
}

// PubSub is a high-performance publish-subscribe system
type PubSub struct {
	channels *mappo.Sharded[string, *Channel]
	// chanMu serialises channel-table create/cleanup.
	chanMu sync.Mutex
	pool   *jack.Pool
	logger *ll.Logger
	nextID atomic.Int64
	hooks  *pipeline.Hooks
	// copyPayload: each subscriber receives its own payload copy instead of
	// one copy shared by all subscribers. Default off.
	copyPayload bool
}

// NewPubSub creates a new PubSub instance
func NewPubSub(pool *jack.Pool, logger *ll.Logger, hooks *pipeline.Hooks, copyPayload bool) *PubSub {
	if logger == nil {
		logger = ll.New(consts.LoggerPubSub).Disable()
	}
	return &PubSub{
		channels:    mappo.NewSharded[string, *Channel](),
		pool:        pool,
		logger:      logger,
		hooks:       hooks,
		copyPayload: copyPayload,
	}
}

// channelSeparator joins the bucket-qualified channel name.
const channelSeparator = consts.KeySeparator

func chanKey(bucket, channel string) string {
	if bucket == "" {
		return channelSeparator + channel
	}
	return bucket + channelSeparator + channel
}

// Subscribe adds a subscriber and returns its ID and an unsubscribe function
func (ps *PubSub) Subscribe(bucket, channel string, fn func([]byte)) (int64, func()) {
	key := chanKey(bucket, channel)
	id := ps.nextID.Add(1)

	ps.chanMu.Lock()
	ch, _ := ps.channels.GetOrSet(key, &Channel{
		subs: mappo.NewSharded[int64, func([]byte)](),
	})
	ch.subs.Set(id, fn)
	ps.chanMu.Unlock()

	if ps.hooks != nil && ps.hooks.OnSubscribe != nil {
		ps.hooks.OnSubscribe(bucket, channel, id)
	}

	var once sync.Once
	return id, func() {
		once.Do(func() { ps.unsubscribeByID(bucket, channel, id) })
	}
}

// SubscribeSimple adds a subscriber and returns only the unsubscribe function
func (ps *PubSub) SubscribeSimple(bucket, channel string, fn func([]byte)) func() {
	_, unsub := ps.Subscribe(bucket, channel, fn)
	return unsub
}

func (ps *PubSub) unsubscribeByID(bucket, channel string, id int64) {
	key := chanKey(bucket, channel)
	if ch, ok := ps.channels.Get(key); ok {
		ch.subs.Delete(id)
	}
	// Drop empty channel tables so channel churn does not leak entries.
	ps.chanMu.Lock()
	if ch, ok := ps.channels.Get(key); ok && ch.subs.Len() == 0 {
		ps.channels.Delete(key)
	}
	ps.chanMu.Unlock()
	if ps.hooks != nil && ps.hooks.OnUnsubscribe != nil {
		ps.hooks.OnUnsubscribe(bucket, channel, id)
	}
}

// UnsubscribeAll removes all subscribers from a channel
func (ps *PubSub) UnsubscribeAll(bucket, channel string) {
	key := chanKey(bucket, channel)
	// chanMu closes the race with Subscribe's GetOrSet: without it a
	// concurrent Subscribe can install a subscriber into a channel that this
	// call deletes immediately afterwards.
	ps.chanMu.Lock()
	ch, ok := ps.channels.Get(key)
	if ok {
		ps.channels.Delete(key)
	}
	ps.chanMu.Unlock()
	if !ok {
		return
	}
	if ps.hooks != nil && ps.hooks.OnUnsubscribe != nil {
		ch.subs.Range(func(id int64, _ func([]byte)) bool {
			ps.hooks.OnUnsubscribe(bucket, channel, id)
			return true
		})
	}
}

// Count returns the number of subscribers for a channel
func (ps *PubSub) Count(bucket, channel string) int {
	key := chanKey(bucket, channel)
	if ch, ok := ps.channels.Get(key); ok {
		return ch.subs.Len()
	}
	return 0
}

// IDs returns all subscriber IDs for a channel
func (ps *PubSub) IDs(bucket, channel string) []int64 {
	key := chanKey(bucket, channel)
	if ch, ok := ps.channels.Get(key); ok {
		return ch.subs.Keys()
	}
	return nil
}

// Publish sends a payload to all subscribers of a channel.
// It snapshots the subscriber list first to avoid holding the sharded map
// during dispatch, then submits each subscriber as a pooled job.
//
// Dispatch is asynchronous, so the payload is copied once (or once per
// subscriber when copyPayload is set) before the call returns; callers may
// reuse or mutate their buffer immediately. Subscribers must not mutate the
// payload they receive.
func (ps *PubSub) Publish(bucket, channel string, payload []byte) (int, error) {
	if ps.hooks != nil && ps.hooks.OnPublish != nil {
		var err error
		payload, err = ps.hooks.OnPublish(bucket, channel, payload)
		if err != nil {
			return 0, fmt.Errorf("publish hook: %w", err)
		}
	}

	key := chanKey(bucket, channel)
	ch, ok := ps.channels.Get(key)
	if !ok || ch == nil {
		return 0, nil
	}

	// Snapshot subscribers into a pooled slice to avoid pool.Get inside Range.
	entriesPtr := subEntryPool.Get().(*[]subEntry)
	entries := (*entriesPtr)[:0]
	ch.subs.Range(func(id int64, fn func([]byte)) bool {
		entries = append(entries, subEntry{id: id, fn: fn})
		return true
	})
	count := len(entries)
	if count == 0 {
		*entriesPtr = entries
		subEntryPool.Put(entriesPtr)
		return 0, nil
	}

	// Jobs run after Publish returns, so the caller's buffer must not be
	// referenced. With copyPayload off, one owned copy is shared by all
	// subscribers; with it on, each gets an independent copy.
	var shared []byte
	if !ps.copyPayload {
		shared = append([]byte(nil), payload...)
	}

	submitted := 0
	for i := range entries {
		e := &entries[i]
		job := pubsubJobPool.Get().(*pubsubJob)
		job.fn = e.fn
		if ps.copyPayload {
			job.payload = append([]byte(nil), payload...)
		} else {
			job.payload = shared
		}
		job.logger = ps.logger
		job.id = e.id
		job.channel = channel

		if err := ps.pool.Submit(job); err != nil {
			job.fn = nil
			job.payload = nil
			pubsubJobPool.Put(job)
			ps.logger.Fields(
				"error", err,
				"channel", channel,
			).Warn("failed to submit pubsub job to worker pool")
			continue
		}
		submitted++
	}

	// Drop the subscriber references before recycling the snapshot.
	clear(entries)
	*entriesPtr = entries[:0]
	subEntryPool.Put(entriesPtr)

	return submitted, nil
}
