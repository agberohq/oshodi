package messaging

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/agberohq/oshodi/internal/pipeline"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
	"github.com/olekukonko/mappo"
)

// pubsubJob is a reusable task to completely eliminate closure allocations during publish
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
		// Clean up and recycle back to the pool
		j.fn = nil
		j.payload = nil
		pubsubJobPool.Put(j)
	}()

	j.fn(j.payload)
	return nil
}

var pubsubJobPool = sync.Pool{
	New: func() any {
		return &pubsubJob{}
	},
}

// Channel holds subscribers for a specific bucket+channel
type Channel struct {
	subs *mappo.Sharded[int64, func([]byte)]
}

// PubSub is a high-performance publish-subscribe system
type PubSub struct {
	channels *mappo.Sharded[string, *Channel]
	pool     *jack.Pool
	logger   *ll.Logger
	nextID   atomic.Int64
	hooks    *pipeline.Hooks
}

// NewPubSub creates a new PubSub instance
func NewPubSub(pool *jack.Pool, logger *ll.Logger, hooks *pipeline.Hooks) *PubSub {
	if logger == nil {
		logger = ll.New("pubsub").Disable()
	}
	return &PubSub{
		channels: mappo.NewSharded[string, *Channel](),
		pool:     pool,
		logger:   logger,
		hooks:    hooks,
	}
}

func chanKey(bucket, channel string) string {
	if bucket == "" {
		return "\x00" + channel
	}
	return bucket + "\x00" + channel
}

// Subscribe adds a subscriber and returns its ID and an unsubscribe function
func (ps *PubSub) Subscribe(bucket, channel string, fn func([]byte)) (int64, func()) {
	key := chanKey(bucket, channel)
	id := ps.nextID.Add(1)

	// Load or create channel atomically
	ch, loaded := ps.channels.Get(key)
	if !loaded {
		ch = &Channel{
			subs: mappo.NewSharded[int64, func([]byte)](),
		}
		ps.channels.SetIfAbsent(key, ch)
		ch, _ = ps.channels.Get(key)
	}

	ch.subs.Set(id, fn)

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
	if ps.hooks != nil && ps.hooks.OnUnsubscribe != nil {
		ps.hooks.OnUnsubscribe(bucket, channel, id)
	}
}

// UnsubscribeAll removes all subscribers from a channel
func (ps *PubSub) UnsubscribeAll(bucket, channel string) {
	key := chanKey(bucket, channel)
	if ch, ok := ps.channels.Get(key); ok {
		ps.channels.Delete(key)
		if ps.hooks != nil && ps.hooks.OnUnsubscribe != nil {
			ch.subs.Range(func(id int64, _ func([]byte)) bool {
				ps.hooks.OnUnsubscribe(bucket, channel, id)
				return true
			})
		}
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

// Publish sends a payload to all subscribers of a channel
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

	// Count subscribers first
	count := ch.subs.Len()
	if count == 0 {
		return 0, nil
	}

	// Iterate and submit jobs - Sharded.Range is already optimized
	ch.subs.Range(func(id int64, fn func([]byte)) bool {
		job := pubsubJobPool.Get().(*pubsubJob)
		job.fn = fn
		job.payload = payload
		job.logger = ps.logger
		job.id = id
		job.channel = channel

		// Submit asynchronously
		if err := ps.pool.Submit(job); err != nil {
			// On error, recycle and continue
			job.fn = nil
			job.payload = nil
			pubsubJobPool.Put(job)
			ps.logger.Fields(
				"error", err,
				"channel", channel,
			).Warn("failed to submit pubsub job to worker pool")
		}
		return true
	})

	return count, nil
}
