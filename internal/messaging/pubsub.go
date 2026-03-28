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

// Channel holds all subscribers for a specific channel name in a lock-free map.
type Channel struct {
	// Using a lock-free map here replaces the slice copy-on-write pattern,
	// eliminating all memory allocations during Subscribe/Unsubscribe.
	subs *mappo.Concurrent[int64, func([]byte)]
}

// PubSub manages all channel subscriptions.
// It relies entirely on mappo.Concurrent (xsync) for lock-free, zero-contention scaling,
// completely eliminating the need for manual array sharding and coarse-grained locks.
type PubSub struct {
	channels *mappo.Concurrent[string, *Channel]
	pool     *jack.Pool
	logger   *ll.Logger
	nextID   atomic.Int64
	hooks    *pipeline.Hooks
}

// NewPubSub creates a new highly concurrent, lock-free PubSub instance.
func NewPubSub(pool *jack.Pool, logger *ll.Logger, hooks *pipeline.Hooks) *PubSub {
	if logger == nil {
		logger = ll.New("pubsub").Disable()
	}

	return &PubSub{
		channels: mappo.NewConcurrent[string, *Channel](),
		pool:     pool,
		logger:   logger,
		hooks:    hooks,
	}
}

// chanKey formats the internal map key.
func chanKey(bucket, channel string) string {
	if bucket == "" {
		return "default\x00" + channel
	}
	return bucket + "\x00" + channel
}

// Subscribe registers a subscription to the specified bucket and channel with a callback function and returns an ID and an unsubscribe function.
func (ps *PubSub) Subscribe(bucket, channel string, fn func([]byte)) (int64, func()) {
	key := chanKey(bucket, channel)
	id := ps.nextID.Add(1)

	// LoadOrStore returns the actual stored value (existing or new)
	ch, _ := ps.channels.LoadOrStore(key, &Channel{
		subs: mappo.NewConcurrent[int64, func([]byte)](),
	})

	// Guaranteed non-nil
	ch.subs.Set(id, fn)

	if ps.hooks != nil && ps.hooks.OnSubscribe != nil {
		ps.hooks.OnSubscribe(bucket, channel, id)
	}

	var once sync.Once
	return id, func() {
		once.Do(func() { ps.unsubscribeByID(bucket, channel, id) })
	}
}

// SubscribeSimple is the original API for when the ID is not needed.
func (ps *PubSub) SubscribeSimple(bucket, channel string, fn func([]byte)) func() {
	_, unsub := ps.Subscribe(bucket, channel, fn)
	return unsub
}

// unsubscribeByID removes a specific subscription lock-free.
func (ps *PubSub) unsubscribeByID(bucket, channel string, id int64) {
	key := chanKey(bucket, channel)

	if ch, ok := ps.channels.Get(key); ok {
		// O(1) Lock-Free deletion
		ch.subs.Delete(id)
	}

	if ps.hooks != nil && ps.hooks.OnUnsubscribe != nil {
		ps.hooks.OnUnsubscribe(bucket, channel, id)
	}
}

// UnsubscribeAll removes every subscriber from a bucket/channel.
func (ps *PubSub) UnsubscribeAll(bucket, channel string) {
	key := chanKey(bucket, channel)

	if ch, ok := ps.channels.Get(key); ok {
		ps.channels.Delete(key) // Remove the channel entirely

		// Trigger hooks for all removed subscribers
		if ps.hooks != nil && ps.hooks.OnUnsubscribe != nil {
			ch.subs.Range(func(id int64, _ func([]byte)) bool {
				ps.hooks.OnUnsubscribe(bucket, channel, id)
				return true
			})
		}
	}
}

// Count returns the number of active subscribers lock-free.
func (ps *PubSub) Count(bucket, channel string) int {
	key := chanKey(bucket, channel)
	if ch, ok := ps.channels.Get(key); ok {
		return ch.subs.Len()
	}
	return 0
}

// IDs returns a snapshot of all active subscription IDs.
func (ps *PubSub) IDs(bucket, channel string) []int64 {
	key := chanKey(bucket, channel)
	if ch, ok := ps.channels.Get(key); ok {
		return ch.subs.Keys()
	}
	return nil
}

// Publish dispatches payload to all subscribers rapidly using the injected jack.Pool.
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

	count := 0

	// Lock-free iteration over all subscribers
	ch.subs.Range(func(id int64, fn func([]byte)) bool {
		count++

		// Dispatch to the worker pool asynchronously.
		// Note: We DO NOT allocate or copy `payload` here.
		// Handlers are expected to treat `payload` as read-only.
		ps.pool.Do(func() {
			defer func() {
				if r := recover(); r != nil {
					ps.logger.Fields(
						"subscriber_id", id,
						"channel", channel,
						"panic", r,
						"stack", string(debug.Stack()),
					).Error("pubsub panic")
				}
			}()

			fn(payload)
		})

		return true // continue iteration
	})

	return count, nil
}
