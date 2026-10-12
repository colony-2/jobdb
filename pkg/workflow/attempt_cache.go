package workflow

import (
	"context"
	"sync"
)

// attemptCache belongs to one attemptRunner, never to the lease or durable job.
// Serializing reads coalesces successful first fetches without caching failures.
// Waiting callers can cancel independently; lease renewal uses a separate lock.
type attemptCache[T any] struct {
	once  sync.Once
	gate  chan struct{}
	value T
}

func (c *attemptCache[T]) lock(ctx context.Context) error {
	c.once.Do(func() { c.gate = make(chan struct{}, 1) })
	select {
	case c.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			c.unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *attemptCache[T]) unlock() { <-c.gate }
