package pagination

import (
	"context"
	"errors"
	"fmt"
)

// ErrNoMoreItems indicates the iterator has been fully consumed.
var ErrNoMoreItems = errors.New("jobdb chapterstore: no more items")

// ErrInfiniteLoop indicates the iterator detected an infinite pagination loop.
var ErrInfiniteLoop = errors.New("jobdb chapterstore: infinite pagination loop detected")

// Iterator is a pull-based pagination helper.
type Iterator[T any] interface {
	Next(ctx context.Context) (T, error)
	HasNext() bool
}

// FetchPageFunc returns the next page of results along with the opaque next page token.
type FetchPageFunc[T any] func(ctx context.Context, pageToken *string) ([]T, *string, error)

// NewIterator creates an Iterator backed by the provided fetch function.
func NewIterator[T any](fetch FetchPageFunc[T]) Iterator[T] {
	return &iterator[T]{fetch: fetch}
}

type iterator[T any] struct {
	fetch FetchPageFunc[T]

	items         []T
	nextPage      *string
	prevToken     *string
	sameTokenSeen int
	emptyPageSeen int
	exhaust       bool
}

const (
	maxSameToken  = 3  // Maximum times we'll accept the same token
	maxEmptyPages = 10 // Maximum consecutive empty pages before giving up
)

func (it *iterator[T]) HasNext() bool {
	if len(it.items) > 0 {
		return true
	}
	return !it.exhaust
}

func (it *iterator[T]) Next(ctx context.Context) (T, error) {
	var zero T
	if len(it.items) == 0 {
		if it.exhaust {
			return zero, ErrNoMoreItems
		}

		// Detect if we're about to send the same token we just sent
		if it.prevToken != nil && it.nextPage != nil && *it.prevToken == *it.nextPage {
			it.sameTokenSeen++
			if it.sameTokenSeen >= maxSameToken {
				return zero, fmt.Errorf("%w: server returned same page token %d times", ErrInfiniteLoop, it.sameTokenSeen)
			}
		} else {
			it.sameTokenSeen = 0
		}

		// Track the token we're about to send
		it.prevToken = it.nextPage

		items, token, err := it.fetch(ctx, it.nextPage)
		if err != nil {
			return zero, err
		}

		// Check if server returned the same token we just sent (infinite loop)
		if token != nil && it.nextPage != nil && *token == *it.nextPage {
			return zero, fmt.Errorf("%w: server returned same page token as input", ErrInfiniteLoop)
		}

		it.items = items
		it.nextPage = token

		if len(it.items) == 0 && token == nil {
			it.exhaust = true
			return zero, ErrNoMoreItems
		}

		if len(it.items) == 0 {
			// Empty page but more pages available
			it.emptyPageSeen++
			if it.emptyPageSeen >= maxEmptyPages {
				return zero, fmt.Errorf("%w: received %d consecutive empty pages", ErrInfiniteLoop, it.emptyPageSeen)
			}
			// No items in this page but more tokens remain; fetch the next page recursively.
			return it.Next(ctx)
		}

		// Got items, reset empty page counter
		it.emptyPageSeen = 0

		if token == nil {
			it.exhaust = true
		}
	}

	next := it.items[0]
	it.items = it.items[1:]
	return next, nil
}
