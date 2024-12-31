package enumerators

import (
	"context"
	"sync"
)

type channelEnumerator[T any] struct {
	context context.Context
	dataCh  chan T
	errCh   chan error
	doneCh  chan struct{}
	current T
	err     error
	once    sync.Once
}

// MoveNext advances the enumerator to the next value in the range.
// Returns true if more values are available, false otherwise.
func (e *channelEnumerator[T]) MoveNext() bool {
	select {
	case <-e.doneCh:
		return false
	case err, ok := <-e.errCh:
		if ok {
			e.err = err
			e.Complete() // Signal completion on error
			return false
		}
	case data, ok := <-e.dataCh:
		if ok {
			e.current = data
			return true
		}
		e.Complete() // Signal completion when data channel is closed
	}
	return false
}

// Current returns the current value and any error encountered.
func (e *channelEnumerator[T]) Current() (T, error) {
	return e.current, e.err
}

// Err returns any error encountered during enumeration.
func (e *channelEnumerator[T]) Err() error {
	return e.err
}

// Dispose cleans up resources and signals termination.
func (e *channelEnumerator[T]) Dispose() {
	e.once.Do(func() {
		close(e.doneCh)
	})
}

// Publish sends a value to the enumerator for consumption.
func (e *channelEnumerator[T]) Publish(msg T) bool {
	select {
	case <-e.context.Done():
		return false // Context canceled
	case <-e.doneCh:
		return false // Enumerator completed
	case e.dataCh <- msg:
		return true
	}
}

// Error signals an error to the enumerator.
func (e *channelEnumerator[T]) Error(err error) {
	select {
	case <-e.context.Done():
		// Context canceled; error won't be sent.
	case e.errCh <- err:
		// Successfully sent error.
	default:
		// Avoid blocking if the error channel is already consumed.
	}
}

// Complete signals that no more values will be sent.
func (e *channelEnumerator[T]) Complete() {
	e.once.Do(func() {
		close(e.doneCh)
		close(e.dataCh)
		close(e.errCh)
	})
}

// Channel creates a new channel-based enumerator.
func Channel[T any](ctx context.Context, size int) *channelEnumerator[T] {
	return &channelEnumerator[T]{
		context: ctx,
		dataCh:  make(chan T, size),
		errCh:   make(chan error, 1),
		doneCh:  make(chan struct{}),
	}
}
