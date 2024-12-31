package enumerators

type channelEnumerator[T any] struct {
	ch      <-chan T
	current T
	err     error
	done    bool
}

// MoveNext advances the enumerator to the next value in the range.
// It returns true if there are more values to enumerate, and false otherwise.
func (e *channelEnumerator[T]) MoveNext() bool {
	if e.done {
		return false
	}

	// Attempt to receive from the channel
	val, ok := <-e.ch
	if !ok {
		e.done = true // Mark as done if the channel is closed
		return false
	}

	e.current = val
	return true
}

// Current returns the current value from the enumerator.
func (e *channelEnumerator[T]) Current() (T, error) {
	return e.current, e.err
}

// Err returns any error encountered during enumeration.
func (e *channelEnumerator[T]) Err() error {
	return e.err
}

// Dispose cleans up the enumerator. No-op for channel enumerator.
func (e *channelEnumerator[T]) Dispose() {
	// no-op, but could close resources if needed in future
}

// Channel creates an Enumerator from a channel.
func Channel[T any](ch <-chan T) Enumerator[T] {
	return &channelEnumerator[T]{
		ch: ch,
	}
}
