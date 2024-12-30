package enumerators

type rangeEnumerator[T any] struct {
	start   int
	end     int
	current T
	err     error
	factory func(int) T
}

// MoveNext advances the enumerator to the next value in the range.
// It returns true if there are more values to enumerate, and false otherwise.
func (e *rangeEnumerator[T]) MoveNext() bool {
	if e.start < e.end {
		e.current = e.factory(e.start)
		e.start++
		return true
	}
	return false
}

// Current returns the current value from the enumerator.
func (e *rangeEnumerator[T]) Current() (T, error) {
	return e.current, e.err
}

func (e *rangeEnumerator[T]) Err() error {
	return e.err
}

func (e *rangeEnumerator[T]) Dispose() {
	// no-op
}

func Range[T any](seed int, count int, factory func(i int) T) Enumerator[T] {
	return &rangeEnumerator[T]{
		start:   seed,
		end:     seed + count,
		factory: factory,
	}
}
