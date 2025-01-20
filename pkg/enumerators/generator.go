package enumerators

import "errors"

// Generator generates values continuously.
type Generator[T any] struct {
	Enumerator[T]
	generateFunc func() T
	current      T
	err          error
	disposed     bool
}

// Create a new generator.
func NewGenerator[T any](generateFunc func() T) Enumerator[T] {
	return &Generator[T]{generateFunc: generateFunc}
}

// Dispose cleans up the enumerator.
func (ce *Generator[T]) Dispose() {
	ce.disposed = true
}

// MoveNext generates the next value.
func (ce *Generator[T]) MoveNext() bool {
	if ce.disposed {
		ce.err = errors.New("enumerator disposed")
		return false
	}

	// Generate the next value
	ce.current = ce.generateFunc()
	return true
}

// Current returns the current value or an error if disposed.
func (ce *Generator[T]) Current() (T, error) {
	if ce.disposed {
		var zero T
		return zero, errors.New("enumerator disposed")
	}
	return ce.current, nil
}

// Err returns the last error.
func (ce *Generator[T]) Err() error {
	return ce.err
}
