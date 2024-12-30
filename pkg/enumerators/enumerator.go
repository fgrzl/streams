package enumerators

import (
	"github.com/fgrzl/woolf/pkg/util"
)

// Enumerator interface for generic iteration.
type Enumerator[T any] interface {
	util.Disposable
	MoveNext() bool
	Current() (T, error)
	Err() error
}

// Empty handles cases where no enumerators are provided
type emptyEnumerator[T any] struct{}

func (e *emptyEnumerator[T]) MoveNext() bool {
	return false
}

func (e *emptyEnumerator[T]) Current() (T, error) {
	var zero T
	return zero, nil
}

func (e *emptyEnumerator[T]) Err() error {
	return nil
}

func (e *emptyEnumerator[T]) Dispose() {}

func Empty[T any]() Enumerator[T] {
	return &emptyEnumerator[T]{}
}

// cleanupEnumerator ensures cleanup always occurs.
type cleanupEnumerator[T any] struct {
	base        Enumerator[T]
	cleanup     func() // Cleanup function to be executed
	cleanupDone bool   // Prevent multiple executions of cleanup
}

// Perform a cleanup method when the enumrator is complete
func Cleanup[T any](enumerator Enumerator[T], cleanup func()) *cleanupEnumerator[T] {
	return &cleanupEnumerator[T]{base: enumerator, cleanup: cleanup}
}

// MoveNext moves to the next element.
func (e *cleanupEnumerator[T]) MoveNext() bool {
	return e.base.MoveNext()
}

// Current returns the current element.
func (e *cleanupEnumerator[T]) Current() (T, error) {
	return e.base.Current()
}

// Err returns any errors encountered.
func (e *cleanupEnumerator[T]) Err() error {
	return e.base.Err()
}

// Dispose ensures cleanup is performed.
func (e *cleanupEnumerator[T]) Dispose() {
	if !e.cleanupDone {
		e.cleanupDone = true
		e.base.Dispose() // Dispose the inner enumerator
		if e.cleanup != nil {
			e.cleanup() // Execute the cleanup function
		}
	}
}
