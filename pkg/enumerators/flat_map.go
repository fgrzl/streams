package enumerators

import "fmt"

type flatMapEnumerator[T any, U any] struct {
	base    Enumerator[T]
	mapper  func(T) Enumerator[U]
	current Enumerator[U]
	err     error
}

func (e *flatMapEnumerator[T, U]) MoveNext() bool {
	// If we have a current enumerator, try to advance it
	for {
		if e.current != nil {
			if e.current.MoveNext() {
				return true
			}
			e.current.Dispose()
			e.current = nil
		}

		// Move to the next item in the base enumerator
		if !e.base.MoveNext() {
			e.err = e.base.Err()
			return false
		}

		// Get the next enumerator from the mapper
		item, err := e.base.Current()
		if err != nil {
			return false
		}
		e.current = e.mapper(item)
	}
}

func (e *flatMapEnumerator[T, U]) Current() (U, error) {
	if e.current == nil {
		var zero U
		return zero, fmt.Errorf("no current item")
	}
	return e.current.Current()
}

func (e *flatMapEnumerator[T, U]) Err() error {
	return e.err
}

func (e *flatMapEnumerator[T, U]) Dispose() {
	e.base.Dispose()
	if e.current != nil {
		e.current.Dispose()
	}
}

// FlatMap creates a flat-mapped enumerator
func FlatMap[T any, U any](parent Enumerator[T], mapper func(T) Enumerator[U]) Enumerator[U] {
	return &flatMapEnumerator[T, U]{
		base:   parent,
		mapper: mapper,
	}
}
