package enumerators

type errorEnumerator[T any] struct {
	err error
}

func (e *errorEnumerator[T]) MoveNext() bool {
	return false
}

func (e *errorEnumerator[T]) Current() (T, error) {
	var zero T
	return zero, e.err
}

func (e *errorEnumerator[T]) Dispose() {
	// No resources to clean up
}

func (e *errorEnumerator[T]) Err() error {
	return e.err
}

// Error creates an enumerator that immediately returns an error.
func Error[T any](err error) Enumerator[T] {
	return &errorEnumerator[T]{err: err}
}
