package enumerators

type filterEnumerator[T any] struct {
	base    Enumerator[T]
	filter  func(T) bool
	current T
	err     error
}

func (e *filterEnumerator[T]) MoveNext() bool {
	for {
		if !e.base.MoveNext() {
			return false
		}

		item, err := e.base.Current()
		if err != nil {
			e.err = err
			return false
		}

		if !e.filter(item) {
			continue
		}

		e.current = item
		return true
	}
}

func (e *filterEnumerator[T]) Current() (T, error) {
	return e.current, e.err
}

func (e *filterEnumerator[T]) Err() error {
	return e.err
}

func (e *filterEnumerator[T]) Dispose() {
	e.base.Dispose()
}

// filter creates a mapped enumerator
func Filter[T any](parent Enumerator[T], filter func(T) bool) Enumerator[T] {
	return &filterEnumerator[T]{
		base:   parent,
		filter: filter,
	}
}
