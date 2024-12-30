package enumerators

type mapEnumerator[T any, U any] struct {
	base    Enumerator[T]
	mapper  func(T) (U, error)
	current U
	err     error
}

func (e *mapEnumerator[T, U]) MoveNext() bool {
	if !e.base.MoveNext() {
		return false
	}

	item, err := e.base.Current()
	if err != nil {
		e.err = err
		return false
	}

	u, err := e.mapper(item)
	if err != nil {
		e.err = err
		return false
	}
	e.current = u
	return true
}

func (e *mapEnumerator[T, U]) Current() (U, error) {
	return e.current, e.err
}

func (e *mapEnumerator[T, U]) Err() error {
	return e.err
}

func (e *mapEnumerator[T, U]) Dispose() {
	e.base.Dispose()
}

// Map creates a mapped enumerator
func Map[T any, U any](enumerator Enumerator[T], mapper func(T) (U, error)) Enumerator[U] {
	return &mapEnumerator[T, U]{
		base:   enumerator,
		mapper: mapper,
	}
}
