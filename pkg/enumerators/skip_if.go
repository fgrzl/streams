package enumerators

type skipIfEnumerator[T any] struct {
	base      Enumerator[T]
	condition func(T) bool
	current   T
	err       error
}

func (e *skipIfEnumerator[T]) MoveNext() bool {
	for {
		if !e.base.MoveNext() {
			return false
		}

		item, err := e.base.Current()
		if err != nil {
			e.err = err
			return false
		}

		if e.condition(item) {
			continue
		}

		e.current = item
		return true
	}
}

func (e *skipIfEnumerator[T]) Current() (T, error) {
	return e.current, e.err
}

func (e *skipIfEnumerator[T]) Err() error {
	return e.err
}

func (e *skipIfEnumerator[T]) Dispose() {
	e.base.Dispose()
}

// skips the item if the contition is true
func SkipIf[T any](enumerator Enumerator[T], condition func(T) bool) Enumerator[T] {
	return &skipIfEnumerator[T]{
		base:      enumerator,
		condition: condition,
	}
}
