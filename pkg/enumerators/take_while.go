package enumerators

type takeWhileEnumerator[T any] struct {
	base      Enumerator[T]
	condition func(T) bool
	current   T
	err       error
}

func (e *takeWhileEnumerator[T]) MoveNext() bool {
	for {
		if !e.base.MoveNext() {
			return false
		}

		item, err := e.base.Current()
		if err != nil {
			e.err = err
			return false
		}

		if !e.condition(item) {
			continue
		}

		e.current = item
		return true
	}
}

func (e *takeWhileEnumerator[T]) Current() (T, error) {
	return e.current, e.err
}

func (e *takeWhileEnumerator[T]) Err() error {
	return e.err
}

func (e *takeWhileEnumerator[T]) Dispose() {
	e.base.Dispose()
}

// take the item if the contition is true
func TakeWhile[T any](enumerator Enumerator[T], condition func(T) bool) Enumerator[T] {
	return &takeWhileEnumerator[T]{
		base:      enumerator,
		condition: condition,
	}
}
