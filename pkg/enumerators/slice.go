package enumerators

type SliceEnumerator[T any] struct {
	slice   []T
	cursor  int
	current T
	err     error
}

func (e *SliceEnumerator[T]) MoveNext() bool {
	e.cursor++
	if e.cursor >= len(e.slice) {
		return false
	}
	e.current = e.slice[e.cursor]
	return true
}

func (e *SliceEnumerator[T]) Current() (T, error) {
	return e.current, e.err
}

func (e *SliceEnumerator[T]) Err() error {
	return e.err
}

func (enumerator *SliceEnumerator[T]) Dispose() {
	// no-op
}

func Slice[T any](slice []T) Enumerator[T] {
	return &SliceEnumerator[T]{
		slice:  slice,
		cursor: -1,
	}
}

func ToSlice[T any](enumerator Enumerator[T]) ([]T, error) {
	defer enumerator.Dispose()
	if sliceEnum, ok := enumerator.(*SliceEnumerator[T]); ok {
		return sliceEnum.slice, nil
	}

	var slice []T
	for enumerator.MoveNext() {
		item, err := enumerator.Current()
		if err != nil {
			return slice, err
		}

		slice = append(slice, item)
	}

	return slice, enumerator.Err()
}
