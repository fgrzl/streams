package enumerators

type filterMapper[TIn any, TOut any] struct {
	base    Enumerator[TIn]
	apply   func(TIn) (TOut, error, bool)
	current TOut
	err     error
}

func (e *filterMapper[TIn, TOut]) MoveNext() bool {
	for {
		if !e.base.MoveNext() {
			return false
		}

		item, err := e.base.Current()
		if err != nil {
			e.err = err
			return false
		}

		u, err, ok := e.apply(item)

		if err != nil {
			e.err = err
			return false
		}

		if !ok {
			continue
		}

		e.err = err
		e.current = u
		return true
	}
}

func (e *filterMapper[TIn, TOut]) Current() (TOut, error) {
	return e.current, e.err
}

func (e *filterMapper[TIn, TOut]) Err() error {
	return e.err
}

func (e *filterMapper[TIn, TOut]) Dispose() {
	e.base.Dispose()
}

// Map creates a mapped enumerator
func FilterMap[TIn any, TOut any](enumerator Enumerator[TIn], apply func(TIn) (TOut, error, bool)) Enumerator[TOut] {
	return &filterMapper[TIn, TOut]{
		base:  enumerator,
		apply: apply,
	}
}
