package enumerators

import (
	"golang.org/x/exp/constraints"
)

// Sum creates an enumerator that returns the sum of elements
func Sum[T any, TSum constraints.Ordered](enumerator Enumerator[T], selector func(item T) (TSum, error)) (TSum, error) {
	defer enumerator.Dispose()
	var sum TSum
	for enumerator.MoveNext() {

		item, err := enumerator.Current()
		if err != nil {
			var zero TSum
			return zero, err
		}

		value, err := selector(item)
		if err != nil {
			var zero TSum
			return zero, err
		}
		sum += value
	}

	return sum, nil
}
