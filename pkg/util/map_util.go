package util

import (
	"sort"

	"golang.org/x/exp/constraints"
)

func GetSortedKeys[TKey constraints.Ordered, TVal any](stores map[TKey]TVal) []TKey {
	// Step 1: Extract keys into a slice
	keys := make([]TKey, 0, len(stores))
	for key := range stores {
		keys = append(keys, key)
	}

	// Step 2: Sort the keys
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Step 3: Return the sorted slice
	return keys
}

func GetSortedValues[TKey constraints.Ordered, TVal any](stores map[TKey]TVal) []TVal {
	// Step 1: Extract keys into a slice
	keys := make([]TKey, 0, len(stores))
	for key := range stores {
		keys = append(keys, key)
	}

	// Step 2: Sort the keys
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Step 3: Collect values in sorted order
	values := make([]TVal, 0, len(stores))
	for _, key := range keys {
		values = append(values, stores[key])
	}

	// Step 4: Return the sorted values
	return values
}
