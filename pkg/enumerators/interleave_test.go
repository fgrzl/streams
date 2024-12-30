package enumerators_test

import (
	"testing"

	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/stretchr/testify/assert"
)

// TestInterleave_Basic tests the interleaving of three slice enumerators.
func TestInterleave_Basic(t *testing.T) {

	// Arrange
	enumerator1 := enumerators.Slice([]int{1, 4, 7})
	enumerator2 := enumerators.Slice([]int{2, 5, 8})
	enumerator3 := enumerators.Slice([]int{3, 6, 9})

	// Act
	interleaved := enumerators.Interleave([]enumerators.Enumerator[int]{enumerator1, enumerator2, enumerator3}, func(item int) int { return item })

	// Assert
	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	results, err := enumerators.ToSlice(interleaved)
	assert.Nil(t, err)
	assert.Equal(t, expected, results, "Interleaved result does not match expected output")
}

// TestInterleave_EmptyInput tests interleaving with no input enumerators.
func TestInterleave_EmptyInput(t *testing.T) {
	interleaved := enumerators.Interleave([]enumerators.Enumerator[int]{}, func(item int) int { return item })

	assert.False(t, interleaved.MoveNext(), "Expected no items in the interleaved enumerator")
}

// TestInterleave_OverlappingKeys tests interleaving with overlapping key values.
func TestInterleave_OverlappingKeys(t *testing.T) {
	enumerator1 := enumerators.Slice([]int{2, 4})
	enumerator2 := enumerators.Slice([]int{2, 3})

	interleaved := enumerators.Interleave([]enumerators.Enumerator[int]{
		enumerator1, enumerator2,
	}, func(item int) int {
		return item
	})

	expected := []int{2, 2, 3, 4}
	var result []int

	for interleaved.MoveNext() {
		item, err := interleaved.Current()
		assert.NoError(t, err, "Unexpected error retrieving current item")
		result = append(result, item)
	}

	assert.Equal(t, expected, result, "Interleaved result does not match expected output")
}

// TestInterleave_SingleEnumerator tests interleaving with a single enumerator.
func TestInterleave_SingleEnumerator(t *testing.T) {
	enumerator := enumerators.Slice[int]([]int{1, 2, 3})

	interleaved := enumerators.Interleave([]enumerators.Enumerator[int]{
		enumerator,
	}, func(item int) int {
		return item
	})

	expected := []int{1, 2, 3}
	results, err := enumerators.ToSlice(interleaved)
	assert.Nil(t, err)
	assert.Equal(t, expected, results, "Interleaved result does not match expected output")
}
