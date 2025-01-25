package enumerators_test

import (
	"context"
	"testing"

	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/stretchr/testify/assert"
)

func TestGroup(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]int{1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 1, 1})
	groupings := setupSourceAndGroupings(source)

	// Act
	result, err := enumerators.CollectGroupingSlices(groupings)

	// Assert
	assertGroupings(t, result, err)
}

func TestGroupOverChannel(t *testing.T) {
	// Arrange
	source := enumerators.Channel[int](context.Background(), 1)
	groupings := setupSourceAndGroupings(source)

	go func() {
		source.Publish(1)
		source.Publish(2)
		source.Publish(2)
		source.Publish(3)
		source.Publish(3)
		source.Publish(3)
		source.Publish(4)
		source.Publish(4)
		source.Publish(4)
		source.Publish(4)
		source.Publish(5)
		source.Publish(5)
		source.Publish(5)
		source.Publish(5)
		source.Publish(5)
		source.Publish(1)
		source.Publish(1)
		source.Complete()
	}()

	// Act
	result, err := enumerators.CollectGroupingSlices(groupings)

	// Assert
	assertGroupings(t, result, err)
}

func TestGroupOverEmptyClosedChannel(t *testing.T) {
	// Arrange
	source := enumerators.Channel[int](context.Background(), 1)
	groupings := setupSourceAndGroupings(source)

	go func() {
		source.Complete()
	}()

	// Act
	result, err := enumerators.CollectGroupingSlices(groupings)

	// Assert
	assert.Nil(t, err)
	var expected []*enumerators.GroupingSlice[int, int]
	assert.Equal(t, expected, result)
}

func setupSourceAndGroupings(source enumerators.Enumerator[int]) enumerators.Enumerator[*enumerators.Grouping[int, int]] {
	return enumerators.Group(source, func(i int) (int, error) { return i, nil })
}

func assertGroupings(t *testing.T, result []*enumerators.GroupingSlice[int, int], err error) {
	assert.Nil(t, err)
	expected := []*enumerators.GroupingSlice[int, int]{
		{Items: []int{1}, Group: 1},
		{Items: []int{2, 2}, Group: 2},
		{Items: []int{3, 3, 3}, Group: 3},
		{Items: []int{4, 4, 4, 4}, Group: 4},
		{Items: []int{5, 5, 5, 5, 5}, Group: 5},
		{Items: []int{1, 1}, Group: 1},
	}
	assert.Equal(t, expected, result)
}
