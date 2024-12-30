package enumerators_test

import (
	"errors"
	"testing"

	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/stretchr/testify/assert"
)

func TestChunk(t *testing.T) {

	// Arrange
	source := enumerators.Slice([]int{1, 2, 3, 4, 5, 6})
	chunks := enumerators.Chunk(source, 5, func(item int) (int, error) { return item, nil })

	// Act
	result, err := enumerators.Collect(chunks)

	// Assert
	assert.Nil(t, err)
	expected := [][]int{
		{1, 2}, // 1 + 2 = 3 (less than target 5)
		{3},    // 3 (exactly hits target 5)
		{4},    // 4 (less than target 5, starts new chunk)
		{5},    // 5 (exactly hits target 5)
		{6},    // 6 (new chunk)
	}

	assert.Equal(t, expected, result)
}

func TestChunk_EmptyInput(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]int{})
	chunks := enumerators.Chunk(source, 5, func(item int) (int, error) { return item, nil })
	// Act
	result, err := enumerators.Collect(chunks)

	// Assert

	assert.Nil(t, err)
	assert.EqualValues(t, 0, len(result))
}

func TestChunk_SingleLargeItem(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]int{10, 2, 3})
	chunks := enumerators.Chunk(source, 5, func(item int) (int, error) { return item, nil })
	// Act
	result, err := enumerators.Collect(chunks)

	// Assert
	assert.Nil(t, err)
	expected := [][]int{
		{10},   // Exceeds target, starts a new chunk
		{2, 3}, // Fits within target
	}

	assert.Equal(t, expected, result)
}

func TestChunk_LargeTarget(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]int{1, 2, 3, 4, 5, 6})
	chunks := enumerators.Chunk(source, 50, func(item int) (int, error) { return item, nil })
	// Act
	result, err := enumerators.Collect(chunks)

	// Assert
	assert.Nil(t, err)
	expected := [][]int{{1, 2, 3, 4, 5, 6}}

	assert.Equal(t, expected, result)
}

func TestChunk_Skip(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]int{1, 2, 3, 4, 5, 6})
	chunks := enumerators.Chunk(source, 5, func(item int) (int, error) { return item, nil })

	// Act
	chunks.MoveNext()

	// Assert
	result, err := enumerators.Collect(chunks)
	assert.Nil(t, err)
	expected := [][]int{
		{3}, // 3 (exactly hits target 5)
		{4}, // 4 (less than target 5, starts new chunk)
		{5}, // 5 (exactly hits target 5)
		{6}, // 6 (new chunk)
	}

	assert.Equal(t, expected, result)
}

func TestChunk_NonUniformSizes(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]int{1, 2, 3, 4, 5})
	chunks := enumerators.Chunk(source, 7, func(item int) (int, error) { return item * 2, nil })

	// Act
	result, err := enumerators.Collect(chunks)

	// Assert
	assert.Nil(t, err)
	expected := [][]int{
		{1, 2}, // 1*2 + 2*2 = 6 (less than 7)
		{3},    // 3*2 = 6 (new chunk)
		{4},    // 4*2 = 8 (new chunk exceeds 7)
		{5},    // 5*2 = 10 (new chunk exceeds 7)
	}
	assert.Equal(t, expected, result)
}

func TestChunk_ItemsEqualTarget(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]int{5, 5, 5})
	chunks := enumerators.Chunk(source, 5, func(item int) (int, error) { return item, nil })

	// Act
	result, err := enumerators.Collect(chunks)

	// Assert
	assert.Nil(t, err)
	expected := [][]int{{5}, {5}, {5}}
	assert.Equal(t, expected, result)
}

func TestChunk_TargetSmallerThanItems(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]int{10, 20, 30})
	chunks := enumerators.Chunk(source, 5, func(item int) (int, error) { return item, nil })

	// Act
	result, err := enumerators.Collect(chunks)

	// Assert
	assert.Nil(t, err)
	expected := [][]int{{10}, {20}, {30}}
	assert.Equal(t, expected, result)
}

func TestChunk_ItemsMatchMultipleTargets(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]int{1, 2, 2, 5, 5})
	chunks := enumerators.Chunk(source, 10, func(item int) (int, error) { return item, nil })

	// Act
	result, err := enumerators.Collect(chunks)

	// Assert
	assert.Nil(t, err)
	expected := [][]int{
		{1, 2, 2, 5}, // 1 + 2 + 2 + 5 = 10
		{5},          // New chunk
	}
	assert.Equal(t, expected, result)
}

type Item struct {
	Weight int
}

func TestChunk_CustomStruct(t *testing.T) {
	// Arrange
	source := enumerators.Slice([]Item{
		{Weight: 3}, {Weight: 4}, {Weight: 2}, {Weight: 1},
	})
	chunks := enumerators.Chunk(source, 5, func(item Item) (int, error) { return item.Weight, nil })

	// Act
	result, err := enumerators.Collect(chunks)

	// Assert
	assert.Nil(t, err)
	expected := [][]Item{
		{{Weight: 3}},
		{{Weight: 4}},
		{{Weight: 2}, {Weight: 1}},
	}
	assert.Equal(t, expected, result)
}

func TestChunk_BubbleError(t *testing.T) {

	// Arrange
	source := enumerators.Slice([]int{1, 2, 3, 4, 5})
	chunks := enumerators.Chunk(source, 5, func(item int) (int, error) {
		if item == 4 {
			return 0, errors.New("bubble error")
		}
		return item, nil
	})
	// Act
	result, err := enumerators.Collect(chunks)

	// Assert
	assert.Equal(t, "bubble error", err.Error())
	expected := [][]int{
		{1, 2},
		{3},
	}

	assert.Equal(t, expected, result)
}
