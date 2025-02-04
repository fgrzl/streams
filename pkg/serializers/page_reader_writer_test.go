package serializers_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/pkg/serializers"
	"github.com/fgrzl/streams/pkg/util"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create test entries
func GetEntries(count int) enumerators.Enumerator[*models.Entry] {
	return enumerators.Range(0, count, func(i int) *models.Entry {
		return &models.Entry{
			Sequence:  uint64(i),
			Timestamp: util.GetTimestamp(),
			Payload:   []byte(fmt.Sprintf("my test entry : %d020", i))}
	})
}
func TestPageReader(t *testing.T) {

	t.Run("should read all entries", func(t *testing.T) {
		// Arrange

		count := 50_000
		entries := GetEntries(count)
		pageFile, err := os.CreateTemp("", "pageReader_test_*.bin")
		page, err := serializers.Write(entries, pageFile)
		require.NoError(t, err)
		require.NotNil(t, page)

		defer os.Remove(pageFile.Name())

		pageFile, err = os.Open(pageFile.Name())
		require.NoError(t, err)
		defer pageFile.Close()
		defer os.Remove(pageFile.Name())

		// Act
		pageReader := serializers.NewPageReader(pageFile, 0)

		// Assert
		results, err := enumerators.ToSlice(pageReader)
		assert.NoError(t, err)
		assert.EqualValues(t, count, len(results))
	})

	t.Run("should read starting at position", func(t *testing.T) {
		// Arrange
		count := 500_000
		entries := GetEntries(count)
		pageFile, err := os.CreateTemp("", "pageReader_test_*.bin")
		page, err := serializers.Write(entries, pageFile)
		require.NoError(t, err)
		require.NotNil(t, page)

		defer os.Remove(pageFile.Name())

		pageFile, err = os.Open(pageFile.Name())
		require.NoError(t, err)
		defer pageFile.Close()
		defer os.Remove(pageFile.Name())

		seq, pos := page.FindNearestKey(480_000)

		// Act
		pageReader := serializers.NewPageReader(pageFile, pos)

		// Assert
		results, err := enumerators.ToSlice(pageReader)
		assert.NoError(t, err)
		assert.EqualValues(t, uint64(count)-seq, len(results))
	})
}

func TestWrite(t *testing.T) {

	t.Run("should write test file", func(t *testing.T) {

		// Arrange
		tmpFile, err := os.CreateTemp("", "pageWriter_test_*.bin")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		count := 1_000_000
		entries := GetEntries(count)

		// Act
		page, err := serializers.Write(entries, tmpFile)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, page)
		assert.EqualValues(t, count, page.Count)
	})

	t.Run("should not write empty file", func(t *testing.T) {
		// Arrange
		count := 0
		entries := GetEntries(count)

		tempFile, err := os.CreateTemp("", "pageWriter_test_empty_*.bin")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		// Act
		page, err := serializers.Write(entries, tempFile)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, page)
		assert.EqualValues(t, count, page.Count)
	})
}
