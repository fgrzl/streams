package streams_test

import (
	"runtime"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streams"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProduce(t *testing.T) {
	for name, bus := range configurations(t) {
		t.Run("should produce "+name, func(t *testing.T) {

			// Arrange
			client := streams.NewClient(bus)
			ctx := t.Context()
			space, segment, records := "space0", "segment0", generateRange(0, 5)

			// Act
			results := client.Produce(ctx, space, segment, records)
			statuses, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, statuses, 1)
		})
	}
}

func TestGetSpaces(t *testing.T) {
	for name, bus := range configurations(t) {
		t.Run("should get spaces "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			client := streams.NewClient(bus)
			setupConsumerData(t, client)

			// Act
			enumerator := client.GetSpaces(ctx)
			spaces, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, spaces, 5)
			assert.Equal(t, "space0", spaces[0])
			assert.Equal(t, "space1", spaces[1])
			assert.Equal(t, "space2", spaces[2])
			assert.Equal(t, "space3", spaces[3])
			assert.Equal(t, "space4", spaces[4])
		})
	}
}

func TestGetSegments(t *testing.T) {
	for name, bus := range configurations(t) {
		t.Run("should get segments "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			client := streams.NewClient(bus)
			setupConsumerData(t, client)

			// Act
			enumerator := client.GetSegments(ctx, "space0")
			segments, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, segments, 5)
			assert.Equal(t, "segment0", segments[0])
			assert.Equal(t, "segment1", segments[1])
			assert.Equal(t, "segment2", segments[2])
			assert.Equal(t, "segment3", segments[3])
			assert.Equal(t, "segment4", segments[4])
		})
	}
}

func TestPeek(t *testing.T) {
	for name, bus := range configurations(t) {
		t.Run("should peek "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			client := streams.NewClient(bus)
			setupConsumerData(t, client)

			// Act
			peek, err := client.Peek(ctx, "space0", "segment0")

			// Assert
			assert.NoError(t, err)
			assert.Equal(t, "space0", peek.Space)
			assert.Equal(t, "segment0", peek.Segment)
			assert.Equal(t, uint64(1_000), peek.Sequence)
		})
	}
}

func TestConsumeSegment(t *testing.T) {
	for name, bus := range configurations(t) {
		t.Run("should consume segment "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			client := streams.NewClient(bus)
			setupConsumerData(t, client)

			args := &streams.ConsumeSegment{
				Space:   "space0",
				Segment: "segment0",
			}

			// Act
			results := client.ConsumeSegment(ctx, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 1_000)
		})
	}
}

func TestConsumeSpace(t *testing.T) {
	for name, bus := range configurations(t) {
		t.Run("should consume space "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			client := streams.NewClient(bus)
			setupConsumerData(t, client)

			args := &streams.ConsumeSpace{
				Space: "space0",
			}

			// Act
			results := client.ConsumeSpace(ctx, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 5_000)
		})
	}
}

func TestConsume(t *testing.T) {
	for name, bus := range configurations(t) {
		t.Run("should consume interleaved spaces "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			client := streams.NewClient(bus)
			setupConsumerData(t, client)
			runtime.Gosched()

			args := &streams.Consume{
				Offsets: map[string]lexkey.LexKey{
					"space0": {},
					"space1": {},
					"space2": {},
					"space3": {},
					"space4": {},
				},
			}

			// Act
			results := client.Consume(ctx, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 25_000)
		})
	}
}

func TestAddNewNode(t *testing.T) {
	for name, bus := range configurations(t) {
		t.Run("should consume space "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			client := streams.NewClient(bus)
			setupConsumerData(t, client)

			status, err := client.GetClusterStatus(ctx)
			require.NoError(t, err)

			newNode := newInstance(t, bus)

			updatedStatus, err := client.GetClusterStatus(ctx)
			require.NoError(t, err)
			require.NotEqual(t, status.NodeCount+1, updatedStatus.NodeCount)

			args := &streams.ConsumeSpace{
				Space: "space0",
			}

			// Act
			results := newNode.Service.ConsumeSpace(ctx, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 5_000)
		})
	}
}
