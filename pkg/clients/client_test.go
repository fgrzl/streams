package clients_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fgrzl/streams/pkg/clients"
	"github.com/fgrzl/streams/pkg/config"
	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/grpc"
	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/test"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestServer(t *testing.T) (context.Context, clients.WoolfClient) {

	tmp := filepath.Join(os.TempDir(), "woolf-client-tests", uuid.NewString())
	os.MkdirAll(tmp, 0755)

	config.SetFileSystemPath(tmp)
	config.SetEnableBackgroundMerge(false)

	ctx := context.Background()
	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		err := os.RemoveAll(tmp)
		if err != nil {
			fmt.Printf("remove failed %v", err)
			t.Fail()
		}
	})

	host := test.GetAvailablePort()
	ready := make(chan struct{}, 1)
	go grpc.StartServer(context.Background(), ready, host)

	select {
	case <-ready:
		return ctx, clients.NewGrpcClient(host)
	case <-time.After(5 * time.Second): // Timeout
		panic("Timeout waiting for server readiness")
	}
}

func ClientTest_CreatePartition(t *testing.T) {
	// Arrange
	ctx, client := setupTestServer(t)
	t.Run("should create partition", func(t *testing.T) {

		space := "space_" + uuid.NewString()
		stream := "partition_" + uuid.NewString()

		args := &models.CreatePartitionArgs{Space: space, Partition: stream}

		// Act
		resp, err := client.CreatePartition(ctx, args)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "OK", resp.Message)
	})
}

func TestGetStatus(t *testing.T) {

	ctx, client := setupTestServer(t)

	t.Run("should return err when stream does not exists", func(t *testing.T) {
		// Arrange

		space := "space_" + uuid.NewString()
		stream := "partition_" + uuid.NewString()
		args := &models.GetStatusArgs{Space: space, Partition: stream}

		// Act
		resp, err := client.GetStatus(ctx, args)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("should return ok when stream exists", func(t *testing.T) {
		// Arrange

		space := "space_" + uuid.NewString()
		stream := "partition_" + uuid.NewString()
		_, err := client.CreatePartition(ctx, &models.CreatePartitionArgs{Space: space, Partition: stream})
		require.NoError(t, err)

		args := &models.GetStatusArgs{Space: space, Partition: stream}

		// Act
		resp, err := client.GetStatus(ctx, args)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "OK", resp.Message)
	})
}

func TestProduceIntegration(t *testing.T) {

	// Arrange
	ctx, client := setupTestServer(t)

	count := 275_000

	space := "space_" + uuid.NewString()
	stream := "partition_" + uuid.NewString()
	_, err := client.CreatePartition(ctx, &models.CreatePartitionArgs{Space: space, Partition: stream})
	require.NoError(t, err)

	args := &models.ProduceArgs{
		Space:     space,
		Partition: stream,
	}

	// Act
	enumerator := client.Produce(ctx, args, test.GetSampleEntries(0, count))
	results, err := enumerators.ToSlice(enumerator)

	// Assert
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(results))
	assert.EqualValues(t, 1, results[0].Number)
	assert.EqualValues(t, 2, results[1].Number)
	assert.EqualValues(t, results[0].LastSequence, results[1].FirstSequence-1)
	assert.EqualValues(t, count, results[0].Count+results[1].Count)
}
