package services_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fgrzl/streams/pkg/config"
	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/pkg/services"
	"github.com/fgrzl/streams/pkg/util"
	"github.com/fgrzl/streams/test"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupService(t *testing.T) (context.Context, services.Service) {

	ctx := context.Background()
	tmp := filepath.Join(os.TempDir(), "woolf-service-tests", uuid.NewString())
	os.MkdirAll(tmp, 0755)

	config.SetFileSystemPath(tmp)
	service := services.NewService(&services.ServiceOptions{
		EnableBackgroundMerge:   false,
		EnableBackgroundPrune:   false,
		EnableBackgroundRebuild: false,
	})

	t.Cleanup(func() {
		service.Dispose()

		err := os.RemoveAll(tmp)
		if err != nil {
			fmt.Printf("remove failed %v", err)
			t.Fail()
		}
	})
	return ctx, service
}

func setupPartition(t *testing.T, ctx context.Context, service services.Service, tenant string, space string, partition string, count int) {
	createPartitionArgs := &models.CreatePartitionArgs{
		Tenant:    tenant,
		Space:     space,
		Partition: partition,
	}
	response, err := service.CreatePartition(ctx, createPartitionArgs)
	require.NoError(t, err)
	require.NotNil(t, response)

	if count > 0 {
		produce(t, ctx, service, tenant, space, partition, 0, count)
	}
}

func produce(t *testing.T, ctx context.Context, service services.Service, tenant string, space string, partition string, seed int, count int) {
	produceArgs := &models.ProduceArgs{
		Tenant:    tenant,
		Space:     space,
		Partition: partition,
	}
	entries := test.GetSampleEntries(seed, count)
	enumerator := service.Produce(ctx, produceArgs, entries)
	actual, err := enumerators.Sum(enumerator, func(page *models.PageDescriptor) (int, error) {
		return int(page.Count), nil
	})
	require.NoError(t, err)
	require.Equal(t, count, actual)
}

func TestService_CreatePartition(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should create a partition if it does not exist", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()

		args := &models.CreatePartitionArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		response, err := service.CreatePartition(ctx, args)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, response)
	})

	t.Run("should not create a partition with invalid tenant", func(t *testing.T) {
		// Arrange
		tenant := ""
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()

		args := &models.CreatePartitionArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		response, err := service.CreatePartition(ctx, args)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, "invalid tenant", err.Error())
		assert.Nil(t, response)
	})

	t.Run("should not create a partition with invalid space", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := ""
		partition := "p_" + uuid.NewString()

		args := &models.CreatePartitionArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		response, err := service.CreatePartition(ctx, args)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, "invalid space", err.Error())
		assert.Nil(t, response)
	})

	t.Run("should not create a partition with invalid partiton", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := ""

		args := &models.CreatePartitionArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		response, err := service.CreatePartition(ctx, args)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, "invalid partition", err.Error())
		assert.Nil(t, response)
	})
}

func TestService_GetSpaces(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should get all spaces in the tenant", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()

		for s := 0; s < 13; s++ {
			space := fmt.Sprintf("s_%d", s)
			for p := 0; p < 29; p++ {
				partition := "p_" + uuid.NewString()
				setupPartition(t, ctx, service, tenant, space, partition, 0)
			}
		}

		args := &models.GetSpacesArgs{
			Tenant: tenant,
		}

		// Act
		enumerator := service.GetSpaces(ctx, args)
		spaces, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, 13, len(spaces))
	})
}
func TestService_GetPartitions(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should get all partitions in the space", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()

		for p := 0; p < 29; p++ {
			partition := fmt.Sprintf("p_%010d", p)
			setupPartition(t, ctx, service, tenant, space, partition, 0)
		}

		args := &models.GetPartitionsArgs{
			Tenant: tenant,
			Space:  space,
		}

		// Act
		enumerator := service.GetPartitions(ctx, args)
		partitions, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, 29, len(partitions))

		last := partitions[len(partitions)-1]
		assert.Equal(t, tenant, last.Tenant)
		assert.Equal(t, space, last.Space)
		assert.Equal(t, "p_0000000028", last.Partition)

	})
}

func TestService_GetStatus(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should get the status", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()

		setupPartition(t, ctx, service, tenant, space, partition, 10_000)

		args := &models.GetStatusArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		response, err := service.GetStatus(ctx, args)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, "OK", response.Message)
	})

	t.Run("should not get the status when does not exist", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()

		args := &models.GetStatusArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		response, err := service.GetStatus(ctx, args)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, "manifest does not exist", err.Error())
		assert.Nil(t, response)
	})
}

func TestService_Peek(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should not peek when not exists", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()

		args := &models.PeekArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		entry, err := service.Peek(ctx, args)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, "manifest does not exist", err.Error())
		assert.Nil(t, entry)
	})

	t.Run("should peek the last entry", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()
		count := rand.Intn(10_000)

		setupPartition(t, ctx, service, tenant, space, partition, count)

		args := &models.PeekArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		envelope, err := service.Peek(ctx, args)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, envelope)
		assert.EqualValues(t, count, envelope.Entry.Sequence)
	})
}
func TestService_ConsumeSpace(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should consume all entries in a space", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()

		var total int
		for p := 0; p < 29; p++ {
			count := rand.Intn(9871)
			partition := "p_" + uuid.NewString()
			setupPartition(t, ctx, service, tenant, space, partition, count)
			total += count
		}

		args := &models.ConsumeSpaceArgs{
			Tenant: tenant,
			Space:  space,
		}

		// Act
		enumerator := service.ConsumeSpace(ctx, args)
		envelopes, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NotNil(t, enumerator)
		assert.NoError(t, err)
		assert.NotEmpty(t, envelopes)
		assert.Len(t, envelopes, total)
	})
}

func TestService_ConsumePartition(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should consume all entries in a partition", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()
		seed1 := rand.Intn(9871)
		seed2 := rand.Intn(9871)
		total := seed1 + seed2
		setupPartition(t, ctx, service, tenant, space, partition, seed1)
		produce(t, ctx, service, tenant, space, partition, seed1, seed2)

		args := &models.ConsumePartitionArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		enumerator := service.ConsumePartition(ctx, args)
		envelopes, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NotNil(t, enumerator)
		assert.NoError(t, err)
		assert.NotEmpty(t, envelopes)
		assert.Len(t, envelopes, total)
	})

	t.Run("should consume all entries starting with exclusive min sequence", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()
		count := 9871
		min := 5899
		setupPartition(t, ctx, service, tenant, space, partition, count)

		args := &models.ConsumePartitionArgs{
			Tenant:      tenant,
			Space:       space,
			Partition:   partition,
			MinSequence: uint64(min),
		}

		// Act
		enumerator := service.ConsumePartition(ctx, args)
		envelopes, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NoError(t, err)
		assert.NotEmpty(t, envelopes)
		assert.Len(t, envelopes, count-min)
		first := envelopes[0].Entry
		assert.EqualValues(t, uint64(min+1), first.Sequence)
	})

	t.Run("should consume all entries til inclusive max timestamp", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()
		count := rand.Intn(9871)
		setupPartition(t, ctx, service, tenant, space, partition, count)
		ts := util.GetTimestamp()
		time.Sleep(3 * time.Millisecond)
		produce(t, ctx, service, tenant, space, partition, count, 25)

		args := &models.ConsumePartitionArgs{
			Tenant:       tenant,
			Space:        space,
			Partition:    partition,
			MaxTimestamp: ts,
		}

		// Act
		enumerator := service.ConsumePartition(ctx, args)
		envelopes, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NoError(t, err)
		assert.NotEmpty(t, envelopes)
		assert.Len(t, envelopes, count)
	})
}

func TestService_Produce(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should produce a single page", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()
		count := 9873

		setupPartition(t, ctx, service, tenant, space, partition, 0)

		args := &models.ProduceArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}
		entries := test.GetSampleEntries(0, count)
		// Act
		enumerator := service.Produce(ctx, args, entries)
		pages, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NoError(t, err)
		assert.Len(t, pages, 1)
		assert.EqualValues(t, count, pages[len(pages)-1].LastSequence)
	})

	t.Run("should produce multiple pages", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()
		count := 289_997

		setupPartition(t, ctx, service, tenant, space, partition, 0)

		args := &models.ProduceArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}
		entries := test.GetSampleEntries(0, count)

		// Act
		enumerator := service.Produce(ctx, args, entries)
		pages, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NoError(t, err)
		assert.Len(t, pages, 2)
		assert.EqualValues(t, count, pages[len(pages)-1].LastSequence)
	})

	t.Run("should not produce given invalid sequence", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()

		setupPartition(t, ctx, service, tenant, space, partition, 0)

		args := &models.ProduceArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}
		entries := enumerators.Slice(
			[]*models.Entry{
				test.GetSampleEntry(tenant, space, partition, 1),
				test.GetSampleEntry(tenant, space, partition, 2),
				test.GetSampleEntry(tenant, space, partition, 0),
			})

		// Act
		enumerator := service.Produce(ctx, args, entries)
		pages, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, "invalid sequence", err.Error())
		assert.Empty(t, pages)
	})

	t.Run("should not produce given sequence gaps", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()

		setupPartition(t, ctx, service, tenant, space, partition, 0)

		args := &models.ProduceArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		entries := enumerators.Slice(
			[]*models.Entry{
				test.GetSampleEntry(tenant, space, partition, 1),
				test.GetSampleEntry(tenant, space, partition, 2),
				test.GetSampleEntry(tenant, space, partition, 8),
			})

		// Act
		enumerator := service.Produce(ctx, args, entries)
		pages, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.Error(t, err, "invalid sequence")
		assert.Empty(t, pages)
	})

	strategies := []string{models.DEFAULT, models.SKIP_ON_DUPLICATE}
	for _, strategy := range strategies {
		t.Run("should produce but skip duplicate sequence : "+strategy, func(t *testing.T) {
			// Arrange
			tenant := "t_" + uuid.NewString()
			space := "s_" + uuid.NewString()
			partition := "p_" + uuid.NewString()

			setupPartition(t, ctx, service, tenant, space, partition, 0)

			duplicate := test.GetSampleEntry(tenant, space, partition, 3)

			args := &models.ProduceArgs{
				Tenant:    tenant,
				Space:     space,
				Partition: partition,
				Strategy:  strategy,
			}
			entries := enumerators.Slice(
				[]*models.Entry{
					test.GetSampleEntry(tenant, space, partition, 1),
					test.GetSampleEntry(tenant, space, partition, 2),
					duplicate,
					duplicate,
					test.GetSampleEntry(tenant, space, partition, 4),
					test.GetSampleEntry(tenant, space, partition, 5),
				})

			// Act
			enumerator := service.Produce(ctx, args, entries)
			pages, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.NoError(t, err)
			assert.NotEmpty(t, pages)
			assert.EqualValues(t, 5, pages[len(pages)-1].LastSequence)
		})
	}

	strategies = []string{models.ERROR_ON_DUPLICATE, models.ALL_OR_NONE}
	for _, strategy := range strategies {
		t.Run("should not produce given duplicate sequence : "+strategy, func(t *testing.T) {
			// Arrange
			tenant := "t_" + uuid.NewString()
			space := "s_" + uuid.NewString()
			partition := "p_" + uuid.NewString()

			setupPartition(t, ctx, service, tenant, space, partition, 0)

			duplicate := test.GetSampleEntry(tenant, space, partition, 3)

			args := &models.ProduceArgs{
				Tenant:    tenant,
				Space:     space,
				Partition: partition,
				Strategy:  strategy,
			}

			entries := enumerators.Slice(
				[]*models.Entry{
					test.GetSampleEntry(tenant, space, partition, 1),
					test.GetSampleEntry(tenant, space, partition, 2),
					duplicate,
					duplicate,
					test.GetSampleEntry(tenant, space, partition, 4),
					test.GetSampleEntry(tenant, space, partition, 5),
				})

			// Act
			enumerator := service.Produce(ctx, args, entries)
			pages, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.Error(t, err)
			assert.Equal(t, "duplicate sequence", err.Error())
			assert.Empty(t, pages)
		})
	}
}
func TestService_Merge(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should not merge given small volume of data", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()

		count := 10_000
		setupPartition(t, ctx, service, tenant, space, partition, count)
		for i := 1; i < 3; i++ {
			produce(t, ctx, service, tenant, space, partition, count*i, count)
		}

		args := &models.MergeArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		enumerator := service.Merge(ctx, args)
		pages, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NoError(t, err)
		assert.Empty(t, pages)
	})

	t.Run("should merge pages from one tier to the next", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()

		count := 10_000
		setupPartition(t, ctx, service, tenant, space, partition, count)
		for i := 1; i < 47; i++ {
			produce(t, ctx, service, tenant, space, partition, count*i, count)
		}

		args := &models.MergeArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		}

		// Act
		enumerator := service.Merge(ctx, args)
		pages, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NoError(t, err)
		assert.NotEmpty(t, pages)
	})
}

func TestService_Prune(t *testing.T) {
	ctx, service := setupService(t)

	t.Run("should prune obsolete pages in a partition", func(t *testing.T) {
		// Arrange
		tenant := "t_" + uuid.NewString()
		space := "s_" + uuid.NewString()
		partition := "p_" + uuid.NewString()
		tier := int32(0)

		count := 10_000
		setupPartition(t, ctx, service, tenant, space, partition, count)
		for i := 1; i < 47; i++ {
			produce(t, ctx, service, tenant, space, partition, count*i, count)
		}

		merge := service.Merge(ctx, &models.MergeArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
		})
		mergedPages, err := enumerators.ToSlice(merge)
		require.NoError(t, err)
		require.NotEmpty(t, mergedPages)

		args := &models.PruneArgs{
			Tenant:    tenant,
			Space:     space,
			Partition: partition,
			Tier:      tier,
		}

		// Act
		enumerator := service.Prune(ctx, args)
		pages, err := enumerators.ToSlice(enumerator)

		// Assert
		assert.NoError(t, err)
		assert.NotEmpty(t, pages)
		assert.Len(t, pages, 46)
	})
}

// func TestService_Rebuild(t *testing.T) {
// 	ctx, service := setupService(t)

// 	t.Run("should rebuild a stream's manifest, followed by merge and prune operations", func(t *testing.T) {
// 		// Arrange
// 		tenant := "t_" + uuid.NewString()
// 		space := "s_" + uuid.NewString()
// 		partition := "p_" + uuid.NewString()
// 		tier := int32(0)

// 		args := models.RebuildArgs{
// 			Tenant:    tenant,
// 			Space:     space,
// 			Partition: partition,
// 			Tier:      tier,
// 		}

// 		// Act
// 		enumerator := service.Rebuild(ctx, args)

// 		// Assert
// 		assert.NotNil(t, enumerator)

// 		// Use ToSlice to get all items
// 		responses, err := enumerators.ToSlice(enumerator)
// 		assert.NoError(t, err)
// 		assert.NotEmpty(t, responses)
// 	})
// }
