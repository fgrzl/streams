package stores_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fgrzl/streams/pkg/config"
	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/pkg/stores"
	"github.com/fgrzl/streams/test"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var names = []string{
	stores.FILE_SYSTEM,
}

// var names = []string{
// 	stores.FILE_SYSTEM,
// 	stores.AZURE,
// 	stores.GOOGLE,
// 	stores.AWS,
// }

func setup(t *testing.T, name string) (context.Context, stores.StreamStore) {
	switch name {
	case stores.AZURE:
	case stores.AWS:
	case stores.GOOGLE:
	case stores.FILE_SYSTEM:
		tmp := filepath.Join(os.TempDir(), uuid.NewString())
		os.Setenv(config.WOOLF_FS_PATH, tmp)
		os.MkdirAll(tmp, 0755)
		t.Cleanup(func() {
			err := os.RemoveAll(tmp)
			if err != nil {
				fmt.Printf("remove failed %v", err)
				t.Fail()
			}
		})
	}

	return context.Background(), stores.NewStore(name)
}

func TestStore_Manifest(t *testing.T) {

	getWriteManifestArgs := func() *models.WriteManifestArgs {
		return &models.WriteManifestArgs{
			Tenant:    "t",
			Space:     "s",
			Partition: uuid.New().String(),
			Tier:      int32(0),
			Manifest: &models.Manifest{
				Pages: []*models.Page{
					{Number: 1},
					{Number: 2},
					{Number: 3},
				},
				LastPage: &models.Page{Number: 3},
			},
		}
	}

	getReadManifestArgs := func(partition string) *models.ReadManifestArgs {
		return &models.ReadManifestArgs{
			Tenant:    "t",
			Space:     "s",
			Partition: partition,
			Tier:      int32(0),
		}
	}

	for _, name := range names {

		ctx, store := setup(t, name)

		t.Run(name+" should fail writing manifest to non-existent tier", func(t *testing.T) {
			// Arrange
			args := getWriteManifestArgs()

			// Act
			tag, err := store.WriteManifest(ctx, args)

			// Assert
			assert.NotNil(t, err)
			assert.Nil(t, tag)
		})

		t.Run(name+" should handle reading manifest from non-existent tier", func(t *testing.T) {
			// Arrange
			args := getReadManifestArgs(uuid.NewString())

			// Act
			wrapper, err := store.ReadManifest(ctx, args)

			// Assert
			assert.Nil(t, err)
			assert.NotNil(t, wrapper)
			assert.Nil(t, wrapper.Manifest)
			assert.Nil(t, wrapper.Tag)
		})

		t.Run(name+" should fail writing manifest with invalid arguments", func(t *testing.T) {
			// Arrange
			args := getWriteManifestArgs()
			args.Tenant = "" // Invalid tenant
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: "t", Space: args.Space, Partition: args.Partition, Tier: args.Tier})

			// Act
			tag, err := store.WriteManifest(ctx, args)

			// Assert
			assert.NotNil(t, err)
			assert.Nil(t, tag)
		})

		t.Run(name+" should write a new manifest", func(t *testing.T) {
			// Arrange
			args := getWriteManifestArgs()
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: args.Tenant, Space: args.Space, Partition: args.Partition, Tier: 0})

			// Act
			tag, err := store.WriteManifest(ctx, args)

			// Assert
			assert.Nil(t, err)
			assert.NotNil(t, tag)
		})

		t.Run(name+" should overwrite an existing manifest", func(t *testing.T) {

			// Arrange
			args := getWriteManifestArgs()
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: args.Tenant, Space: args.Space, Partition: args.Partition, Tier: 0})

			tag, err := store.WriteManifest(ctx, args)
			args.Tag = tag

			// Act
			tag, err = store.WriteManifest(ctx, args)

			// Assert
			assert.Nil(t, err)
			assert.NotNil(t, tag)
		})

		t.Run(name+" should require the correct concurrency tag", func(t *testing.T) {

			// Arrange
			args := getWriteManifestArgs()
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: args.Tenant, Space: args.Space, Partition: args.Partition, Tier: 0})
			tag, err := store.WriteManifest(ctx, args)
			args.Tag = time.Now()

			// Act
			tag, err = store.WriteManifest(ctx, args)

			// Assert
			assert.IsType(t, &models.ConcurrencyError{}, err)
			assert.Nil(t, tag)
		})

		t.Run(name+" should get empty manifest", func(t *testing.T) {

			// Arrange
			args := getReadManifestArgs(uuid.New().String())
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: args.Tenant, Space: args.Space, Partition: args.Partition, Tier: 0})

			// Act
			wrapper, err := store.ReadManifest(ctx, args)

			// Assert
			assert.Nil(t, err)
			assert.NotNil(t, wrapper)
			assert.Nil(t, wrapper.Manifest)
			assert.Nil(t, wrapper.Tag)
		})

		t.Run(name+" should get manifest and concurrency tag", func(t *testing.T) {
			// Arrange
			writeArgs := getWriteManifestArgs()
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: writeArgs.Tenant, Space: writeArgs.Space, Partition: writeArgs.Partition, Tier: writeArgs.Tier})
			tag, err := store.WriteManifest(ctx, writeArgs)
			args := getReadManifestArgs(writeArgs.Partition)

			// Act
			wrapper, err := store.ReadManifest(ctx, args)

			// Assert
			assert.Nil(t, err)
			assert.NotNil(t, wrapper.Manifest)
			assert.NotNil(t, wrapper.Manifest)
			assert.Equal(t, tag, wrapper.Tag)
		})
	}
}

func TestStore_Page(t *testing.T) {
	for _, name := range names {
		ctx, store := setup(t, name)

		t.Run(name+" should write a page", func(t *testing.T) {
			// Arrange
			count := 10
			args := &models.WritePageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: uuid.NewString(),
				Tier:      int32(0),
				Number:    1,
			}
			entries := test.GetSampleEntries(0, count)
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: args.Tenant, Space: args.Space, Partition: args.Partition, Tier: args.Tier})

			// Act
			page, err := store.WritePage(ctx, args, entries)

			// Assert
			assert.Nil(t, err)
			assert.NotNil(t, page)
			assert.EqualValues(t, args.Number, page.Number)
			assert.EqualValues(t, count, page.Count)
		})

		t.Run(name+" should not overwrite a page", func(t *testing.T) {
			// Arrange
			count := 10
			args := &models.WritePageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: uuid.NewString(),
				Tier:      int32(0),
				Number:    1,
			}
			entries := test.GetSampleEntries(0, count)
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: args.Tenant, Space: args.Space, Partition: args.Partition, Tier: args.Tier})

			page, err := store.WritePage(ctx, args, entries)
			entries = test.GetSampleEntries(0, count)

			// Act
			page, err = store.WritePage(ctx, args, entries)

			// Assert
			assert.IsType(t, &models.ConcurrencyError{}, err)
			assert.Nil(t, page)
		})

		t.Run(name+" should not write page under min size", func(t *testing.T) {
			// Arrange
			count := 10
			args := &models.WritePageArgs{
				Tenant:      "t",
				Space:       "s",
				Partition:   uuid.NewString(),
				Tier:        int32(0),
				Number:      1,
				MinPageSize: 1000,
			}
			entries := test.GetSampleEntries(0, count)
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: args.Tenant, Space: args.Space, Partition: args.Partition, Tier: args.Tier})

			// Act
			page, err := store.WritePage(ctx, args, entries)

			// Assert
			assert.Nil(t, err)
			assert.Nil(t, page)
		})

		t.Run(name+" should read a page", func(t *testing.T) {
			// Arrange
			count := 10
			writeArgs := &models.WritePageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: uuid.NewString(),
				Tier:      int32(0),
				Number:    1,
			}
			results := test.GetSampleEntries(0, count)
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: writeArgs.Tenant, Space: writeArgs.Space, Partition: writeArgs.Partition, Tier: writeArgs.Tier})
			store.WritePage(ctx, writeArgs, results)

			readArgs := &models.ReadPageArgs{
				Tenant:    writeArgs.Tenant,
				Space:     writeArgs.Space,
				Partition: writeArgs.Partition,
				Tier:      writeArgs.Tier,
				Number:    writeArgs.Number,
			}

			// Act
			enumerator := store.ReadPage(ctx, readArgs)

			// Assert
			entrySlice, err := enumerators.ToSlice(enumerator)
			assert.Nil(t, err)
			assert.Len(t, entrySlice, int(count))
		})

		t.Run(name+" should delete page", func(t *testing.T) {
			// Arrange
			count := 10
			writeArgs := &models.WritePageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: uuid.NewString(),
				Tier:      int32(0),
				Number:    1,
			}
			entries := test.GetSampleEntries(0, count)
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: writeArgs.Tenant, Space: writeArgs.Space, Partition: writeArgs.Partition, Tier: writeArgs.Tier})
			store.WritePage(ctx, writeArgs, entries)

			deleteArgs := &models.DeletePageArgs{
				Tenant:    writeArgs.Tenant,
				Space:     writeArgs.Space,
				Partition: writeArgs.Partition,
				Tier:      writeArgs.Tier,
				Number:    writeArgs.Number,
			}

			// Act
			err := store.DeletePage(ctx, deleteArgs)

			// Assert
			assert.Nil(t, err)

		})

		t.Run(name+" should delete page when does not exist", func(t *testing.T) {
			// Arrange
			args := &models.DeletePageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: uuid.NewString(),
				Tier:      int32(1),
				Number:    1,
			}

			// Act
			err := store.DeletePage(ctx, args)

			// Assert
			assert.Nil(t, err)
		})

		t.Run(name+" should fail writing page to non-existent tier", func(t *testing.T) {
			// Arrange
			args := &models.WritePageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: uuid.NewString(),
				Tier:      int32(0),
				Number:    1,
			}
			entries := test.GetSampleEntries(0, 10)

			// Act
			page, err := store.WritePage(ctx, args, entries)

			// Assert
			assert.NotNil(t, err)
			assert.Nil(t, page)
		})

		t.Run(name+" should handle reading page from non-existent tier", func(t *testing.T) {
			// Arrange
			args := &models.ReadPageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: uuid.NewString(),
				Tier:      int32(0),
				Number:    1,
			}

			// Act
			enumerator := store.ReadPage(ctx, args)
			entries, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.Error(t, err)
			assert.Empty(t, entries)
		})

		t.Run(name+" should fail deleting page from non-existent tier", func(t *testing.T) {
			// Arrange
			args := &models.DeletePageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: uuid.NewString(),
				Tier:      int32(0),
				Number:    1,
			}

			// Act
			err := store.DeletePage(ctx, args)

			// Assert
			assert.Nil(t, err) // Deleting a non-existent page should not throw an error
		})

		t.Run(name+" should list pages in a partition", func(t *testing.T) {
			// Arrange
			partition := uuid.NewString()
			store.CreateTier(ctx, &models.CreateTierArgs{Tenant: "t", Space: "s", Partition: partition, Tier: 0})
			entries := test.GetSampleEntries(0, 5)
			store.WritePage(ctx, &models.WritePageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: partition,
				Tier:      int32(0),
				Number:    1,
			}, entries)

			entries = test.GetSampleEntries(5, 5)
			store.WritePage(ctx, &models.WritePageArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: partition,
				Tier:      int32(0),
				Number:    2,
			}, entries)

			// Act
			enumerator := store.GetPages(ctx, &models.GetPagesArgs{
				Tenant:    "t",
				Space:     "s",
				Partition: partition,
				Tier:      0,
			})
			pages, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.Nil(t, err)
			assert.Len(t, pages, 2)
		})
	}
}
