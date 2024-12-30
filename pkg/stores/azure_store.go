package stores

import (
	"context"

	"github.com/fgrzl/woolf/pkg/enumerators"
	"github.com/fgrzl/woolf/pkg/models"
)

const AZURE = "AZURE"

func init() {
	RegisterStore(AZURE, NewAzureStore)
}

// NewFileSystemStore creates a new instance of FileSystemPartitionStore
func NewAzureStore() StreamStore {
	return &AzureStreamStore{}
}

// FileSystemStreamStore represents a store for file-based streams
type AzureStreamStore struct {
}

// CreateTier implements StreamStore.
func (s *AzureStreamStore) CreateTier(ctx context.Context, args models.CreateTierArgs) error {
	panic("unimplemented")
}

// DeletePage implements StreamStore.
func (s *AzureStreamStore) DeletePage(ctx context.Context, args models.DeletePageArgs) error {
	panic("unimplemented")
}

// DeletePartition implements StreamStore.
func (s *AzureStreamStore) DeletePartition(ctx context.Context, args models.DeletePartitionArgs) error {
	panic("unimplemented")
}

// DeleteSpace implements StreamStore.
func (s *AzureStreamStore) DeleteSpace(ctx context.Context, args models.DeleteSpaceArgs) error {
	panic("unimplemented")
}

// GetPages implements StreamStore.
func (s *AzureStreamStore) GetPages(ctx context.Context, args models.GetPagesArgs) enumerators.Enumerator[int32] {
	panic("unimplemented")
}

// GetPartitions implements StreamStore.
func (s *AzureStreamStore) GetPartitions(ctx context.Context, args models.GetPartitionsArgs) enumerators.Enumerator[string] {
	panic("unimplemented")
}

// GetSpaces implements StreamStore.
func (s *AzureStreamStore) GetSpaces(ctx context.Context, args models.GetSpacesArgs) enumerators.Enumerator[string] {
	panic("unimplemented")
}

// ReadManifest implements StreamStore.
func (s *AzureStreamStore) ReadManifest(ctx context.Context, args models.ReadManifestArgs) (*models.ManifestWrapper, error) {
	panic("unimplemented")
}

// ReadPage implements StreamStore.
func (s *AzureStreamStore) ReadPage(ctx context.Context, args models.ReadPageArgs) enumerators.Enumerator[*models.Entry] {
	panic("unimplemented")
}

// WriteManifest implements StreamStore.
func (s *AzureStreamStore) WriteManifest(ctx context.Context, args models.WriteManifestArgs) (models.ConcurrencyTag, error) {
	panic("unimplemented")
}

// WritePage implements StreamStore.
func (s *AzureStreamStore) WritePage(ctx context.Context, args models.WritePageArgs) (*models.Page, error) {
	panic("unimplemented")
}
