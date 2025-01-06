package stores

import (
	"context"

	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/models"
)

const GOOGLE = "GOOGLE"

func init() {
	RegisterStore(GOOGLE, NewGoogleStore)
}

// NewFileSystemStore creates a new instance of FileSystemPartitionStore
func NewGoogleStore() StreamStore {
	return &GoogleStreamStore{}
}

// FileSystemStreamStore represents a store for file-based streams
type GoogleStreamStore struct {
}

// CreateTier implements StreamStore.
func (s *GoogleStreamStore) CreateTier(ctx context.Context, args *models.CreateTierArgs) error {
	panic("unimplemented")
}

// DeletePage implements StreamStore.
func (s *GoogleStreamStore) DeletePage(ctx context.Context, args *models.DeletePageArgs) error {
	panic("unimplemented")
}

// DeletePartition implements StreamStore.
func (s *GoogleStreamStore) DeletePartition(ctx context.Context, args *models.DeletePartitionArgs) error {
	panic("unimplemented")
}

// DeleteSpace implements StreamStore.
func (s *GoogleStreamStore) DeleteSpace(ctx context.Context, args *models.DeleteSpaceArgs) error {
	panic("unimplemented")
}

// GetPages implements StreamStore.
func (s *GoogleStreamStore) GetPages(ctx context.Context, args *models.GetPagesArgs) enumerators.Enumerator[int32] {
	panic("unimplemented")
}

// GetPartitions implements StreamStore.
func (s *GoogleStreamStore) GetPartitions(ctx context.Context, args *models.GetPartitionsArgs) enumerators.Enumerator[string] {
	panic("unimplemented")
}

// GetSpaces implements StreamStore.
func (s *GoogleStreamStore) GetSpaces(ctx context.Context, args *models.GetSpacesArgs) enumerators.Enumerator[string] {
	panic("unimplemented")
}

// ReadManifest implements StreamStore.
func (s *GoogleStreamStore) ReadManifest(ctx context.Context, args *models.ReadManifestArgs) (*models.ManifestWrapper, error) {
	panic("unimplemented")
}

// ReadPage implements StreamStore.
func (s *GoogleStreamStore) ReadPage(ctx context.Context, args *models.ReadPageArgs) enumerators.Enumerator[*models.Entry] {
	panic("unimplemented")
}

// WriteManifest implements StreamStore.
func (s *GoogleStreamStore) WriteManifest(ctx context.Context, args *models.WriteManifestArgs) (models.ConcurrencyTag, error) {
	panic("unimplemented")
}

// WritePage implements StreamStore.
func (s *GoogleStreamStore) WritePage(ctx context.Context, args *models.WritePageArgs, entries enumerators.Enumerator[*models.Entry]) (*models.Page, error) {
	panic("unimplemented")
}
