package stores

import (
	"context"

	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/models"
)

const AWS = "S3"

func init() {
	RegisterStore(GOOGLE, NewS3Store)
}

// FileSystemStreamStore represents a store for file-based streams
type S3StreamStore struct {
}

// CreateTier implements StreamStore.
func (s *S3StreamStore) CreateTier(ctx context.Context, args models.CreateTierArgs) error {
	panic("unimplemented")
}

// DeletePage implements StreamStore.
func (s *S3StreamStore) DeletePage(ctx context.Context, args models.DeletePageArgs) error {
	panic("unimplemented")
}

// DeletePartition implements StreamStore.
func (s *S3StreamStore) DeletePartition(ctx context.Context, args models.DeletePartitionArgs) error {
	panic("unimplemented")
}

// DeleteSpace implements StreamStore.
func (s *S3StreamStore) DeleteSpace(ctx context.Context, args models.DeleteSpaceArgs) error {
	panic("unimplemented")
}

// GetPages implements StreamStore.
func (s *S3StreamStore) GetPages(ctx context.Context, args models.GetPagesArgs) enumerators.Enumerator[int32] {
	panic("unimplemented")
}

// GetPartitions implements StreamStore.
func (s *S3StreamStore) GetPartitions(ctx context.Context, args models.GetPartitionsArgs) enumerators.Enumerator[string] {
	panic("unimplemented")
}

// GetSpaces implements StreamStore.
func (s *S3StreamStore) GetSpaces(ctx context.Context, args models.GetSpacesArgs) enumerators.Enumerator[string] {
	panic("unimplemented")
}

// ReadManifest implements StreamStore.
func (s *S3StreamStore) ReadManifest(ctx context.Context, args models.ReadManifestArgs) (*models.ManifestWrapper, error) {
	panic("unimplemented")
}

// ReadPage implements StreamStore.
func (s *S3StreamStore) ReadPage(ctx context.Context, args models.ReadPageArgs) enumerators.Enumerator[*models.Entry] {
	panic("unimplemented")
}

// WriteManifest implements StreamStore.
func (s *S3StreamStore) WriteManifest(ctx context.Context, args models.WriteManifestArgs) (models.ConcurrencyTag, error) {
	panic("unimplemented")
}

// WritePage implements StreamStore.
func (s *S3StreamStore) WritePage(ctx context.Context, args models.WritePageArgs) (*models.Page, error) {
	panic("unimplemented")
}

// NewFileSystemStore creates a new instance of FileSystemPartitionStore
func NewS3Store() StreamStore {
	return &S3StreamStore{}
}
