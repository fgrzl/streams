package clients

import (
	"context"

	"github.com/fgrzl/woolf/pkg/enumerators"
	"github.com/fgrzl/woolf/pkg/models"
	"github.com/fgrzl/woolf/pkg/util"
)

type WoolfClient interface {
	util.Disposable

	// Create a stream if does not exist
	CreatePartition(ctx context.Context, args models.CreatePartitionArgs) (*models.StatusResponse, error)

	// Get all the spaces
	GetSpaces(ctx context.Context, args models.GetSpacesArgs) enumerators.Enumerator[*models.SpaceDescriptor]

	// Get all streams in a space.
	GetPartitions(ctx context.Context, args models.GetPartitionsArgs) enumerators.Enumerator[*models.PartitionDescriptor]

	// Get the status of a stream.
	GetStatus(ctx context.Context, args models.GetStatusArgs) (*models.StatusResponse, error)

	// Get the last entry in a stream.
	Peek(ctx context.Context, args models.PeekArgs) (*models.EntryEnvelope, error)

	// Consume the space. This will interleave all of the streams in the space.
	ConsumeSpace(ctx context.Context, args models.ConsumeSpaceArgs) enumerators.Enumerator[*models.EntryEnvelope]

	// Consume a stream.
	ConsumePartition(ctx context.Context, args models.ConsumePartitionArgs) enumerators.Enumerator[*models.EntryEnvelope]

	// Produce stream items.
	Produce(ctx context.Context, args models.ProduceArgs) enumerators.Enumerator[*models.PageDescriptor]

	// Merge consolidates the pages of a streams. This is normally done in the background.
	Merge(ctx context.Context, args models.MergeArgs) enumerators.Enumerator[*models.PageDescriptor]

	// Prune will trim obsolete pages. This is normally done in the background.
	Prune(ctx context.Context, args models.PruneArgs) enumerators.Enumerator[*models.PageDescriptor]

	// Rebuild will repair a manifest followed by merge and prune operations. This is normally done in the background.
	Rebuild(ctx context.Context, args models.RebuildArgs) enumerators.Enumerator[*models.RebuildResponse]
}
