package stores

import (
	"context"

	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/models"
)

var stores = make(map[string]func() StreamStore)

func RegisterStore(name string, factory func() StreamStore) {
	stores[name] = factory
}

func NewStore(name string) StreamStore {
	factory := stores[name]
	return factory()
}

// StreamStore defines the interface for operations on stream data, including stream creation,
// reading and writing manifests, managing pages, and handling spaces and streams.
type StreamStore interface {

	// CreatePartition creates a new stream based on the provided arguments.
	// Arguments: CreatePartitionArgs - the parameters for creating the stream.
	// Returns: error - an error if stream creation fails, nil if successful.
	CreateTier(ctx context.Context, args models.CreateTierArgs) error

	// ReadManifest reads the manifest associated with a specific stream or space.
	// Arguments: ReadManifestArgs - the parameters to locate the manifest.
	// Returns: ManifestWrapper - the wrapped manifest data, or an error if reading fails.
	ReadManifest(ctx context.Context, args models.ReadManifestArgs) (*models.ManifestWrapper, error)

	// WriteManifest writes a manifest to storage.
	// Arguments: WriteManifestArgs - contains details for writing the manifest.
	// Returns: ConcurrencyTag - the concurrency tag representing the write operation's version.
	//          error - any error encountered during the write process.
	WriteManifest(ctx context.Context, args models.WriteManifestArgs) (models.ConcurrencyTag, error)

	// ReadPage retrieves entries from a specific page.
	// Arguments: ReadPageArgs - contains the parameters needed to locate the page.
	// Returns: Enumerator[*models.Entry] - a lazy enumerator for the entries within the page.
	//          The entries are retrieved on demand as the enumerator is iterated over.
	ReadPage(ctx context.Context, args models.ReadPageArgs) enumerators.Enumerator[*models.Entry]

	// WritePage writes a page containing entries to storage.
	// Arguments: WritePageArgs - parameters for creating the page and its entries.
	// Returns: Page - the page that was written, Entry - an entry in the page,
	//          and an error if there was a failure in writing.
	WritePage(ctx context.Context, args models.WritePageArgs) (*models.Page, error)

	// DeletePage deletes a specific page from storage.
	// Arguments: DeletePageArgs - parameters identifying the page to be deleted.
	// Returns: error - any error encountered during the deletion of the page.
	DeletePage(ctx context.Context, args models.DeletePageArgs) error

	// GetPages retrieves a list of page numbers.
	// Arguments: GetPagesArgs - parameters for retrieving the page information.
	// Returns: Enumerator[int32] - a lazy enumerator of page numbers.
	//          The page numbers are returned one by one as the enumerator is iterated over.
	GetPages(ctx context.Context, args models.GetPagesArgs) enumerators.Enumerator[int32]

	// GetSpaces retrieves a list of space keys.
	// Arguments: GetSpacesArgs - parameters for retrieving space information.
	// Returns: Enumerator[string] - a lazy enumerator of space keys.
	//          The space keys are returned one by one as the enumerator is iterated over.
	GetSpaces(ctx context.Context, args models.GetSpacesArgs) enumerators.Enumerator[string]

	// GetPartitions retrieves a list of stream keys.
	// Arguments: GetPartitionsArgs - parameters for retrieving stream information.
	// Returns: Enumerator[string] - a lazy enumerator of stream keys.
	//          The stream keys are returned one by one as the enumerator is iterated over.
	GetPartitions(ctx context.Context, args models.GetPartitionsArgs) enumerators.Enumerator[string]

	// DeleteSpace deletes a space from storage.
	// Arguments: DeleteSpaceArgs - parameters identifying the space to be deleted.
	// Returns: error - any error encountered during the deletion of the space.
	DeleteSpace(ctx context.Context, args models.DeleteSpaceArgs) error

	// DeletePartition deletes a stream from storage.
	// Arguments: DeletePartitionArgs - parameters identifying the stream to be deleted.
	// Returns: error - any error encountered during the deletion of the stream.
	DeletePartition(ctx context.Context, args models.DeletePartitionArgs) error
}
