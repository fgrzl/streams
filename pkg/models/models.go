package models

import "github.com/fgrzl/streams/pkg/enumerators"

// Arguments for GetSpaces operation
type GetSpacesArgs struct {
	Tenant string `json:"tenant"`
}

// Arguments for GetPartitions operation
type GetPartitionsArgs struct {
	Tenant string `json:"tenant"`
	Space  string `json:"space"`
}

// Arguments for GetPartition operation
type GetPartitionArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
}

// Arguments for CreatePartition operation
type CreatePartitionArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
}

// Arguments for CreateTier operation
type CreateTierArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
	Tier      int32  `json:"tier"`
}

// Arguments for Produce operation
type ProduceArgs struct {
	Tenant    string                         `json:"tenant"`
	Space     string                         `json:"space"`
	Partition string                         `json:"partition"`
	Strategy  string                         `json:"strategy"`
	Entries   enumerators.Enumerator[*Entry] `json:"entries"`
}

// Arguments for Merge operation
type MergeArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
	Tier      int32  `json:"tier"`
}

// Arguments for Prune operation
type PruneArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
	Tier      int32  `json:"tier"`
}

// Arguments for Rebuild operation
type RebuildArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
	Tier      int32  `json:"tier"`
}

// Arguments for GetStatus operation
type GetStatusArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
}

// Arguments for ConsumeSpace operation
type ConsumeSpaceArgs struct {
	Tenant       string             `json:"tenant"`
	Space        string             `json:"space"`
	Offsets      map[string]*Offset `json:"offsets"`
	MinTimestamp int64              `json:"min_timestamp"`
	MaxSequence  uint64             `json:"max_sequence"`
	MaxTimestamp int64              `json:"max_timestamp"`
}

// Arguments for ConsumePartition operation
type ConsumePartitionArgs struct {
	Tenant       string `json:"tenant"`
	Space        string `json:"space"`
	Partition    string `json:"partition"`
	MinSequence  uint64 `json:"min_sequence"`
	MinTimestamp int64  `json:"min_timestamp"`
	MaxSequence  uint64 `json:"max_sequence"`
	MaxTimestamp int64  `json:"max_timestamp"`
}

// Arguments for Peek operation
type PeekArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
}

// Arguments for ReadPage operation
type ReadPageArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
	Tier      int32  `json:"tier"`
	Number    int32  `json:"number"`
	Position  int64  `json:"position"` // the position to seek to
}

// Arguments for DeletePage operation
type DeletePageArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
	Tier      int32  `json:"tier"`
	Number    int32  `json:"number"`
}

// Arguments for GetPages operation
type GetPagesArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
	Tier      int32  `json:"tier"`
}

// Arguments for WritePage operation
type WritePageArgs struct {
	Tenant      string                         `json:"tenant"`
	Space       string                         `json:"space"`
	Partition   string                         `json:"partition"`
	Tier        int32                          `json:"tier"`
	Number      int32                          `json:"number"`
	MinPageSize int64                          `json:"min_page_size"`
	MaxPageSize int64                          `json:"max_page_size"`
	Entries     enumerators.Enumerator[*Entry] `json:"entries"`
}

// Arguments for ReadManifest operation
type ReadManifestArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
	Tier      int32  `json:"tier"`
}

// Arguments for WriteManifest operation
type WriteManifestArgs struct {
	Tenant    string         `json:"tenant"`
	Space     string         `json:"space"`
	Partition string         `json:"partition"`
	Tier      int32          `json:"tier"`
	Tag       ConcurrencyTag `json:"tag"`
	Manifest  *Manifest      `json:"manifest"`
}

// Arguments for DeleteSpace operation
type DeleteSpaceArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
}

// Arguments for DeletePartition operation
type DeletePartitionArgs struct {
	Tenant    string `json:"tenant"`
	Space     string `json:"space"`
	Partition string `json:"partition"`
}

// Constant values for operation strategies
const (
	DEFAULT            = "DEFAULT"
	SKIP_ON_DUPLICATE  = "SKIP_ON_DUPLICATE"
	ERROR_ON_DUPLICATE = "ERROR_ON_DUPLICATE"
	ALL_OR_NONE        = "ALL_OR_NONE"
)
