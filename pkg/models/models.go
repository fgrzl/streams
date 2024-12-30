package models

import "github.com/fgrzl/streams/pkg/enumerators"

// Arguments for GetSpaces operation
type GetSpacesArgs struct {
	Tenant string
}

// Arguments for GetPartitions operation
type GetPartitionsArgs struct {
	Tenant string
	Space  string
}

// Arguments for GetPartition operation
type GetPartitionArgs struct {
	Tenant    string
	Space     string
	Partition string
}

// Arguments for CreatePartition operation
type CreatePartitionArgs struct {
	Tenant    string
	Space     string
	Partition string
}

// Arguments for CreateTier operation
type CreateTierArgs struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
}

// Arguments for Produce operation
type ProduceArgs struct {
	Tenant    string
	Space     string
	Partition string
	Strategy  string
	Entries   enumerators.Enumerator[*Entry]
}

// Arguments for Merge operation
type MergeArgs struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
}

// Arguments for Prune operation
type PruneArgs struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
}

// Arguments for Rebuild operation
type RebuildArgs struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
}

// Arguments for GetStatus operation
type GetStatusArgs struct {
	Tenant    string
	Space     string
	Partition string
}

// Arguments for ConsumeSpace operation
type ConsumeSpaceArgs struct {
	Tenant       string
	Space        string
	Offsets      map[string]*Offset
	MinTimestamp int64
	MaxSequence  uint64
	MaxTimestamp int64
}

// Arguments for ConsumePartition operation
type ConsumePartitionArgs struct {
	Tenant       string
	Space        string
	Partition    string
	MinSequence  uint64
	MinTimestamp int64
	MaxSequence  uint64
	MaxTimestamp int64
}

// Arguments for Peek operation
type PeekArgs struct {
	Tenant    string
	Space     string
	Partition string
}

// Arguments for ReadPage operation
type ReadPageArgs struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
	Number    int32
	Position  int64 // the position to seek to
}

// Arguments for DeletePage operation
type DeletePageArgs struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
	Number    int32
}

// Arguments for GetPages operation
type GetPagesArgs struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
}

// Arguments for WritePage operation
type WritePageArgs struct {
	Tenant      string
	Space       string
	Partition   string
	Tier        int32
	Number      int32
	MinPageSize int64
	MaxPageSize int64
	Entries     enumerators.Enumerator[*Entry]
}

// Arguments for ReadManifest operation
type ReadManifestArgs struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
}

// Arguments for WriteManifest operation
type WriteManifestArgs struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
	Tag       ConcurrencyTag
	Manifest  *Manifest
}

// Arguments for DeleteSpace operation
type DeleteSpaceArgs struct {
	Tenant    string
	Space     string
	Partition string
}

// Arguments for DeletePartition operation
type DeletePartitionArgs struct {
	Tenant    string
	Space     string
	Partition string
}

// Constant values for operation strategies
const (
	DEFAULT            = "DEFAULT"
	SKIP_ON_DUPLICATE  = "SKIP_ON_DUPLICATE"
	ERROR_ON_DUPLICATE = "ERROR_ON_DUPLICATE"
	ALL_OR_NONE        = "ALL_OR_NONE"
)
