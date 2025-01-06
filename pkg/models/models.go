package models

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

// Constant values for operation strategies
const (
	DEFAULT            = "DEFAULT"
	SKIP_ON_DUPLICATE  = "SKIP_ON_DUPLICATE"
	ERROR_ON_DUPLICATE = "ERROR_ON_DUPLICATE"
	ALL_OR_NONE        = "ALL_OR_NONE"
)
