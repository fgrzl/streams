package models

// ManifestWrapper is a struct that wraps a stream and an optional concurrency tag.
type ManifestWrapper struct {
	Manifest *Manifest
	Tag      ConcurrencyTag
}
