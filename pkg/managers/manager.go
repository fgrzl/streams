package managers

import (
	"context"
	"sync"

	"github.com/fgrzl/woolf/pkg/repositories"
	"github.com/fgrzl/woolf/pkg/stores"
	"github.com/fgrzl/woolf/pkg/util"
)

// The manager brokers access to our stores and providers
type Manager interface {
	util.Disposable
	GetTiers() []int32
	GetStore(tier int32) stores.StreamStore
	GetManifestRepository(tenantKey string, spaceKey string, partitionKey string, tier int32) repositories.ManifestRepository
}

func NewManager(stores map[int32]stores.StreamStore) Manager {

	mm := &manager{
		tiers:  util.GetSortedKeys(stores),
		stores: util.GetSortedValues(stores),
	}

	return mm
}

// the internal impl of the manifest manager using a sync.Map
// we want to ttl out inactive repositories
type manager struct {
	util.Disposable
	tiers    []int32
	stores   []stores.StreamStore
	registry sync.Map
	dispose  context.CancelFunc
}

func (mm *manager) Dispose() {
	mm.dispose()
}

func (mm *manager) GetTiers() []int32 {
	return mm.tiers
}

func (mm *manager) GetStore(tier int32) stores.StreamStore {
	return mm.stores[tier]
}

func (mm *manager) GetManifestRepository(tenantKey string, spaceKey string, partitionKey string, tier int32) repositories.ManifestRepository {

	key := repositories.ManifestKey{
		Tenant:    tenantKey,
		Space:     spaceKey,
		Partition: partitionKey,
		Tier:      tier}

	// Try to load the value
	if value, ok := mm.registry.Load(key); ok {
		return value.(repositories.ManifestRepository)
	}

	// Compute the value using the factory
	value := repositories.NewManifestRepository(key, mm.GetStore(tier))

	// Store the value if it doesn't already exist
	actual, loaded := mm.registry.LoadOrStore(key, value)
	if loaded {
		// If another goroutine stored the value, discard our computation
		value.Dispose()
		return actual.(repositories.ManifestRepository)
	}

	return value
}
