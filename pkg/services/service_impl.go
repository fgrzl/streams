package services

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/fgrzl/streams/pkg/config"
	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/managers"
	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/pkg/stores"
	"github.com/fgrzl/streams/pkg/util"
	"github.com/google/uuid"
)

//// EVENTS

type PageWritten struct {
	Tenant    string
	Space     string
	Partition string
	Tier      int32
	Number    int32
}

// // IMPL
const tier0 = int32(0)

type serviceImpl struct {
	manager         managers.Manager
	processingLocks sync.Map
	cancel          context.CancelFunc
	backgroundCh    chan any
}

type ServiceOptions struct {
	EnableBackgroundMerge   bool
	EnableBackgroundPrune   bool
	EnableBackgroundRebuild bool
}

// The default services options load the values from the environment variables
var DefaultServiceOptions = &ServiceOptions{
	EnableBackgroundMerge:   config.GetEnableBackgroundMerge(),
	EnableBackgroundPrune:   config.GetEnableBackgroundPrune(),
	EnableBackgroundRebuild: config.GetEnableBackgroundRebuild(),
}

// NewService creates a new instance of the Service interface.
func NewService(options *ServiceOptions) Service {

	if options == nil {
		options = DefaultServiceOptions
	}

	ctx, cancel := context.WithCancel(context.Background())

	// todo: based on how the tiers are configured create a single instance of each type of store
	// todo: assign the correct instance to each tier

	storeMap := make(map[int32]stores.StreamStore, 3)
	store := stores.NewFileSystemStore()
	storeMap[0] = store
	storeMap[1] = store
	storeMap[2] = store

	s := &serviceImpl{
		manager:      managers.NewManager(storeMap),
		cancel:       cancel,
		backgroundCh: make(chan any, 100),
	}

	ready := make(chan any)
	go s.backgroundConsumer(ctx, options, ready)
	<-ready

	return s

}

func (s *serviceImpl) Dispose() {
	defer close(s.backgroundCh)
	s.cancel()
}

func (s *serviceImpl) CreatePartition(ctx context.Context, args *models.CreatePartitionArgs) (*models.StatusResponse, error) {

	if args.Tenant == "" {
		return nil, errors.New("invalid tenant")
	}

	if args.Space == "" {
		return nil, errors.New("invalid space")
	}

	if args.Partition == "" {
		return nil, errors.New("invalid partition")
	}

	for _, t := range s.manager.GetTiers() {
		store := s.manager.GetStore(t)
		err := store.CreateTier(ctx, &models.CreateTierArgs{Tenant: args.Tenant, Space: args.Space, Partition: args.Partition, Tier: t})
		if err != nil {
			return nil, err
		}
		repo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, t)
		err = repo.Create()
		if err != nil {
			return nil, err
		}
	}

	return &models.StatusResponse{Message: "OK"}, nil
}

func (s *serviceImpl) GetStatus(ctx context.Context, args *models.GetStatusArgs) (*models.StatusResponse, error) {

	for _, t := range s.manager.GetTiers() {
		repo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, t)
		manifest, err := repo.GetManifest()
		if err != nil {
			return nil, err
		}
		if manifest == nil {
			return nil, fmt.Errorf("manifest does not exist")
		}
	}

	return &models.StatusResponse{Message: "OK"}, nil
}

func (s *serviceImpl) GetSpaces(ctx context.Context, args *models.GetSpacesArgs) enumerators.Enumerator[*models.SpaceDescriptor] {
	return enumerators.Map(
		s.manager.GetStore(int32(0)).GetSpaces(ctx, args),
		func(space string) (*models.SpaceDescriptor, error) {
			return &models.SpaceDescriptor{
				Tenant: args.Tenant,
				Space:  space,
			}, nil
		})
}

func (s *serviceImpl) GetPartitions(ctx context.Context, args *models.GetPartitionsArgs) enumerators.Enumerator[*models.PartitionDescriptor] {
	return enumerators.Map(
		s.manager.GetStore(int32(0)).GetPartitions(ctx, args),
		func(partition string) (*models.PartitionDescriptor, error) {
			return &models.PartitionDescriptor{
				Tenant:    args.Tenant,
				Space:     args.Space,
				Partition: partition,
			}, nil
		})
}

func (s *serviceImpl) Peek(ctx context.Context, args *models.PeekArgs) (*models.EntryEnvelope, error) {
	const tier = int32(0)

	repo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, tier)

	// get the manifest from tier 0
	m, err := repo.GetManifest()
	if err != nil {
		return nil, err
	}

	if m == nil {
		return nil, errors.New("manifest does not exist")
	}

	if m.LastPage == nil {
		m.LastPage = &models.Page{}
	}

	lastSequence := m.LastPage.LastSequence - 1

	enumerator := s.ConsumePartition(ctx, &models.ConsumePartitionArgs{Tenant: args.Tenant, Space: args.Space, Partition: args.Partition, MinSequence: lastSequence})
	defer enumerator.Dispose()

	var lastEnvelope *models.EntryEnvelope

	for enumerator.MoveNext() {
		lastEnvelope, err = enumerator.Current()
		if err != nil {
			return nil, err
		}
	}
	return lastEnvelope, nil
}

func (s *serviceImpl) Produce(ctx context.Context, args *models.ProduceArgs, entries enumerators.Enumerator[*models.Entry]) enumerators.Enumerator[*models.PageDescriptor] {
	// Initialize repository and store
	repo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, tier0)
	store := s.manager.GetStore(tier0)

	// Fetch the manifest
	manifest, err := repo.GetManifest()
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}

	if manifest.LastPage == nil {
		manifest.LastPage = &models.Page{}
	}

	// Initialize state variables
	tier := int32(0)
	lastSequence := manifest.LastPage.LastSequence
	lastTimestamp := manifest.LastPage.LastTimestamp
	var maxPageSize int64

	if args.Strategy == models.ALL_OR_NONE {
		maxPageSize = math.MaxInt64
	} else {
		maxPageSize = models.GetMaxPageSize(tier)
	}
	pageNumber := manifest.LastPage.Number + 1

	// Process entries and produce responses
	return enumerators.Map(
		enumerators.Chunk(
			enumerators.FilterMap(entries, func(entry *models.Entry) (*models.Entry, error, bool) {
				// Context cancellation check
				if ctx.Err() != nil {
					return entry, ctx.Err(), false
				}

				// Validate entry id
				if entry.EntryId == nil || len(entry.EntryId) != 16 {
					return entry, errors.New("the entry id is required and must be 16 bytes"), false
				}

				// validate correlation id
				if entry.CorrelationId == nil || len(entry.CorrelationId) == 0 {
					correlationId := uuid.New()
					entry.CorrelationId = correlationId[:]
				} else if len(entry.CorrelationId) != 16 {
					return entry, errors.New("the correlation id must be 16 bytes"), false
				}

				// validate causation id
				if entry.CausationId == nil || len(entry.CausationId) == 0 {
					causationId := uuid.New()
					entry.CausationId = causationId[:]
				} else if len(entry.CausationId) != 16 {
					return entry, errors.New("the causation id must be 16 bytes"), false
				}

				// Validate and update entry sequence
				if entry.Sequence == 0 {
					return entry, errors.New("invalid sequence"), false
				}
				if entry.Sequence <= lastSequence {
					switch args.Strategy {
					case models.DEFAULT, models.SKIP_ON_DUPLICATE:
						return nil, nil, false
					case models.ERROR_ON_DUPLICATE, models.ALL_OR_NONE:
						return nil, errors.New("duplicate sequence"), false
					default:
						return nil, nil, false
					}
				} else if entry.Sequence > lastSequence+1 {
					return entry, fmt.Errorf("sequence mismatch: expected %d, got %d", lastSequence+1, entry.Sequence), false
				}

				// Validate and update timestamp
				timestamp := util.GetTimestamp()
				if timestamp < lastTimestamp {
					return entry, fmt.Errorf("timestamp error: expected >= %d, got %d", lastTimestamp, timestamp), false
				}

				entry.Timestamp = timestamp
				lastSequence = entry.Sequence
				lastTimestamp = timestamp
				return entry, nil, true
			}),
			maxPageSize,
			func(entry *models.Entry) (int64, error) {
				return int64(len(entry.Payload)), nil
			},
		),
		func(chunk enumerators.Enumerator[*models.Entry]) (*models.PageDescriptor, error) {
			// Context cancellation check
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			// Prepare arguments for writing a page
			writePageArgs := &models.WritePageArgs{
				Tenant:    args.Tenant,
				Space:     args.Space,
				Partition: args.Partition,
				Tier:      tier,
				Number:    pageNumber,
			}

			// Write the page and update state
			page, err := store.WritePage(ctx, writePageArgs, chunk)
			if err != nil {
				return nil, err
			}

			repo.AddPage(page)

			pageNumber++

			// Send the event to the background processor
			s.backgroundCh <- PageWritten{
				Tenant:    writePageArgs.Tenant,
				Space:     writePageArgs.Space,
				Partition: writePageArgs.Partition,
				Tier:      writePageArgs.Tier,
				Number:    page.Number,
			}

			// Return producer response
			return &models.PageDescriptor{
				Tenant:         writePageArgs.Tenant,
				Space:          writePageArgs.Space,
				Partition:      writePageArgs.Partition,
				Tier:           writePageArgs.Tier,
				Number:         page.Number,
				FirstSequence:  page.FirstSequence,
				FirstTimestamp: page.FirstTimestamp,
				LastSequence:   page.LastSequence,
				LastTimestamp:  page.LastTimestamp,
				Count:          page.Count,
				Size:           page.Size,
			}, nil
		},
	)
}

func (s *serviceImpl) ConsumeSpace(ctx context.Context, args *models.ConsumeSpaceArgs) enumerators.Enumerator[*models.EntryEnvelope] {
	const tier = int32(0)
	store := s.manager.GetStore(tier)
	partitions := store.GetPartitions(ctx, &models.GetPartitionsArgs{Tenant: args.Tenant, Space: args.Space})
	defer partitions.Dispose()

	var consumers []enumerators.Enumerator[*models.EntryEnvelope]

	for partitions.MoveNext() {
		if ctx.Err() != nil {
			return enumerators.Error[*models.EntryEnvelope](ctx.Err())
		}

		partitionKey, err := partitions.Current()
		if err != nil {
			return enumerators.Error[*models.EntryEnvelope](err)
		}

		var minSequence uint64
		var minTimestamp int64

		// Check if offsets are provided for the stream
		if offset, ok := args.Offsets[partitionKey]; ok {
			minSequence = offset.Sequence
			minTimestamp = offset.Timestamp
		}

		consumerPartitionArgs := &models.ConsumePartitionArgs{
			Tenant:       args.Tenant,
			Space:        args.Space,
			Partition:    partitionKey,
			MinSequence:  minSequence,
			MinTimestamp: minTimestamp,
			MaxSequence:  args.MaxSequence,
			MaxTimestamp: args.MaxTimestamp,
		}

		enumerator := s.ConsumePartition(ctx, consumerPartitionArgs)

		// Skip adding empty streams
		if enumerator == nil {
			continue
		}

		consumers = append(consumers, enumerator)
	}

	if len(consumers) == 0 {
		return enumerators.Empty[*models.EntryEnvelope]()
	}

	return enumerators.Interleave(consumers, func(envelope *models.EntryEnvelope) int64 {
		return envelope.Entry.Timestamp
	})
}

func (s *serviceImpl) ConsumePartition(ctx context.Context, args *models.ConsumePartitionArgs) enumerators.Enumerator[*models.EntryEnvelope] {
	// Collect tiered pages with an early return on error
	var tieredPages []models.TieredPage
	for _, tier := range s.manager.GetTiers() {
		repo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, tier)
		manifest, err := repo.GetManifest()
		if err != nil {
			return enumerators.Error[*models.EntryEnvelope](err)
		}

		for _, page := range manifest.Pages {
			tieredPages = append(tieredPages, models.TieredPage{Tier: tier, Page: page})
		}
	}

	// Get covering pages based on sequence bounds and early return if no pages
	coveringPages := models.GetCoveringPages(tieredPages, args.MinSequence, args.MaxSequence)
	if len(coveringPages.Pages) == 0 {
		return enumerators.Empty[*models.EntryEnvelope]()
	}

	minSequence := args.MinSequence
	maxTimestamp := args.MaxTimestamp

	if maxTimestamp == int64(0) {
		maxTimestamp = util.GetTimestamp()
	}

	// Use FlatMap to transform pages into entry enumerators
	return enumerators.FlatMap(
		enumerators.Slice(coveringPages.Pages),
		func(tieredPage models.TieredPage) enumerators.Enumerator[*models.EntryEnvelope] {
			// Check for context cancellation
			if ctx.Err() != nil {
				return enumerators.Error[*models.EntryEnvelope](ctx.Err())
			}

			_, pos := tieredPage.Page.FindNearestKey(args.MinSequence)

			// Prepare arguments for reading the page
			readPageArgs := &models.ReadPageArgs{
				Tenant:    args.Tenant,
				Space:     args.Space,
				Partition: args.Partition,
				Tier:      tieredPage.Tier,
				Number:    tieredPage.Page.Number,
				Position:  pos,
			}
			entries := s.manager.GetStore(tieredPage.Tier).ReadPage(ctx, readPageArgs)

			return enumerators.Map(
				enumerators.TakeWhile(
					enumerators.SkipIf(
						entries,
						func(entry *models.Entry) bool {
							return entry.Sequence <= minSequence
						}),
					func(entry *models.Entry) bool {
						t := entry.Timestamp <= maxTimestamp
						return t
					}),
				func(entry *models.Entry) (*models.EntryEnvelope, error) {
					envelope := &models.EntryEnvelope{
						PartitionDescriptor: &models.PartitionDescriptor{
							Tenant:    args.Tenant,
							Space:     args.Space,
							Partition: args.Partition,
						},
						Entry: entry,
					}
					minSequence = entry.Sequence
					return envelope, nil
				})
		})
}

func (s *serviceImpl) Merge(ctx context.Context, args *models.MergeArgs) enumerators.Enumerator[*models.PageDescriptor] {

	// guard against multiple merge operations runing concurrently
	mergeID, loaded := s.processingLocks.LoadOrStore(args, uuid.New())
	if loaded {
		return enumerators.Empty[*models.PageDescriptor]()
	}

	// Validate source tier
	sourceTier := args.Tier
	if sourceTier < 0 {
		return enumerators.Error[*models.PageDescriptor](fmt.Errorf("invalid source tier"))
	}

	// Validate target tier
	tiers := s.manager.GetTiers()
	targetTier := sourceTier + 1
	if targetTier > tiers[len(tiers)-1] {
		return enumerators.Error[*models.PageDescriptor](fmt.Errorf("invalid target tier"))
	}

	// Retrieve target manifest
	targetRepo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, targetTier)
	targetStore := s.manager.GetStore(targetTier)
	targetManifest, err := targetRepo.GetManifest()
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}

	if targetManifest == nil {
		return enumerators.Error[*models.PageDescriptor](errors.New("no such manifest"))
	}
	targetMinPageSize := models.GetMinPageSize(targetTier)
	targetMaxPageSize := models.GetMaxPageSize(targetTier)

	// Retrieve source manifest
	sourceRepo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, args.Tier)
	sourceStore := s.manager.GetStore(tier0)
	sourceManifest, err := sourceRepo.GetManifest()
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}
	if sourceManifest == nil {
		return enumerators.Error[*models.PageDescriptor](errors.New("no such manifest"))
	}

	lastSequence := targetManifest.LastPage.LastSequence
	pageNumber := targetManifest.LastPage.Number + 1

	// Create and transform the enumerator chain
	sourceEnumerator := enumerators.Slice(sourceManifest.Pages)
	filteredSource := enumerators.Filter(sourceEnumerator, func(page *models.Page) bool {
		return page.LastSequence > lastSequence
	})

	flatMappedSource := enumerators.FlatMap(filteredSource, func(page *models.Page) enumerators.Enumerator[*models.Entry] {

		_, pos := page.FindNearestKey(lastSequence)
		readArgs := &models.ReadPageArgs{
			Tenant:    args.Tenant,
			Space:     args.Space,
			Partition: args.Partition,
			Tier:      args.Tier,
			Number:    page.Number,
			Position:  pos,
		}

		// Return entries from the source store
		return sourceStore.ReadPage(ctx, readArgs)
	})

	filteredEntries := enumerators.Filter(flatMappedSource, func(entry *models.Entry) bool {
		return entry.Sequence > lastSequence
	})

	chunkedEntries := enumerators.Chunk(filteredEntries, models.GetMaxPageSize(targetTier), func(entry *models.Entry) (int64, error) {
		return int64(len(entry.Payload)), nil
	})

	// Map chunked entries to merge responses
	return enumerators.Cleanup(
		enumerators.FilterMap(chunkedEntries, func(chunk enumerators.Enumerator[*models.Entry]) (*models.PageDescriptor, error, bool) {
			writePageArgs := &models.WritePageArgs{
				Tenant:      args.Tenant,
				Space:       args.Space,
				Partition:   args.Partition,
				Tier:        targetTier,
				Number:      pageNumber,
				MinPageSize: targetMinPageSize,
				MaxPageSize: targetMaxPageSize,
			}

			// Write page and handle response
			page, err := targetStore.WritePage(ctx, writePageArgs, chunk)
			if err != nil {
				return nil, err, false
			}

			if page == nil {
				return nil, nil, false
			}

			targetRepo.AddPage(page)
			pageNumber++

			// Send the event to the background processor
			s.backgroundCh <- PageWritten{
				Tenant:    writePageArgs.Tenant,
				Space:     writePageArgs.Space,
				Partition: writePageArgs.Partition,
				Tier:      writePageArgs.Tier,
				Number:    page.Number,
			}

			return &models.PageDescriptor{
				Tenant:         writePageArgs.Tenant,
				Space:          writePageArgs.Space,
				Partition:      writePageArgs.Partition,
				Tier:           writePageArgs.Tier,
				Number:         page.Number,
				FirstSequence:  page.FirstSequence,
				FirstTimestamp: page.FirstTimestamp,
				LastSequence:   page.LastSequence,
				LastTimestamp:  page.LastTimestamp,
				Count:          page.Count,
				Size:           page.Size,
			}, nil, true
		}),
		func() {
			s.processingLocks.CompareAndDelete(args, mergeID)
		})
}

func (s *serviceImpl) Prune(ctx context.Context, args *models.PruneArgs) enumerators.Enumerator[*models.PageDescriptor] {

	// guard against multiple prune operations runing concurrently
	pruneID, loaded := s.processingLocks.LoadOrStore(args, uuid.New())
	if loaded {
		return enumerators.Empty[*models.PageDescriptor]()
	}

	sourceTier := args.Tier
	if sourceTier < 0 {
		return enumerators.Error[*models.PageDescriptor](fmt.Errorf("invalid source tier"))
	}

	tiers := s.manager.GetTiers()

	nextTier := sourceTier + 1
	if nextTier > tiers[len(tiers)-1] {
		return enumerators.Error[*models.PageDescriptor](fmt.Errorf("invalid target tier"))
	}

	nextRepo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, nextTier)
	nextManifest, err := nextRepo.GetManifest()
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}

	currentRepo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, args.Tier)
	currentStore := s.manager.GetStore(tier0)
	currentManifiest, err := currentRepo.GetManifest()
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}

	return enumerators.Cleanup(enumerators.FilterMap(
		enumerators.Slice(currentManifiest.Pages),
		func(page *models.Page) (*models.PageDescriptor, error, bool) {

			if page.LastSequence >= nextManifest.LastPage.LastSequence {
				return nil, nil, false
			}

			deletePageArgs := &models.DeletePageArgs{
				Tenant:    args.Tenant,
				Space:     args.Space,
				Partition: args.Partition,
				Tier:      args.Tier,
				Number:    page.Number,
			}
			err := currentStore.DeletePage(ctx, deletePageArgs)
			if err != nil {
				return nil, err, false
			}
			return &models.PageDescriptor{
				Tenant:    deletePageArgs.Tenant,
				Space:     deletePageArgs.Space,
				Partition: deletePageArgs.Partition,
				Tier:      deletePageArgs.Tier,
				Number:    deletePageArgs.Number,
			}, nil, true
		}), func() {
		s.processingLocks.CompareAndDelete(args, pruneID)
	})
}

func (s *serviceImpl) Rebuild(ctx context.Context, args *models.RebuildArgs) enumerators.Enumerator[*models.RebuildResponse] {

	// repo := s.manager.GetManifestRepository(args.Tenant, args.Space, args.Partition, args.Tier)
	// store := s.manager.GetStore(tier0)
	// currentManifiest, err := repo.GetManifest()
	// if err != nil {
	// 	return enumerators.Error[*models.PageDescriptor](err)
	// }

	// return enumerators.Cleanup(

	// 	, func() {
	// 	s.processingLocks.CompareAndDelete(args, rebuildID)
	// })
	return enumerators.Error[*models.RebuildResponse](errors.New("not implemented"))
}

func (s *serviceImpl) backgroundConsumer(ctx context.Context, options *ServiceOptions, ready chan<- any) {
	close(ready)
	for {
		select {
		case <-ctx.Done():
			// Context cancelled; exit gracefully
			return

		case msg, ok := <-s.backgroundCh:
			if !ok {
				// Channel closed; exit gracefully
				return
			}

			// Handle message based on type
			switch event := msg.(type) {
			case PageWritten:

				if options.EnableBackgroundMerge {
					// Merge operation
					merge := s.Merge(ctx, &models.MergeArgs{
						Tenant:    event.Tenant,
						Space:     event.Space,
						Partition: event.Partition,
						Tier:      event.Tier,
					})
					defer merge.Dispose()
					for merge.MoveNext() {
						// handle error
					}
				}

				if options.EnableBackgroundPrune {
					// Prune operation
					prune := s.Prune(ctx, &models.PruneArgs{
						Tenant:    event.Tenant,
						Space:     event.Space,
						Partition: event.Partition,
						Tier:      event.Tier,
					})
					for prune.MoveNext() {
						// handle error
					}
				}
			}
		}
	}
}
