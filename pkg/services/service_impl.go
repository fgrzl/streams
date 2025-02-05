package services

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/pkg/config"
	"github.com/fgrzl/streams/pkg/managers"
	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/pkg/stores"
	"github.com/fgrzl/streams/pkg/util"
	"github.com/google/uuid"
)

type PageWritten struct {
	Space     string
	Partition string
	Tier      int32
	Number    int32
}

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
	Stores                  map[int32]stores.StreamStore
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
		options.Stores = make(map[int32]stores.StreamStore, 3)
		store := stores.NewFileSystemStore()
		options.Stores[0] = store
		options.Stores[1] = store
		options.Stores[2] = store
	} else {

		if len(options.Stores) < 2 || len(options.Stores) > 6 {
			log.Fatalln("stores should have at least 2 tiers and no more than 6 tiers")
		}
		for i := 0; i < len(options.Stores); i++ {
			if _, ok := options.Stores[int32(i)]; !ok {
				log.Fatalf("store for tier %d is not configured\n", i)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Track which stores have been scavenged
	scavengedStores := make(map[stores.StreamStore]bool)

	// Call Scavenge for each unique store
	for _, store := range options.Stores {
		if !scavengedStores[store] {
			store.Scavenge(ctx)
			scavengedStores[store] = true
		}
	}

	s := &serviceImpl{
		manager:      managers.NewManager(options.Stores),
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

	if args.Space == "" {
		return nil, errors.New("invalid space")
	}

	if args.Partition == "" {
		return nil, errors.New("invalid partition")
	}

	for _, t := range s.manager.GetTiers() {
		store := s.manager.GetStore(t)
		err := store.CreateTier(ctx, &models.CreateTierArgs{
			Space:     args.Space,
			Partition: args.Partition,
			Tier:      t,
		})
		if err != nil {
			return nil, err
		}
		repo := s.manager.GetManifestRepository(args.Space, args.Partition, t)
		err = repo.Create()
		if err != nil {
			return nil, err
		}
	}

	return &models.StatusResponse{Message: "OK"}, nil
}

func (s *serviceImpl) GetStatus(ctx context.Context, args *models.GetStatusArgs) (*models.StatusResponse, error) {

	for _, t := range s.manager.GetTiers() {
		repo := s.manager.GetManifestRepository(args.Space, args.Partition, t)
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
				Space: space,
			}, nil
		})
}

func (s *serviceImpl) GetPartitions(ctx context.Context, args *models.GetPartitionsArgs) enumerators.Enumerator[*models.PartitionDescriptor] {
	return enumerators.Map(
		s.manager.GetStore(int32(0)).GetPartitions(ctx, args),
		func(partition string) (*models.PartitionDescriptor, error) {
			return &models.PartitionDescriptor{
				Space:     args.Space,
				Partition: partition,
			}, nil
		})
}

func (s *serviceImpl) Peek(ctx context.Context, args *models.PeekArgs) (*models.EntryEnvelope, error) {
	const tier = int32(0)

	repo := s.manager.GetManifestRepository(args.Space, args.Partition, tier)

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

	enumerator := s.ConsumePartition(ctx, &models.ConsumePartitionArgs{Space: args.Space, Partition: args.Partition, MinSequence: lastSequence})
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
	repo := s.manager.GetManifestRepository(args.Space, args.Partition, tier0)
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
			enumerators.FilterMap(entries, func(entry *models.Entry) (*models.Entry, bool, error) {
				// Context cancellation check
				if ctx.Err() != nil {
					return entry, false, ctx.Err()
				}

				// Validate entry id
				if entry.EntryId == nil || len(entry.EntryId) != 16 {
					return entry, false, errors.New("the entry id is required and must be 16 bytes")
				}

				// validate correlation id
				if entry.CorrelationId == nil || len(entry.CorrelationId) == 0 {
					correlationId := uuid.New()
					entry.CorrelationId = correlationId[:]
				} else if len(entry.CorrelationId) != 16 {
					return entry, false, errors.New("the correlation id must be 16 bytes")
				}

				// validate causation id
				if entry.CausationId == nil || len(entry.CausationId) == 0 {
					causationId := uuid.New()
					entry.CausationId = causationId[:]
				} else if len(entry.CausationId) != 16 {
					return entry, false, errors.New("the causation id must be 16 bytes")
				}

				// Validate and update entry sequence
				if entry.Sequence == 0 {
					return entry, false, errors.New("invalid sequence")
				}
				if entry.Sequence <= lastSequence {
					switch args.Strategy {
					case models.DEFAULT, models.SKIP_ON_DUPLICATE:
						return nil, false, nil
					case models.ERROR_ON_DUPLICATE, models.ALL_OR_NONE:
						return nil, false, errors.New("duplicate sequence")
					default:
						return nil, false, nil
					}
				} else if entry.Sequence > lastSequence+1 {
					return entry, false, fmt.Errorf("sequence mismatch: expected %d, got %d", lastSequence+1, entry.Sequence)
				}

				// Validate and update timestamp
				timestamp := util.GetTimestamp()
				if timestamp < lastTimestamp {
					return entry, false, fmt.Errorf("timestamp error: expected >= %d, got %d", lastTimestamp, timestamp)
				}

				entry.Timestamp = timestamp
				lastSequence = entry.Sequence
				lastTimestamp = timestamp
				return entry, true, nil
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
				Space:     writePageArgs.Space,
				Partition: writePageArgs.Partition,
				Tier:      writePageArgs.Tier,
				Number:    page.Number,
			}

			// Return producer response
			return &models.PageDescriptor{
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
	partitions := store.GetPartitions(ctx, &models.GetPartitionsArgs{Space: args.Space})
	defer partitions.Dispose()

	var consumers []enumerators.Enumerator[*models.EntryEnvelope]

	if args.MaxTimestamp == 0 {
		args.MaxTimestamp = util.GetTimestamp()
	}

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
			Space:        args.Space,
			Partition:    partitionKey,
			MinSequence:  minSequence,
			MinTimestamp: minTimestamp,
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
		repo := s.manager.GetManifestRepository(args.Space, args.Partition, tier)
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
	space := args.Space
	partition := args.Partition

	// Validate source tier
	sourceTier := args.Tier
	if sourceTier < 0 {
		return enumerators.Error[*models.PageDescriptor](fmt.Errorf("invalid source tier"))
	}

	// Retrieve source manifest
	sourceRepo := s.manager.GetManifestRepository(space, partition, sourceTier)
	sourceStore := s.manager.GetStore(tier0)
	sourceManifest, err := sourceRepo.GetManifest()
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}
	if sourceManifest == nil {
		return enumerators.Error[*models.PageDescriptor](errors.New("no such manifest"))
	}

	if len(sourceManifest.Pages) < 3 {
		return enumerators.Empty[*models.PageDescriptor]()
	}

	// Validate target tier
	tiers := s.manager.GetTiers()
	targetTier := sourceTier + 1
	if targetTier > tiers[len(tiers)-1] {
		return enumerators.Error[*models.PageDescriptor](fmt.Errorf("invalid target tier"))
	}

	// Retrieve target manifest
	targetRepo := s.manager.GetManifestRepository(space, partition, targetTier)
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
		enumerators.FilterMap(chunkedEntries, func(chunk enumerators.Enumerator[*models.Entry]) (*models.PageDescriptor, bool, error) {
			writePageArgs := &models.WritePageArgs{
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
				return nil, false, err
			}

			if page == nil {
				return nil, false, nil
			}

			targetRepo.AddPage(page)
			pageNumber++

			// Send the event to the background processor
			s.backgroundCh <- PageWritten{
				Space:     writePageArgs.Space,
				Partition: writePageArgs.Partition,
				Tier:      writePageArgs.Tier,
				Number:    page.Number,
			}

			return &models.PageDescriptor{
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
			}, true, nil
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
	space := args.Space
	partition := args.Partition
	currentTier := args.Tier
	if currentTier < 0 {
		return enumerators.Error[*models.PageDescriptor](fmt.Errorf("invalid source tier"))
	}

	tiers := s.manager.GetTiers()

	nextTier := currentTier + 1
	if nextTier > tiers[len(tiers)-1] {
		return enumerators.Error[*models.PageDescriptor](fmt.Errorf("invalid target tier"))
	}

	nextRepo := s.manager.GetManifestRepository(space, partition, nextTier)
	nextManifest, err := nextRepo.GetManifest()
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}

	currentRepo := s.manager.GetManifestRepository(space, partition, currentTier)
	currentStore := s.manager.GetStore(tier0)
	currentManifiest, err := currentRepo.GetManifest()
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}

	return enumerators.Cleanup(enumerators.FilterMap(
		enumerators.Slice(currentManifiest.Pages),
		func(page *models.Page) (*models.PageDescriptor, bool, error) {

			if page.LastSequence >= nextManifest.LastPage.LastSequence {
				return nil, false, nil
			}

			deletePageArgs := &models.DeletePageArgs{
				Space:     args.Space,
				Partition: args.Partition,
				Tier:      args.Tier,
				Number:    page.Number,
			}
			err := currentStore.DeletePage(ctx, deletePageArgs)
			if err != nil {
				return nil, false, err
			}
			return &models.PageDescriptor{
				Space:     deletePageArgs.Space,
				Partition: deletePageArgs.Partition,
				Tier:      deletePageArgs.Tier,
				Number:    deletePageArgs.Number,
			}, true, nil
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
