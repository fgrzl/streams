package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streams/broker"
	"github.com/fgrzl/streams/server"
	"github.com/fgrzl/timestamp"
	"github.com/google/uuid"
)

// Constants
const (
	BatchSize         int           = 100
	CacheTTL          time.Duration = time.Minute * 97
	CacheCleanup      time.Duration = time.Second * 59
	ShutdownTimeout   time.Duration = time.Second * 59
	InitialRetryDelay time.Duration = time.Millisecond * 100
	MaxRetryAttempts  int           = 3
	LAST_ENTRY        string        = "LAST_ENTRY"
)

// Types
type Entity struct {
	PartitionKey string `json:"PartitionKey"`
	RowKey       string `json:"RowKey"`
	Value        []byte `json:"Value,omitempty"`
}

type TableProviderOptions struct {
	Prefix                    string
	Table                     string
	Endpoint                  string
	UseDefaultAzureCredential bool
	SharedKeyCredential       *aztables.SharedKeyCredential
}

type AzureService struct {
	client    *aztables.Client
	bus       broker.Bus
	cache     *server.ExpiringCache
	wg        sync.WaitGroup
	closeOnce sync.Once
}

type batchEntry struct {
	Entry        *server.Entry
	EncodedValue []byte
}

// Ensure AzureService implements server.Service
var _ server.Service = (*AzureService)(nil)

// NewSharedKeyCredential creates a new shared key credential
func NewSharedKeyCredential(accountName, accountKey string) (*aztables.SharedKeyCredential, error) {
	return aztables.NewSharedKeyCredential(accountName, accountKey)
}

// NewService creates a new AzureService with optimized initialization
func NewService(bus broker.Bus, opts *TableProviderOptions) (server.Service, error) {
	client, err := getClient(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	s := &AzureService{
		client: client,
		bus:    bus,
		cache:  server.NewExpiringCache(CacheTTL, CacheCleanup),
	}

	if err := s.createTableIfNotExists(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize table: %w", err)
	}

	// todo : complete any transactions that may be in flight during startup, if needed

	return s, nil
}

// GetClusterStatus returns cluster status
func (s *AzureService) GetClusterStatus() *server.ClusterStatus {
	return &server.ClusterStatus{NodeCount: 1}
}

// GetSpaces returns space enumerator
func (s *AzureService) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	query := buildQuery(
		lexkey.EncodeFirst(server.INVENTORY, server.SPACES).ToHexString(),
		lexkey.EncodeLast(server.INVENTORY, server.SPACES).ToHexString(),
	)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &query,
		Format: ptr(aztables.MetadataFormatNone),
	}))

	return enumerators.Map(entities, func(e *Entity) (string, error) {
		return e.RowKey, nil
	})
}

// ConsumeSpace consumes entries from a space
func (s *AzureService) ConsumeSpace(ctx context.Context, args *server.ConsumeSpace) enumerators.Enumerator[*server.Entry] {
	ts := timestamp.GetTimestamp()
	bounds := calculateTimeBounds(ts, args.MinTimestamp, args.MaxTimestamp)

	query := buildQuery(
		getSpaceLowerBound(args.Space, bounds.Min, args.Offset).ToHexString(),
		lexkey.EncodeLast(server.DATA, server.SPACES, args.Space).ToHexString(),
	)

	return s.queryEntries(ctx, query, bounds.Min, bounds.Max)
}

// GetSegments returns segment enumerator
func (s *AzureService) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	query := buildQuery(
		lexkey.EncodeFirst(server.INVENTORY, server.SEGMENTS, space).ToHexString(),
		lexkey.EncodeLast(server.INVENTORY, server.SEGMENTS, space).ToHexString(),
	)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &query,
		Format: ptr(aztables.MetadataFormatNone),
	}))

	return enumerators.Map(entities, func(e *Entity) (string, error) {
		return e.RowKey, nil
	})
}

// ConsumeSegment consumes entries from a segment
func (s *AzureService) ConsumeSegment(ctx context.Context, args *server.ConsumeSegment) enumerators.Enumerator[*server.Entry] {
	ts := timestamp.GetTimestamp()
	bounds := calculateSegmentBounds(ts, args)

	pLower := lexkey.EncodeFirst(server.DATA, server.SEGMENTS, args.Space, args.Segment).ToHexString()
	pUpper := lexkey.EncodeLast(server.DATA, server.SEGMENTS, args.Space, args.Segment).ToHexString()
	rLower := lexkey.EncodeFirst(bounds.MinSeq).ToHexString()
	rUpper := lexkey.EncodeLast(bounds.MaxSeq).ToHexString()

	query := fmt.Sprintf("PartitionKey ge '%s' and PartitionKey le '%s' and RowKey ge '%s' and RowKey le '%s'",
		pLower, pUpper, rLower, rUpper)

	entities := NewAzureTableEnumerator(ctx, s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &query,
		Format: ptr(aztables.MetadataFormatNone),
	}))

	entries := enumerators.Map(entities, func(e *Entity) (*server.Entry, error) {
		return decodeEntry(e.Value)
	})

	return enumerators.TakeWhile(entries, func(e *server.Entry) bool {
		return e.Sequence > bounds.MinSeq &&
			e.Sequence <= bounds.MaxSeq &&
			e.Timestamp > bounds.MinTS &&
			e.Timestamp <= bounds.MaxTS
	})
}

// Peek returns the last entry in a segment
func (s *AzureService) Peek(ctx context.Context, space, segment string) (*server.Entry, error) {
	cacheKey := fmt.Sprintf("peek:%s:%s", space, segment)
	if cached, ok := s.cache.Get(cacheKey); ok {
		if entry, ok := cached.(*server.Entry); ok {
			return entry, nil
		}
	}

	pk := lexkey.Encode(LAST_ENTRY, space, segment).ToHexString()
	rk := lexkey.Encode(lexkey.EndMarker).ToHexString()

	resp, err := s.client.GetEntity(ctx, pk, rk, nil)
	if err != nil {
		if isNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to peek: %w", err)
	}

	entry, err := decodeSnappyEntryEntity(resp.Value)
	if err != nil {
		return nil, err
	}
	s.cache.Set(cacheKey, entry)
	return entry, nil
}

// Consume consumes entries across multiple spaces
func (s *AzureService) Consume(ctx context.Context, args *server.Consume) enumerators.Enumerator[*server.Entry] {
	spaces := make([]enumerators.Enumerator[*server.Entry], 0, len(args.Offsets))
	for space, offset := range args.Offsets {
		spaces = append(spaces, s.ConsumeSpace(ctx, &server.ConsumeSpace{
			Space:        space,
			MinTimestamp: args.MinTimestamp,
			MaxTimestamp: args.MaxTimestamp,
			Offset:       offset,
		}))
	}
	return enumerators.Interleave(spaces, func(e *server.Entry) int64 { return e.Timestamp })
}

// Produce produces records to a segment
func (s *AzureService) Produce(ctx context.Context, args *server.Produce, records enumerators.Enumerator[*server.Record]) enumerators.Enumerator[*server.SegmentStatus] {
	if args == nil || args.Space == "" || args.Segment == "" {
		return enumerators.Error[*server.SegmentStatus](errors.New("invalid produce arguments"))
	}

	lastEntry, err := s.Peek(ctx, args.Space, args.Segment)
	if err != nil {
		return enumerators.Error[*server.SegmentStatus](err)
	}
	if lastEntry == nil {
		lastEntry = &server.Entry{Sequence: 0, TRX: server.TRX{Number: 0}}
	}

	chunks := enumerators.ChunkByCount(records, BatchSize)
	var lastSeq, lastTrx = lastEntry.Sequence, lastEntry.TRX.Number

	return enumerators.Map(chunks, func(chunk enumerators.Enumerator[*server.Record]) (*server.SegmentStatus, error) {
		return s.processChunkWithRetry(ctx, args.Space, args.Segment, chunk, &lastSeq, &lastTrx)
	})
}

// processChunkWithRetry handles chunk processing with retries
func (s *AzureService) processChunkWithRetry(ctx context.Context, space, segment string, chunk enumerators.Enumerator[*server.Record], lastSeq, lastTrx *uint64) (*server.SegmentStatus, error) {
	var lastErr error
	for attempt := range MaxRetryAttempts {
		status, err := s.processChunk(ctx, space, segment, chunk, *lastSeq, *lastTrx)
		if err == nil {
			*lastSeq = status.LastSequence
			*lastTrx += 1
			return status, nil
		}
		if !isRetryableError(err) {
			return nil, err
		}
		lastErr = err
		time.Sleep(InitialRetryDelay << attempt)
	}
	return nil, fmt.Errorf("failed after %d attempts: %w", MaxRetryAttempts, lastErr)
}

// processChunk processes a chunk of records
func (s *AzureService) processChunk(ctx context.Context, space, segment string, chunk enumerators.Enumerator[*server.Record], lastSeq, lastTrx uint64) (*server.SegmentStatus, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	trx := server.TRX{ID: uuid.New(), Number: lastTrx + 1}
	entries, err := createEntries(chunk, space, segment, trx, lastSeq)
	if err != nil {
		return nil, err
	}

	batch, err := prepareBatchEntries(entries)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare batch: %w", err)
	}

	if err := s.executeTransaction(ctx, space, segment, trx, batch); err != nil {
		return nil, err
	}

	status := createSegmentStatus(space, segment, entries)
	s.notify(status)
	return status, nil
}

// Close cleans up resources
func (s *AzureService) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if !s.waitForTasks(ShutdownTimeout) {
			err = errors.New("timeout waiting for tasks to complete")
			slog.Warn("timeout waiting for tasks to complete")
		}
		s.cache.Close()
	})
	return err
}

// Helper Functions

func (s *AzureService) executeTransaction(ctx context.Context, space, segment string, trx server.TRX, batch []batchEntry) error {
	entries := make([]*server.Entry, len(batch))
	for i, b := range batch {
		entries[i] = b.Entry
	}

	trxEntity, err := createTransactionEntity(trx, space, segment, entries)
	if err != nil {
		return fmt.Errorf("failed to create transaction entity: %w", err)
	}

	if _, err := s.client.AddEntity(ctx, mustMarshal(trxEntity), nil); err != nil {
		return fmt.Errorf("failed to write transaction: %w", err)
	}

	errChan := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(3)

	go s.writeLastEntry(ctx, batch[len(batch)-1], errChan, &wg)
	go s.writeSegmentBatch(ctx, batch, errChan, &wg)
	go s.writeSpaceBatch(ctx, batch, errChan, &wg)

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	if _, err := s.client.DeleteEntity(ctx, trxEntity.PartitionKey, trxEntity.RowKey, nil); err != nil {
		return fmt.Errorf("failed to delete transaction: %w", err)
	}

	return s.updateInventory(ctx, space, segment)
}

func (s *AzureService) writeLastEntry(ctx context.Context, entry batchEntry, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	entity := Entity{
		PartitionKey: lexkey.Encode(LAST_ENTRY, entry.Entry.Space, entry.Entry.Segment).ToHexString(),
		RowKey:       lexkey.Encode(lexkey.EndMarker).ToHexString(),
		Value:        entry.EncodedValue,
	}

	if _, err := s.client.UpsertEntity(ctx, mustMarshal(entity), &aztables.UpsertEntityOptions{
		UpdateMode: aztables.UpdateModeReplace,
	}); err != nil {
		errChan <- err
	}
}

func (s *AzureService) writeSegmentBatch(ctx context.Context, entries []batchEntry, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	entities := make([]Entity, len(entries))
	for i, entry := range entries {
		entities[i] = Entity{
			PartitionKey: lexkey.Encode(server.DATA, server.SEGMENTS, entry.Entry.Space, entry.Entry.Segment, entry.Entry.TRX.Number).ToHexString(),
			RowKey:       lexkey.Encode(entry.Entry.Sequence).ToHexString(),
			Value:        entry.EncodedValue,
		}
	}

	if err := s.writeBatch(ctx, entities); err != nil {
		errChan <- err
	}
}

func (s *AzureService) writeSpaceBatch(ctx context.Context, entries []batchEntry, errChan chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	entities := make([]Entity, len(entries))
	for i, entry := range entries {
		entities[i] = Entity{
			PartitionKey: lexkey.Encode(server.DATA, server.SPACES, entry.Entry.Space, entry.Entry.Timestamp, entry.Entry.Segment).ToHexString(),
			RowKey:       lexkey.Encode(entry.Entry.Sequence).ToHexString(),
			Value:        entry.EncodedValue,
		}
	}

	if err := s.writeBatch(ctx, entities); err != nil {
		errChan <- err
	}
}

func (s *AzureService) writeBatch(ctx context.Context, entities []Entity) error {
	if len(entities) == 0 {
		return nil
	}

	actions := make([]aztables.TransactionAction, len(entities))
	for i := range entities {
		actions[i] = aztables.TransactionAction{
			ActionType: aztables.TransactionTypeInsertReplace,
			Entity:     mustMarshal(entities[i]),
		}
	}

	_, err := s.client.SubmitTransaction(ctx, actions, nil)
	if err != nil && err.Error() != "unexpected EOF" {
		return fmt.Errorf("batch write failed: %w", err)
	}
	return nil
}

func createTransactionEntity(trx server.TRX, space, segment string, entries []*server.Entry) (*Entity, error) {
	t := &server.Transaction{
		TRX:           trx,
		Space:         space,
		Segment:       segment,
		FirstSequence: entries[0].Sequence,
		LastSequence:  entries[len(entries)-1].Sequence,
		Entries:       entries,
		Timestamp:     timestamp.GetTimestamp(),
	}

	value, err := server.EncodeTransactionSnappy(t)
	if err != nil {
		return nil, err
	}

	return &Entity{
		PartitionKey: lexkey.Encode(server.TRANSACTION, space, segment, trx.Number).ToHexString(),
		RowKey:       lexkey.Encode(lexkey.EndMarker).ToHexString(),
		Value:        value,
	}, nil
}

func createEntries(chunk enumerators.Enumerator[*server.Record], space, segment string, trx server.TRX, lastSeq uint64) ([]*server.Entry, error) {
	ts := timestamp.GetTimestamp()
	enumerator := enumerators.Map(chunk, func(r *server.Record) (*server.Entry, error) {
		lastSeq++
		if r.Sequence != lastSeq {
			return nil, server.ERR_SEQUENCE_MISMATCH
		}
		return &server.Entry{
			TRX:       trx,
			Space:     space,
			Segment:   segment,
			Sequence:  r.Sequence,
			Timestamp: ts,
			Payload:   r.Payload,
			Metadata:  r.Metadata,
		}, nil
	})
	return enumerators.ToSlice(enumerator)
}

func prepareBatchEntries(entries []*server.Entry) ([]batchEntry, error) {
	batch := make([]batchEntry, len(entries))
	for i, e := range entries {
		encoded, err := server.EncodeEntrySnappy(e)
		if err != nil {
			return nil, err
		}
		batch[i] = batchEntry{Entry: e, EncodedValue: encoded}
	}
	return batch, nil
}

func createSegmentStatus(space, segment string, entries []*server.Entry) *server.SegmentStatus {
	return &server.SegmentStatus{
		Space:          space,
		Segment:        segment,
		FirstSequence:  entries[0].Sequence,
		FirstTimestamp: entries[0].Timestamp,
		LastSequence:   entries[len(entries)-1].Sequence,
		LastTimestamp:  entries[len(entries)-1].Timestamp,
	}
}

func (s *AzureService) notify(status *server.SegmentStatus) {
	if err := s.bus.Notify(status); err != nil {
		slog.Error("failed to notify supervisor", "error", err)
	}
}

func (s *AzureService) updateInventory(ctx context.Context, space, segment string) error {

	segmentKey := lexkey.Encode(server.INVENTORY, server.SEGMENTS, space, segment).ToHexString()

	_, ok := s.cache.Get(segmentKey)
	if ok {
		return nil // short-circuit if already in cache, no need to update inventory again
	}

	updateOptions := &aztables.UpsertEntityOptions{UpdateMode: aztables.UpdateModeReplace}

	if _, err := s.client.UpsertEntity(ctx, mustMarshal(Entity{
		PartitionKey: segmentKey,
		RowKey:       segment,
	}), updateOptions); err != nil {
		return fmt.Errorf("failed to update segment inventory: %w", err)
	}

	spaceKey := lexkey.Encode(server.INVENTORY, server.SPACES, space).ToHexString()

	if _, err := s.client.UpsertEntity(ctx, mustMarshal(Entity{
		PartitionKey: spaceKey,
		RowKey:       space,
	}), updateOptions); err != nil {
		return fmt.Errorf("failed to update space inventory: %w", err)
	}

	s.cache.Set(segmentKey, struct{}{}) // cache the segment key to avoid redundant updates

	return nil
}

func (s *AzureService) waitForTasks(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.wg.Wait()
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (s *AzureService) createTableIfNotExists(ctx context.Context) error {
	_, err := s.client.CreateTable(ctx, nil)
	if err == nil {
		return nil // Table created successfully
	}

	var responseErr *azcore.ResponseError
	if errors.As(err, &responseErr) && responseErr.ErrorCode == string(aztables.TableAlreadyExists) {
		return nil // Table already exists, no further action needed
	}

	return fmt.Errorf("failed to create table: %w", err)
}

func getClient(opts *TableProviderOptions) (*aztables.Client, error) {
	tableName := sanitizeTableName(fmt.Sprintf("%s%s", opts.Prefix, opts.Table))
	url := fmt.Sprintf("%s/%s", opts.Endpoint, tableName)

	var optsClient aztables.ClientOptions
	if opts.SharedKeyCredential != nil && opts.SharedKeyCredential.AccountName() == "devstoreaccount1" {
		optsClient.InsecureAllowCredentialWithHTTP = true
	}

	if opts.UseDefaultAzureCredential {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}
		return aztables.NewClient(url, cred, &optsClient)
	}
	if opts.SharedKeyCredential != nil {
		return aztables.NewClientWithSharedKey(url, opts.SharedKeyCredential, &optsClient)
	}
	return nil, errors.New("please provide a valid Azure credential")
}

func sanitizeTableName(name string) string {
	if len(name) == 0 {
		return ""
	}

	var sanitized []byte
	if isLetter(name[0]) {
		sanitized = append(sanitized, name[0])
	} else {
		sanitized = append(sanitized, 'T')
	}

	for i := 1; i < len(name); i++ {
		if isAlphanumeric(name[i]) {
			sanitized = append(sanitized, name[i])
		}
	}

	for len(sanitized) < 3 {
		sanitized = append(sanitized, '0')
	}
	if len(sanitized) > 63 {
		sanitized = sanitized[:63]
	}
	return string(sanitized)
}

func isLetter(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isAlphanumeric(c byte) bool {
	return isLetter(c) || (c >= '0' && c <= '9')
}

func isNotFoundError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "ResourceNotFound")
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "Conflict") ||
		strings.Contains(errStr, "PreconditionFailed") ||
		strings.Contains(errStr, "ServiceUnavailable") ||
		strings.Contains(errStr, "429")
}

func decodeSnappyEntryEntity(value []byte) (*server.Entry, error) {
	var entity Entity
	if err := json.Unmarshal(value, &entity); err != nil {
		return nil, fmt.Errorf("failed to unmarshal entity: %w", err)
	}
	return decodeEntry(entity.Value)
}

func decodeEntry(value []byte) (*server.Entry, error) {
	entry := &server.Entry{}
	if err := server.DecodeEntrySnappy(value, entry); err != nil {
		return nil, fmt.Errorf("failed to decode entry: %w", err)
	}
	return entry, nil
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal: %v", err))
	}
	return data
}

func (s *AzureService) queryEntries(ctx context.Context, filter string, minTS, maxTS int64) enumerators.Enumerator[*server.Entry] {
	pager := s.client.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Filter: &filter,
		Format: ptr(aztables.MetadataFormatNone),
	})

	entities := NewAzureTableEnumerator(ctx, pager)
	entries := enumerators.Map(entities, func(e *Entity) (*server.Entry, error) {
		return decodeEntry(e.Value)
	})

	return enumerators.TakeWhile(entries, func(e *server.Entry) bool {
		return e.Timestamp > minTS && e.Timestamp <= maxTS
	})
}

func buildQuery(lower, upper string) string {
	return fmt.Sprintf("PartitionKey ge '%s' and PartitionKey le '%s'", lower, upper)
}

func calculateTimeBounds(current, min, max int64) struct{ Min, Max int64 } {
	bounds := struct{ Min, Max int64 }{Min: min}
	if min > current {
		bounds.Min = current
	}
	bounds.Max = max
	if max == 0 || max > current {
		bounds.Max = current
	}
	return bounds
}

func getSpaceLowerBound(space string, minTS int64, offset lexkey.LexKey) lexkey.LexKey {
	if len(offset) > 0 {
		return offset
	}
	return lexkey.EncodeFirst(server.DATA, server.SPACES, space, minTS)
}

func calculateSegmentBounds(ts int64, args *server.ConsumeSegment) struct {
	MinSeq, MaxSeq uint64
	MinTS, MaxTS   int64
} {
	bounds := struct {
		MinSeq, MaxSeq uint64
		MinTS, MaxTS   int64
	}{
		MinSeq: args.MinSequence,
		MaxSeq: args.MaxSequence,
		MinTS:  args.MinTimestamp,
	}
	if bounds.MinTS > ts {
		bounds.MinTS = ts
	}
	if args.MaxTimestamp == 0 || args.MaxTimestamp > ts {
		bounds.MaxTS = ts
	} else {
		bounds.MaxTS = args.MaxTimestamp
	}
	if bounds.MaxSeq == 0 {
		bounds.MaxSeq = math.MaxUint64
	}
	return bounds
}

func ptr[T any](v T) *T {
	return &v
}
