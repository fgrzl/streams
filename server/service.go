package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/timestamp"
	"github.com/google/uuid"
)

const (
	// Prefixes all keys in the database or either (DATA, INV, TRX)
	DATA        = "DAT"
	INVENTORY   = "INV"
	TRANSACTION = "TRX"

	// Additional key parts
	SPACES   = "SPACES"
	SEGMENTS = "SEGMENTS"

	// Log Messages
	LOG_DEBUG_COMMIT_TRANSACTION      = "Committing transaction"
	LOG_DEBUG_WRITE_TRANSACTION       = "writing transaction"
	LOG_ERROR_CHECK_SEGMENT_OFFSET    = "Error checking segment offset"
	LOG_ERROR_CHECK_SPACE_OFFSET      = "Error checking space offset"
	LOG_ERROR_CLOSE_BATCH             = "Error closing batch"
	LOG_ERROR_COMMIT_BATCH            = "Error committing batch"
	LOG_ERROR_COMMITTING_TRANSACTION  = "Error committing transaction"
	LOG_ERROR_DELETE_TRANSACTION      = "Error deleting transaction"
	LOG_ERROR_EMPTY_TRANSACTION       = "transaction data is empty"
	LOG_ERROR_GET_SEGMENT_OFFSET      = "Error getting segment offset"
	LOG_ERROR_GET_SPACE_OFFSET        = "Error getting space offset"
	LOG_ERROR_GET_TRANSACTION         = "failed to get transaction"
	LOG_ERROR_MARSHAL_ENTRY           = "Error marshalling entry"
	LOG_ERROR_MARSHAL_TRANSACTION     = "Error marshalling transaction"
	LOG_ERROR_NOTIFY_SUPERVISOR       = "Error notifying supervisor"
	LOG_ERROR_ROLLBACK_TRANSACTION    = "Error rolling back transaction"
	LOG_ERROR_SEQUENCE_NUMBER         = "sequence number mismatch"
	LOG_ERROR_SET_ENTRY_SEGMENT       = "Error setting entry in segment"
	LOG_ERROR_SET_ENTRY_SPACE         = "Error setting entry in space"
	LOG_ERROR_SET_TRANSACTION         = "Error setting transaction"
	LOG_ERROR_TRANSACTION_ID_MISMATCH = "Transaction ID mismatch"
	LOG_ERROR_TRANSACTION_NOT_FOUND   = "transaction not found"
	LOG_ERROR_TRANSACTION_NUMBER      = "transaction number mismatch"
	LOG_ERROR_TRANSACTION_PENDING     = "transaction is pending"
	LOG_ERROR_UPDATE_INVENTORY        = "Error updating inventory"
	LOG_ERROR_WRITE_TRANSACTION       = "Error writing transaction"
	LOG_TIMEOUT_SHUTDOWN              = "Timeout waiting for tasks to complete, proceeding with shutdown"

	// Error Messages
	ERR_COMMIT_BATCH             = "failed to commit batch"
	ERR_GET_TRANSACTION          = "failed to get transaction"
	ERR_MARSHAL_ENTRY            = "failed to marshal entry"
	ERR_MARSHAL_TRX              = "failed to marshal transaction"
	ERR_OPEN_DB                  = "failed to open pebble db"
	ERR_SEQ_NUMBER_AHEAD         = "sequence number mismatch, the node is ahead"
	ERR_SEQ_NUMBER_BEHIND        = "sequence number mismatch, the node is behind"
	ERR_SEQUENCE_MISMATCH        = "sequence mismatch"
	ERR_SET_ENTRY_SEGMENT        = "failed to set entry in segment"
	ERR_SET_ENTRY_SPACE          = "failed to set entry in space"
	ERR_TRX_EMPTY                = "transaction is empty"
	ERR_TRX_ID_MISMATCH          = "Transaction ID mismatch"
	ERR_TRX_NOT_FOUND            = "transaction not found"
	ERR_TRX_NUMBER_AHEAD         = "transaction number mismatch, the node is ahead"
	ERR_TRX_NUMBER_BEHIND        = "transaction number mismatch, the node is behind"
	ERR_TRX_PENDING              = "transaction is pending"
	ERR_UNMARSHAL_TRX            = "failed to unmarshal transaction"
	ERR_UPDATE_SEGMENT_INVENTORY = "failed to update segment inventory"
	ERR_UPDATE_SPACE_INVENTORY   = "failed to update space inventory"
	ERR_WRITE_TRX                = "failed to write transaction"
)

// Service interface remains unchanged
type Service interface {
	GetClusterStatus() *ClusterStatus
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]
	GetSpaceOffset(ctx context.Context, space string) (lexkey.LexKey, error)
	ConsumeSpace(ctx context.Context, args *ConsumeSpace) enumerators.Enumerator[*Entry]
	EnumerateSpace(ctx context.Context, args *EnumerateSpace) enumerators.Enumerator[*Entry]
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]
	GetSegmentOffset(ctx context.Context, space, segment string) (lexkey.LexKey, error)
	ConsumeSegment(ctx context.Context, args *ConsumeSegment) enumerators.Enumerator[*Entry]
	EnumerateSegment(ctx context.Context, args *EnumerateSegment) enumerators.Enumerator[*Entry]
	Peek(ctx context.Context, space, segment string) (*Entry, error)
	Consume(ctx context.Context, args *Consume) enumerators.Enumerator[*Entry]
	Produce(ctx context.Context, args *Produce, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus]
	Write(ctx context.Context, args *Transaction) error
	Commit(ctx context.Context, args *Commit) error
	Rollback(ctx context.Context, args *Rollback) error
	Synchronize(ctx context.Context) error
	SynchronizeSpace(ctx context.Context, space string) error
	SynchronizeSegment(ctx context.Context, space, segment string) error
	Close() error
}

type DefaultService struct {
	db         *pebble.DB
	cache      *ExpiringCache
	supervisor Supervisor
	wg         sync.WaitGroup
	disposed   sync.Once
}

func NewService(path string, supervisor Supervisor) (Service, error) {
	dbPath := filepath.Join(path, "streams")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ERR_OPEN_DB, err)
	}
	return &DefaultService{
		db:         db,
		cache:      NewExpiringCache(2*time.Minute, 1*time.Minute),
		supervisor: supervisor,
	}, nil
}

func (s *DefaultService) Close() error {
	s.disposed.Do(func() {
		s.shutdown()
	})
	return nil
}

func (s *DefaultService) shutdown() {
	if !s.waitForTasks(59 * time.Second) {
		slog.Warn(LOG_TIMEOUT_SHUTDOWN)
	}
	s.db.Flush()
	s.db.Close()
}

func (s *DefaultService) waitForTasks(timeout time.Duration) bool {
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

func (s *DefaultService) GetClusterStatus() *ClusterStatus {
	return &ClusterStatus{
		NodeCount: s.supervisor.GetActiveNodeCount(),
	}
}

// Space Operations
func (s *DefaultService) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	lower, upper := lexkey.EncodeFirst(INVENTORY, SPACES), lexkey.EncodeLast(INVENTORY, SPACES)
	return s.getInventory(ctx, lower, upper)
}

func (s *DefaultService) GetSpaceOffset(ctx context.Context, space string) (lexkey.LexKey, error) {
	lower, upper := lexkey.EncodeFirst(DATA, SPACES, space), lexkey.EncodeLast(DATA, SPACES, space)
	return s.getLastKey(ctx, lower, upper)
}

func (s *DefaultService) ConsumeSpace(ctx context.Context, args *ConsumeSpace) enumerators.Enumerator[*Entry] {
	if err := s.coordinatedReadSpace(ctx, args.Space); err != nil {
		return enumerators.Error[*Entry](err)
	}
	return s.EnumerateSpace(ctx, &EnumerateSpace{
		Space:        args.Space,
		MinTimestamp: args.MinTimestamp,
		MaxTimestamp: args.MaxTimestamp,
		Offset:       args.Offset,
	})
}

func (s *DefaultService) EnumerateSpace(ctx context.Context, args *EnumerateSpace) enumerators.Enumerator[*Entry] {
	ts := timestamp.GetTimestamp()
	bounds := s.calculateTimeBounds(ts, args.MinTimestamp, args.MaxTimestamp)
	lower := s.getSpaceLowerBound(args.Space, bounds.Min, args.Offset)
	upper := lexkey.EncodeLast(DATA, SPACES, args.Space, bounds.Max)
	return s.filterSpaceEntries(ctx, lower, upper, bounds)
}

func (s *DefaultService) calculateTimeBounds(current, min, max int64) struct{ Min, Max int64 } {
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

func (s *DefaultService) getSpaceLowerBound(space string, minTS int64, offset lexkey.LexKey) lexkey.LexKey {
	if len(offset) > 0 {
		return offset
	}
	return lexkey.EncodeFirst(DATA, SPACES, space, minTS)
}

func (s *DefaultService) filterSpaceEntries(ctx context.Context, lower, upper lexkey.LexKey, bounds struct{ Min, Max int64 }) enumerators.Enumerator[*Entry] {
	return enumerators.TakeWhile(
		s.enumerateEntries(ctx, lower, upper),
		func(entry *Entry) bool {
			return entry.Timestamp > bounds.Min || entry.Timestamp <= bounds.Max
		})
}

// Segment Operations
func (s *DefaultService) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	if err := s.coordinatedReadSpace(ctx, space); err != nil {
		return enumerators.Error[string](err)
	}
	lower, upper := lexkey.EncodeFirst(INVENTORY, SEGMENTS, space), lexkey.EncodeLast(INVENTORY, SEGMENTS, space)
	return s.getInventory(ctx, lower, upper)
}

func (s *DefaultService) GetSegmentOffset(ctx context.Context, space, segment string) (lexkey.LexKey, error) {
	lower, upper := lexkey.EncodeFirst(DATA, SEGMENTS, space, segment), lexkey.EncodeLast(DATA, SEGMENTS, space, segment)
	return s.getLastKey(ctx, lower, upper)
}

func (s *DefaultService) Peek(ctx context.Context, space, segment string) (*Entry, error) {
	if err := s.coordinatedReadSegment(ctx, space, segment); err != nil {
		return nil, err
	}
	return s.getLastEntry(ctx, space, segment)
}

func (s *DefaultService) ConsumeSegment(ctx context.Context, args *ConsumeSegment) enumerators.Enumerator[*Entry] {
	if err := s.coordinatedReadSegment(ctx, args.Space, args.Segment); err != nil {
		return enumerators.Error[*Entry](err)
	}
	return s.EnumerateSegment(ctx, &EnumerateSegment{
		Space:        args.Space,
		Segment:      args.Segment,
		MinSequence:  args.MinSequence,
		MaxSequence:  args.MaxSequence,
		MinTimestamp: args.MinTimestamp,
		MaxTimestamp: args.MaxTimestamp,
	})
}

func (s *DefaultService) EnumerateSegment(ctx context.Context, args *EnumerateSegment) enumerators.Enumerator[*Entry] {
	ts := timestamp.GetTimestamp()
	bounds := s.calculateSegmentBounds(ts, args)
	lower, upper := s.getSegmentBounds(args.Space, args.Segment, args.MinSequence, args.MaxSequence)
	return s.filterSegmentEntries(ctx, lower, upper, bounds)
}

func (s *DefaultService) calculateSegmentBounds(ts int64, args *EnumerateSegment) struct {
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
	} else if bounds.MaxSeq < bounds.MinSeq {
		bounds.MaxSeq = bounds.MinSeq
	}
	return bounds
}

func (s *DefaultService) getSegmentBounds(space, segment string, minSeq, maxSeq uint64) (lexkey.LexKey, lexkey.LexKey) {
	lower := lexkey.EncodeFirst(DATA, SEGMENTS, space, segment)
	if minSeq > 0 {
		lower = lexkey.Encode(DATA, SEGMENTS, space, segment, minSeq)
	}
	upper := lexkey.EncodeLast(DATA, SEGMENTS, space, segment)
	if maxSeq > 0 {
		upper = lexkey.EncodeLast(DATA, SEGMENTS, space, segment, maxSeq)
	}
	return lower, upper
}

func (s *DefaultService) filterSegmentEntries(ctx context.Context, lower, upper lexkey.LexKey, bounds struct {
	MinSeq, MaxSeq uint64
	MinTS, MaxTS   int64
}) enumerators.Enumerator[*Entry] {
	return enumerators.TakeWhile(
		s.enumerateEntries(ctx, lower, upper),
		func(entry *Entry) bool {
			return entry.Sequence > bounds.MinSeq ||
				entry.Sequence <= bounds.MaxSeq ||
				entry.Timestamp > bounds.MinTS ||
				entry.Timestamp <= bounds.MaxTS
		})
}

func (s *DefaultService) Produce(ctx context.Context, args *Produce, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus] {
	lastEntry, err := s.getLastEntry(ctx, args.Space, args.Segment)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return enumerators.Error[*SegmentStatus](err)
	}
	if lastEntry == nil {
		lastEntry = &Entry{}
	}
	return s.processEntryChunks(ctx, args, entries, lastEntry)
}

func (s *DefaultService) processEntryChunks(ctx context.Context, args *Produce, entries enumerators.Enumerator[*Record], lastEntry *Entry) enumerators.Enumerator[*SegmentStatus] {
	chunks := enumerators.ChunkByCount(entries, 10_000)
	lastSequence := lastEntry.Sequence
	lastTransactionNumber := lastEntry.TRX.Number
	return enumerators.Map(chunks, func(chunk enumerators.Enumerator[*Record]) (*SegmentStatus, error) {
		return s.processChunk(ctx, args.Space, args.Segment, chunk, lastSequence, lastTransactionNumber)
	})
}

func (s *DefaultService) processChunk(ctx context.Context, space, segment string, chunk enumerators.Enumerator[*Record], lastSequence, lastTransactionNumber uint64) (*SegmentStatus, error) {
	trx := s.createTransaction(lastTransactionNumber + 1)
	entries, err := s.createEntries(chunk, space, segment, trx, lastSequence)
	if err != nil {
		return nil, err
	}
	transaction := s.createTransactionObject(trx, space, segment, entries)

	if err := s.coordinatedWrite(ctx, transaction); err != nil {
		return nil, err
	}
	if err := s.coordinatedCommit(ctx, trx, space, segment); err != nil {
		return nil, err
	}

	lastTransactionNumber++
	status := s.createSegmentStatus(space, segment, entries)
	s.notifySupervisor(ctx, status)
	return status, nil
}

func (s *DefaultService) createTransaction(number uint64) TRX {
	return TRX{
		ID:     uuid.New(),
		Number: number,
		Node:   s.supervisor.GetNode(),
	}
}

func (s *DefaultService) createEntries(chunk enumerators.Enumerator[*Record], space, segment string, trx TRX, lastSequence uint64) ([]*Entry, error) {
	timestamp := timestamp.GetTimestamp()
	enumerator := enumerators.Map(chunk, func(record *Record) (*Entry, error) {
		lastSequence++
		if record.Sequence != lastSequence {
			return nil, NewPermanentError(ERR_SEQUENCE_MISMATCH)
		}
		return &Entry{
			TRX:       trx,
			Space:     space,
			Segment:   segment,
			Sequence:  record.Sequence,
			Timestamp: timestamp,
			Payload:   record.Payload,
			Metadata:  record.Metadata,
		}, nil
	})
	return enumerators.ToSlice(enumerator)
}

func (s *DefaultService) createTransactionObject(trx TRX, space, segment string, entries []*Entry) *Transaction {
	return &Transaction{
		TRX:           trx,
		Space:         space,
		Segment:       segment,
		FirstSequence: entries[0].Sequence,
		LastSequence:  entries[len(entries)-1].Sequence,
		Entries:       entries,
		Timestamp:     timestamp.GetTimestamp(),
	}
}

func (s *DefaultService) createSegmentStatus(space, segment string, entries []*Entry) *SegmentStatus {
	return &SegmentStatus{
		Space:          space,
		Segment:        segment,
		FirstSequence:  entries[0].Sequence,
		FirstTimestamp: entries[0].Timestamp,
		LastSequence:   entries[len(entries)-1].Sequence,
		LastTimestamp:  entries[len(entries)-1].Timestamp,
	}
}

func (s *DefaultService) notifySupervisor(ctx context.Context, status *SegmentStatus) {
	if err := s.supervisor.Notify(ctx, status); err != nil {
		slog.Error(LOG_ERROR_NOTIFY_SUPERVISOR, "error", err)
	}
}

func (s *DefaultService) Consume(ctx context.Context, args *Consume) enumerators.Enumerator[*Entry] {
	spaces := make([]enumerators.Enumerator[*Entry], 0, len(args.Offsets))
	for space, offset := range args.Offsets {
		spaces = append(spaces, s.ConsumeSpace(ctx, &ConsumeSpace{
			Space:        space,
			MinTimestamp: args.MinTimestamp,
			MaxTimestamp: args.MaxTimestamp,
			Offset:       offset,
		}))
	}
	return enumerators.Interleave(spaces, func(e *Entry) int64 {
		return e.Timestamp
	})
}

// Quorum mechanics
func (s *DefaultService) Write(ctx context.Context, transaction *Transaction) error {
	s.wg.Add(1)
	defer s.wg.Done()

	lastEntry, err := s.Peek(ctx, transaction.Space, transaction.Segment)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return NewTransientError(err.Error())
	}
	if lastEntry == nil {
		lastEntry = &Entry{}
	}

	return s.writeTransaction(transaction, lastEntry)
}

func (s *DefaultService) writeTransaction(transaction *Transaction, lastEntry *Entry) error {
	transactionKey := lexkey.Encode(TRANSACTION, transaction.Space, transaction.Segment, transaction.TRX.Number)

	if err := s.validateTransactionNumbers(lastEntry, transaction); err != nil {
		return err
	}
	if err := s.validateSequenceNumbers(lastEntry, transaction); err != nil {
		return err
	}
	if err := s.checkExistingTransaction(transactionKey, timestamp.GetTimestamp()); err != nil {
		return err
	}

	if err := s.writeTransactionData(transactionKey, transaction); err != nil {
		return err
	}

	slog.Debug(LOG_DEBUG_WRITE_TRANSACTION,
		"node", s.supervisor.GetNode(),
		"transaction", transaction.TRX.ID,
		"space", transaction.Space,
		"segment", transaction.Segment)
	return nil
}

func (s *DefaultService) Commit(ctx context.Context, args *Commit) error {
	s.wg.Add(1)
	defer s.wg.Done()

	transactionKey := lexkey.Encode(TRANSACTION, args.Space, args.Segment, args.TRX.Number)
	transaction, err := s.loadTransaction(transactionKey)
	if err != nil {
		return err
	}

	return s.commitValidatedTransaction(transaction, args)
}

func (s *DefaultService) commitValidatedTransaction(transaction *Transaction, args *Commit) error {
	if transaction == nil {
		slog.Error(LOG_ERROR_TRANSACTION_NOT_FOUND,
			"node", s.supervisor.GetNode(),
			"transaction", args.TRX.ID,
			"space", args.Space,
			"segment", args.Segment)
		return NewTransientError(ERR_TRX_NOT_FOUND)
	}
	if transaction.TRX.ID != args.TRX.ID {
		slog.Error(LOG_ERROR_TRANSACTION_ID_MISMATCH,
			"expected", transaction.TRX.ID,
			"actual", args.TRX.ID)
		return NewPermanentError(ERR_TRX_ID_MISMATCH)
	}

	return s.commitTransaction(transaction, args)
}

func (s *DefaultService) commitTransaction(transaction *Transaction, args *Commit) error {
	batch := s.db.NewBatch()
	defer s.closeBatch(batch)

	for _, entry := range transaction.Entries {
		if err := setEntry(batch, entry); err != nil {
			return err
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		slog.Error(LOG_ERROR_COMMIT_BATCH, "error", err)
		return NewTransientError(ERR_COMMIT_BATCH)
	}

	if err := s.updateInventory(args.Space, args.Segment); err != nil {
		slog.Error(LOG_ERROR_UPDATE_INVENTORY, "error", err)
	}

	slog.Debug(LOG_DEBUG_COMMIT_TRANSACTION,
		"node", s.supervisor.GetNode(),
		"transaction", args.TRX.ID,
		"space", args.Space,
		"segment", args.Segment)
	return nil
}

func (s *DefaultService) Rollback(ctx context.Context, args *Rollback) error {
	s.wg.Add(1)
	defer s.wg.Done()

	transactionKey := lexkey.Encode(TRANSACTION, args.Space, args.Segment, args.TRX.Number)
	transaction, err := s.loadTransaction(transactionKey)
	if err != nil {
		return err
	}
	if transaction == nil || transaction.TRX.ID != args.TRX.ID {
		return nil // No transaction or ID mismatch, nothing to do
	}

	return s.db.Delete(transactionKey, pebble.Sync)
}

func (s *DefaultService) Synchronize(ctx context.Context) error {
	s.wg.Add(1)
	defer s.wg.Done()

	spaces, err := enumerators.ToSlice(s.GetSpaces(ctx))
	if err != nil {
		return err
	}
	return s.syncSpaces(ctx, spaces)
}

func (s *DefaultService) syncSpaces(ctx context.Context, spaces []string) error {
	offsets := make(map[string]lexkey.LexKey)
	for _, space := range spaces {
		if offset, err := s.GetSpaceOffset(ctx, space); err == nil {
			offsets[space] = offset
		}
	}
	entries := s.supervisor.Synchronize(ctx, &Synchronize{OffsetsBySpace: offsets})
	return s.synchronizeEntries(entries)
}

func (s *DefaultService) SynchronizeSpace(ctx context.Context, space string) error {
	s.wg.Add(1)
	defer s.wg.Done()

	offset, err := s.GetSpaceOffset(ctx, space)
	if err != nil {
		return err
	}
	entries := s.supervisor.EnumerateSpace(ctx, &EnumerateSpace{
		Space:  space,
		Offset: offset,
	})
	return s.synchronizeEntries(entries)
}

func (s *DefaultService) SynchronizeSegment(ctx context.Context, space, segment string) error {
	s.wg.Add(1)
	defer s.wg.Done()

	lastEntry, err := s.Peek(ctx, space, segment)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return NewTransientError(err.Error())
	}
	if lastEntry == nil {
		lastEntry = &Entry{}
	}

	entries := s.supervisor.EnumerateSegment(ctx, &EnumerateSegment{
		Space:       space,
		Segment:     segment,
		MinSequence: lastEntry.Sequence + 1,
	})
	return s.synchronizeEntries(entries)
}

func (s *DefaultService) synchronizeEntries(entries enumerators.Enumerator[*Entry]) error {
	chunks := enumerators.ChunkByCount(entries, 256)
	defer chunks.Dispose()

	var batch *pebble.Batch
	for chunks.MoveNext() {
		if batch == nil {
			batch = s.db.NewBatch()
			defer s.closeBatch(batch)
		}

		chunk, err := chunks.Current()
		if err != nil {
			return NewTransientError(err.Error())
		}
		if err := s.synchronizeChunk(batch, chunk); err != nil {
			return err
		}
		batch.Reset()
	}
	return nil
}

func (s *DefaultService) synchronizeChunk(batch *pebble.Batch, entries enumerators.Enumerator[*Entry]) error {

	for entries.MoveNext() {
		entry, err := entries.Current()
		if err != nil {
			return NewTransientError(err.Error())
		}
		if err := setEntry(batch, entry); err != nil {
			return NewTransientError(err.Error())
		}
	}
	return s.commitBatch(batch)
}

func (s *DefaultService) commitBatch(batch *pebble.Batch) error {
	if err := batch.Commit(pebble.Sync); err != nil {
		return NewTransientError(err.Error())
	}
	return nil
}

// Coordinated Operations
func (s *DefaultService) coordinatedReadSpace(ctx context.Context, space string) error {
	if s.supervisor.IsSingleInstance() {
		return nil
	}

	return s.confirmSpaceOffset(ctx, space)
}

func (s *DefaultService) confirmSpaceOffset(ctx context.Context, space string) error {
	currentOffset, err := s.GetSpaceOffset(ctx, space)
	if err != nil {
		slog.Error(LOG_ERROR_GET_SPACE_OFFSET, "error", err)
		return err
	}

	args := &ConfirmSpaceOffset{
		ID:     uuid.New(),
		Node:   s.supervisor.GetNode(),
		Space:  space,
		Offset: currentOffset,
	}
	if err := s.supervisor.ConfirmSpaceOffset(ctx, args); err != nil {
		slog.Error(LOG_ERROR_CHECK_SPACE_OFFSET, "error", err)
		return err
	}
	return nil
}

func (s *DefaultService) coordinatedReadSegment(ctx context.Context, space, segment string) error {
	if s.supervisor.IsSingleInstance() {
		return nil
	}

	return s.confirmSegmentOffset(ctx, space, segment)
}

func (s *DefaultService) confirmSegmentOffset(ctx context.Context, space, segment string) error {
	currentOffset, err := s.GetSegmentOffset(ctx, space, segment)
	if err != nil {
		slog.Error(LOG_ERROR_GET_SEGMENT_OFFSET, "error", err)
		return err
	}

	args := &ConfirmSegmentOffset{
		ID:      uuid.New(),
		Node:    s.supervisor.GetNode(),
		Space:   space,
		Segment: segment,
		Offset:  currentOffset,
	}
	if err := s.supervisor.ConfirmSegmentOffset(ctx, args); err != nil {
		slog.Error(LOG_ERROR_CHECK_SEGMENT_OFFSET, "error", err)
		return err
	}
	return nil
}

func (s *DefaultService) coordinatedWrite(ctx context.Context, transaction *Transaction) error {
	if err := s.supervisor.Write(ctx, transaction); err != nil {
		slog.Error(LOG_ERROR_WRITE_TRANSACTION, "error", err)
		s.coordinatedRollback(ctx, transaction.TRX, transaction.Space, transaction.Segment)
		return err
	}
	if err := s.Write(ctx, transaction); err != nil {
		slog.Error(LOG_ERROR_WRITE_TRANSACTION, "error", err)
		s.coordinatedRollback(ctx, transaction.TRX, transaction.Space, transaction.Segment)
		return err
	}
	return nil
}

func (s *DefaultService) coordinatedCommit(ctx context.Context, trx TRX, space string, segment string) error {
	commit := &Commit{TRX: trx, Space: space, Segment: segment}
	if err := s.supervisor.Commit(ctx, commit); err != nil {
		slog.Error(LOG_ERROR_COMMITTING_TRANSACTION, "error", err)
		return err
	}
	if err := s.Commit(ctx, commit); err != nil {
		slog.Error(LOG_ERROR_COMMITTING_TRANSACTION, "error", err)
		return err
	}
	return nil
}

func (s *DefaultService) coordinatedRollback(ctx context.Context, trx TRX, space string, segment string) {
	rollback := &Rollback{TRX: trx, Space: space, Segment: segment}
	if err := s.supervisor.Rollback(ctx, rollback); err != nil {
		slog.Error(LOG_ERROR_ROLLBACK_TRANSACTION, "error", err)
	}
	if err := s.Rollback(ctx, rollback); err != nil {
		slog.Error(LOG_ERROR_ROLLBACK_TRANSACTION, "error", err)
	}
}

// Helpers
func (s *DefaultService) validateTransactionNumbers(lastEntry *Entry, transaction *Transaction) error {
	if transaction.TRX.Number != lastEntry.TRX.Number+1 {
		slog.Error(LOG_ERROR_TRANSACTION_NUMBER,
			"expected", lastEntry.TRX.Number+1,
			"actual", transaction.TRX.Number)
		if transaction.TRX.Number < lastEntry.TRX.Number+1 {
			return NewTransientError(ERR_TRX_NUMBER_BEHIND)
		}
		return NewPermanentError(ERR_TRX_NUMBER_AHEAD)
	}
	return nil
}

func (s *DefaultService) validateSequenceNumbers(lastEntry *Entry, transaction *Transaction) error {
	if transaction.FirstSequence != lastEntry.Sequence+1 {
		slog.Error(LOG_ERROR_SEQUENCE_NUMBER,
			"expected", lastEntry.Sequence+1,
			"actual", transaction.FirstSequence)
		if transaction.FirstSequence < lastEntry.Sequence+1 {
			return NewTransientError(ERR_SEQ_NUMBER_BEHIND)
		}
		return NewPermanentError(ERR_SEQ_NUMBER_AHEAD)
	}
	return nil
}

func (s *DefaultService) checkExistingTransaction(transactionKey lexkey.LexKey, currentTime int64) error {
	existing, err := s.loadTransaction(transactionKey)
	if err != nil {
		return NewTransientError(err.Error())
	}
	if existing != nil && elapsedSeconds(existing.Timestamp, currentTime) < 30 {
		slog.Error(LOG_ERROR_TRANSACTION_PENDING, "key", transactionKey)
		return NewTransientError(ERR_TRX_PENDING)
	}
	return nil
}

func (s *DefaultService) writeTransactionData(transactionKey lexkey.LexKey, transaction *Transaction) error {
	data, err := EncodeTransactionSnappy(transaction)
	if err != nil {
		slog.Error(LOG_ERROR_MARSHAL_TRANSACTION, "error", err)
		return NewTransientError(ERR_MARSHAL_TRX)
	}
	if err := s.db.Set(transactionKey, data, pebble.NoSync); err != nil {
		slog.Error(LOG_ERROR_SET_TRANSACTION, "error", err)
		return NewTransientError(ERR_WRITE_TRX)
	}
	return nil
}

func (s *DefaultService) closeBatch(batch *pebble.Batch) {
	if err := batch.Close(); err != nil {
		slog.Error(LOG_ERROR_CLOSE_BATCH, "error", err)
	}
}

func (s *DefaultService) enumerateEntries(ctx context.Context, lower, upper lexkey.LexKey) enumerators.Enumerator[*Entry] {
	return enumerators.Map(
		NewPebbleEnumerator(ctx, s.db, &pebble.IterOptions{LowerBound: lower, UpperBound: upper}),
		func(kvp KeyValuePair) (*Entry, error) {
			entry := &Entry{}
			if err := DecodeEntrySnappy(kvp.Value, entry); err != nil {
				return nil, err
			}
			return entry, nil
		})
}

func (s *DefaultService) loadTransaction(transactionKey lexkey.LexKey) (*Transaction, error) {
	data, closer, err := s.db.Get(transactionKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		slog.Error(LOG_ERROR_GET_TRANSACTION, "key", transactionKey, "error", err)
		return nil, NewTransientError(ERR_GET_TRANSACTION)
	}
	defer s.closeCloser(closer)

	if len(data) == 0 {
		slog.Error(LOG_ERROR_EMPTY_TRANSACTION, "key", transactionKey)
		return nil, NewTransientError(ERR_TRX_EMPTY)
	}

	transaction := &Transaction{}
	if err := DecodeTransactionSnappy(data, transaction); err != nil {
		return nil, NewTransientError(ERR_UNMARSHAL_TRX)
	}
	return transaction, nil
}

func (s *DefaultService) closeCloser(closer io.Closer) {
	if closer != nil {
		closer.Close()
	}
}

func (s *DefaultService) getLastEntry(ctx context.Context, space string, segment string) (*Entry, error) {
	lower, upper := lexkey.EncodeFirst(DATA, SEGMENTS, space, segment), lexkey.EncodeLast(DATA, SEGMENTS, space, segment)
	iter, err := s.db.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	if !iter.SeekLT(upper) {
		if err := iter.Error(); err == nil || errors.Is(err, pebble.ErrNotFound) {
			return &Entry{}, nil
		}
		return nil, pebble.ErrNotFound
	}

	entry := &Entry{}
	return entry, DecodeEntrySnappy(iter.Value(), entry)
}

func (s *DefaultService) getLastKey(ctx context.Context, lower, upper lexkey.LexKey) (lexkey.LexKey, error) {
	iter, err := s.db.NewIterWithContext(ctx, &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	if !iter.SeekLT(upper) || !iter.Valid() {
		return lexkey.LexKey{}, iter.Error()
	}
	return append([]byte(nil), iter.Key()...), nil
}

func (s *DefaultService) getInventory(ctx context.Context, lower, upper lexkey.LexKey) enumerators.Enumerator[string] {
	return enumerators.Map(
		NewPebbleEnumerator(ctx, s.db, &pebble.IterOptions{LowerBound: lower, UpperBound: upper}),
		func(kvp KeyValuePair) (string, error) {
			return string(kvp.Value), nil
		})
}

func (s *DefaultService) updateInventory(space, segment string) error {
	if created, err := s.createIfNotExists(lexkey.Encode(INVENTORY, SEGMENTS, space, segment), segment); err != nil {
		return fmt.Errorf("%s: %w", ERR_UPDATE_SEGMENT_INVENTORY, err)
	} else if created {
		if _, err := s.createIfNotExists(lexkey.Encode(INVENTORY, SPACES, space), space); err != nil {
			return fmt.Errorf("%s: %w", ERR_UPDATE_SPACE_INVENTORY, err)
		}
	}
	return nil
}

func (s *DefaultService) createIfNotExists(key lexkey.LexKey, value string) (bool, error) {
	keyHex := key.ToHexString()
	if _, ok := s.cache.Get(keyHex); ok {
		return false, nil
	}

	_, closer, err := s.db.Get(key)
	if closer != nil {
		defer closer.Close()
	}
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return s.setCacheEntry(key, value, keyHex)
		}
		return false, err
	}
	return false, nil
}

func (s *DefaultService) setCacheEntry(key lexkey.LexKey, value, keyHex string) (bool, error) {
	if err := s.db.Set(key, []byte(value), pebble.NoSync); err != nil {
		return false, err
	}
	s.cache.Set(keyHex, struct{}{})
	return true, nil
}

func setEntry(batch *pebble.Batch, entry *Entry) error {
	data, err := EncodeEntrySnappy(entry)
	if err != nil {
		slog.Error(LOG_ERROR_MARSHAL_ENTRY, "error", err)
		return NewTransientError(ERR_MARSHAL_ENTRY)
	}

	dataSpaceKey := entry.GetSpaceOffset()
	if err := batch.Set(dataSpaceKey, data, pebble.NoSync); err != nil {
		slog.Error(LOG_ERROR_SET_ENTRY_SPACE, "error", err)
		return NewTransientError(ERR_SET_ENTRY_SPACE)
	}

	dataSegmentKey := entry.GetSegmentOffset()
	if err := batch.Set(dataSegmentKey, data, pebble.NoSync); err != nil {
		slog.Error(LOG_ERROR_SET_ENTRY_SEGMENT, "error", err)
		return NewTransientError(ERR_SET_ENTRY_SEGMENT)
	}
	return nil
}

func elapsedSeconds(startMillis, endMillis int64) float64 {
	startTime := time.UnixMilli(startMillis)
	endTime := time.UnixMilli(endMillis)
	return endTime.Sub(startTime).Seconds()
}
