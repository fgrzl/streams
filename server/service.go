package server

import (
	"context"
	"errors"
	"fmt"
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
)

type Service interface {

	// Get active node count.
	GetClusterStatus() *ClusterStatus

	// Get all the spaces
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]

	// Get space offset
	GetSpaceOffset(ctx context.Context, space string) (lexkey.LexKey, error)

	// Consume a space with coordination of the quorum.
	ConsumeSpace(ctx context.Context, args *ConsumeSpace) enumerators.Enumerator[*Entry]

	// Enumerate a space without coordination of the quorum.
	EnumerateSpace(ctx context.Context, args *EnumerateSpace) enumerators.Enumerator[*Entry]

	// Get all segments in a space.
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]

	// Get space offset
	GetSegmentOffset(ctx context.Context, space, segment string) (lexkey.LexKey, error)

	// Consume a segment with coordination of the quorum.
	ConsumeSegment(ctx context.Context, args *ConsumeSegment) enumerators.Enumerator[*Entry]

	// Enumerate a segment without coordination of the quorum.
	EnumerateSegment(ctx context.Context, args *EnumerateSegment) enumerators.Enumerator[*Entry]

	// Get the last entry in a stream.
	Peek(ctx context.Context, space, segment string) (*Entry, error)

	// Consume a interleaved spaces with coordination of the quorum.
	Consume(ctx context.Context, args *Consume) enumerators.Enumerator[*Entry]

	// Produce stream entries.
	Produce(ctx context.Context, args *Produce, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus]

	// Write the transaction.
	Write(ctx context.Context, args *Transaction) error

	// Commit the transaction.
	Commit(ctx context.Context, args *Commit) error

	// Rollback the transaction.
	Rollback(ctx context.Context, args *Rollback) error

	// Synchronize this node with other nodes.
	Synchronize(ctx context.Context) error
	SynchronizeSpace(ctx context.Context, space string) error
	SynchronizeSegment(ctx context.Context, space, segment string) error

	Close() error
}

type SegmentStatus struct {
	Space          string `json:"space"`
	Segment        string `json:"segment"`
	FirstSequence  uint64 `json:"first_sequence"`
	FirstTimestamp int64  `json:"first_timestamp"`
	LastSequence   uint64 `json:"last_sequence"`
	LastTimestamp  int64  `json:"last_timestamp"`
}

func (s *SegmentStatus) GetDiscriminator() string {
	return fmt.Sprintf("%T", s)
}

func (s *SegmentStatus) GetRoute() string {
	route := "status"
	if s.Space != "" {
		return "." + s.Space
	}
	if s.Segment != "" {
		return "." + s.Segment
	}
	return route
}

type Record struct {
	Sequence uint64            `json:"sequence"`
	Payload  []byte            `json:"payload"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Entry struct {
	Sequence  uint64            `json:"sequence"`
	Timestamp int64             `json:"timestamp,omitempty"`
	TRX       TRX               `json:"trx"`
	Payload   []byte            `json:"payload"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Space     string            `json:"space"`
	Segment   string            `json:"segment"`
}

func (e *Entry) GetSpaceOffset() lexkey.LexKey {
	return lexkey.Encode(DATA, SPACES, e.Space, e.Timestamp, e.Segment, e.Sequence)
}

func (e *Entry) GetSegmentOffset() lexkey.LexKey {
	return lexkey.Encode(DATA, SEGMENTS, e.Space, e.Segment, e.Sequence)
}

var (
	_ Service = (*DefaultService)(nil)
)

func NewService(path string, supervisor Supervisor) (Service, error) {
	dbPath := filepath.Join(path, "streams")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}
	return &DefaultService{
		db:         db,
		cache:      NewExpiringCache(2*time.Minute, 1*time.Minute),
		supervisor: supervisor,
	}, nil
}

type DefaultService struct {
	db         *pebble.DB
	cache      *ExpiringCache
	supervisor Supervisor
	wg         sync.WaitGroup
	disposed   sync.Once
}

func (s *DefaultService) Close() error {
	s.disposed.Do(func() {
		done := make(chan struct{})

		go func() {
			defer close(done)
			s.wg.Wait()
		}()

		select {
		case <-done:
			s.db.Flush()
			s.db.Close()
		case <-time.After(59 * time.Second):
			slog.Warn("Timeout waiting for tasks to complete, proceeding with shutdown")
		}
	})

	return nil
}

func (s *DefaultService) GetClusterStatus() *ClusterStatus {
	return &ClusterStatus{
		NodeCount: s.supervisor.GetActiveNodeCount(),
	}
}

//
// Space Operations
//

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
	return s.EnumerateSpace(
		ctx,
		&EnumerateSpace{
			Space:        args.Space,
			MinTimestamp: args.MinTimestamp,
			MaxTimestamp: args.MaxTimestamp,
			Offset:       args.Offset,
		})
}

func (s *DefaultService) EnumerateSpace(ctx context.Context, args *EnumerateSpace) enumerators.Enumerator[*Entry] {
	// capture the current timestamp this will be the upper bound
	ts := timestamp.GetTimestamp()

	var minTS, maxTS int64
	if args.MinTimestamp > ts {
		minTS = ts
	} else {
		minTS = args.MinTimestamp
	}

	if args.MaxTimestamp == 0 {
		maxTS = ts
	} else if args.MaxTimestamp > ts {
		maxTS = ts
	}

	var lower lexkey.LexKey
	if len(args.Offset) > 0 {
		lower = args.Offset
	} else {
		lower = lexkey.EncodeFirst(DATA, SPACES, args.Space, minTS)
	}

	upper := lexkey.EncodeLast(DATA, SPACES, args.Space, maxTS)
	return enumerators.TakeWhile(
		s.enumerateEntries(ctx, lower, upper),
		func(entry *Entry) bool {
			return entry.Timestamp > minTS || entry.Timestamp <= maxTS
		})
}

//
// Segment Operations
//

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
	return s.EnumerateSegment(
		ctx,
		&EnumerateSegment{
			Space:        args.Space,
			Segment:      args.Segment,
			MinSequence:  args.MinSequence,
			MaxSequence:  args.MaxSequence,
			MinTimestamp: args.MinTimestamp,
			MaxTimestamp: args.MaxTimestamp,
		})
}

func (s *DefaultService) EnumerateSegment(ctx context.Context, args *EnumerateSegment) enumerators.Enumerator[*Entry] {
	// capture the current timestamp this will be the upper bound
	ts := timestamp.GetTimestamp()

	var lower, upper lexkey.LexKey
	if args.MinSequence == 0 {
		lower = lexkey.EncodeFirst(DATA, SEGMENTS, args.Space, args.Segment)
	} else {
		lower = lexkey.Encode(DATA, SEGMENTS, args.Space, args.Segment, args.MinSequence)
	}
	if args.MaxSequence == 0 {
		upper = lexkey.EncodeLast(DATA, SEGMENTS, args.Space, args.Segment)
	} else {
		upper = lexkey.EncodeLast(DATA, SEGMENTS, args.Space, args.Segment, args.MaxSequence)
	}

	var minSeq, maxSeq uint64
	if args.MinSequence > 0 {
		minSeq = args.MinSequence
	}

	if args.MaxSequence > minSeq {
		maxSeq = args.MaxSequence
	} else {
		maxSeq = math.MaxUint64
	}

	var minTS, maxTS int64

	if args.MinTimestamp > ts {
		minTS = ts
	} else {
		minTS = args.MinTimestamp
	}

	if args.MaxTimestamp == 0 {
		maxTS = ts
	} else if args.MaxTimestamp > ts {
		maxTS = ts
	}

	return enumerators.TakeWhile(
		s.enumerateEntries(ctx, lower, upper),
		func(entry *Entry) bool {
			return entry.Sequence > minSeq || entry.Sequence <= maxSeq || entry.Timestamp > minTS || entry.Timestamp <= maxTS
		})
}

func (s *DefaultService) Produce(ctx context.Context, args *Produce, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus] {

	space, segment := args.Space, args.Segment

	// we need to peek at the last entry so we can determine the next sequence number
	lastEntry, err := s.getLastEntry(ctx, space, segment)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return enumerators.Error[*SegmentStatus](err)
	}

	chunks := enumerators.ChunkByCount(entries, 10_000)
	lastSequence := lastEntry.Sequence
	lastTransactionNumber := lastEntry.TRX.Number
	return enumerators.Map(chunks,
		func(chunk enumerators.Enumerator[*Record]) (*SegmentStatus, error) {
			timestamp := timestamp.GetTimestamp()
			trx := TRX{
				ID:     uuid.New(),
				Number: lastTransactionNumber + 1,
				Node:   s.supervisor.GetNode(),
			}

			enumerator := enumerators.Map(chunk, func(record *Record) (*Entry, error) {
				nextSequence := lastSequence + 1
				if record.Sequence != nextSequence {
					return nil, fmt.Errorf("sequence mismatch")
				}
				lastSequence = record.Sequence
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

			// convert the enumerator to a slice so we can write to the supervisor
			entries, err := enumerators.ToSlice(enumerator)
			if err != nil {
				return nil, err
			}

			// use the supervisor to coordinate the transaction
			transaction := &Transaction{
				TRX:           trx,
				Space:         space,
				Segment:       segment,
				FirstSequence: entries[0].Sequence,
				LastSequence:  entries[len(entries)-1].Sequence,
				Entries:       entries,
				Timestamp:     timestamp,
			}

			if err := s.coordinatedWrite(ctx, transaction); err != nil {
				return nil, err
			}

			if err := s.coordinatedCommit(ctx, trx, space, segment); err != nil {
				return nil, err
			}

			lastTransactionNumber += 1

			status := &SegmentStatus{
				Space:          space,
				Segment:        segment,
				FirstSequence:  entries[0].Sequence,
				FirstTimestamp: entries[0].Timestamp,
				LastSequence:   entries[len(entries)-1].Sequence,
				LastTimestamp:  entries[len(entries)-1].Timestamp,
			}

			if err := s.supervisor.Notify(ctx, status); err != nil {
				slog.Error("Error notifying supervisor", "error", err)
			}

			return status, nil
		})
}

func (s *DefaultService) Consume(ctx context.Context, args *Consume) enumerators.Enumerator[*Entry] {
	var spaces []enumerators.Enumerator[*Entry]
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

//
// Quorum mechanics
//

func (s *DefaultService) Write(ctx context.Context, transaction *Transaction) error {
	node := s.supervisor.GetNode()

	s.wg.Add(1)
	defer s.wg.Done()

	space, segment, transactionNumber := transaction.Space, transaction.Segment, transaction.TRX.Number

	// peek at the last entry so we can determine the next sequence number and transaction number
	lastEntry, err := s.Peek(ctx, space, segment)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return NewTransientError(err.Error())
	}

	// validate the transaction number
	expectedTransactionNumber := lastEntry.TRX.Number + 1
	if transactionNumber < expectedTransactionNumber {
		slog.Error("transaction number mismatch", "expected", expectedTransactionNumber, "actual", transactionNumber)
		return NewTransientError("transaction number mismatch, the node is behind")
	} else if transactionNumber > expectedTransactionNumber {
		slog.Error("transaction number mismatch", "expected", expectedTransactionNumber, "actual", transactionNumber)
		return NewPermanentError("transaction number mismatch, the node is ahead")
	}

	// validate the sequence number
	epectedSequence := lastEntry.Sequence + 1
	if transaction.FirstSequence < epectedSequence {
		slog.Error("sequence number mismatch", "expected", epectedSequence, "actual", transaction.FirstSequence)
		return NewTransientError("sequence number mismatch, the node is behind")
	} else if transaction.FirstSequence > epectedSequence {
		slog.Error("sequence number mismatch", "expected", epectedSequence, "actual", transaction.FirstSequence)
		return NewPermanentError("sequence number mismatch, the node is ahead")
	}

	transactionKey := lexkey.Encode(TRANSACTION, space, segment, transactionNumber)
	existing, err := s.loadTransaction(transactionKey)
	if err != nil {
		return NewTransientError(err.Error())
	}
	if existing != nil {
		existing := &Transaction{}
		if elapsedSeconds(existing.Timestamp, timestamp.GetTimestamp()) < 30 {
			slog.Error("transaction is pending", "key", transactionKey)
			return NewTransientError("transaction is pending")
		}
		slog.Error("transaction is pending", "key", transactionKey)
		return NewTransientError("transaction is pending")
	}

	data, err := EncodeTransactionSnappy(transaction)
	if err != nil {
		slog.Error("Error marshalling transaction", "error", err)
		return NewTransientError("failed to marshal transaction")
	}
	if err := s.db.Set(transactionKey, data, pebble.NoSync); err != nil {
		slog.Error("Error setting transaction", "error", err)
		return NewTransientError("failed to write transaction")
	}
	slog.Debug("writing transaction", "node", node, "transaction", transaction.TRX.ID, "space", transaction.Space, "segment", transaction.Segment)
	return nil
}

func (s *DefaultService) Commit(ctx context.Context, args *Commit) error {
	node := s.supervisor.GetNode()

	s.wg.Add(1)
	defer s.wg.Done()

	space, segment, trx := args.Space, args.Segment, args.TRX
	transactionKey := lexkey.Encode(TRANSACTION, space, segment, trx.Number)
	transaction, err := s.loadTransaction(transactionKey)
	if err != nil {
		return err
	}

	if transaction == nil {
		slog.Error("transaction not found", "node", node, "transaction", args.TRX.ID, "space", args.Space, "segment", args.Segment)
		return NewTransientError("transaction not found")
	}

	if transaction.TRX.ID != trx.ID {
		slog.Error("Transaction ID mismatch", "expected", transaction.TRX.ID, "actual", trx.ID)
		return NewPermanentError("Transaction ID mismatch")
	}

	batch := s.db.NewBatch()
	defer func() {
		if err := batch.Close(); err != nil {
			slog.Error("Error closing batch", "error", err)
		}
	}()

	for _, entry := range transaction.Entries {
		if err := setEntry(batch, entry); err != nil {
			return err
		}
	}

	// if err := batch.Delete(transactionKey, pebble.NoSync); err != nil {
	// 	slog.Error("Error deleting transaction", "error", err)
	// 	return NewTransientError("failed to set transaction")
	// }

	// commit batch
	if err := batch.Commit(pebble.Sync); err != nil {
		slog.Error("Error committing batch", "error", err)
		return NewTransientError("failed to commit batch")
	}

	if err := s.updateInventory(space, segment); err != nil {
		slog.Error("Error updating inventory", "error", err)
	}
	slog.Debug("Committing transaction", "node", node, "transaction", args.TRX.ID, "space", args.Space, "segment", args.Segment)
	return nil
}

func (s *DefaultService) Rollback(ctx context.Context, args *Rollback) error {
	s.wg.Add(1)
	defer s.wg.Done()

	space, segment, trx := args.Space, args.Segment, args.TRX
	transactionKey := lexkey.Encode(TRANSACTION, space, segment, trx.Number)
	transaction, err := s.loadTransaction(transactionKey)
	if err != nil {
		return err
	}
	if transaction == nil {
		return nil
	}

	if transaction.TRX.ID != trx.ID {
		return fmt.Errorf("transaction id mismatch")
	}

	if err := s.db.Delete(transactionKey, pebble.Sync); err != nil {
		return err
	}
	return nil
}

func (s *DefaultService) Synchronize(ctx context.Context) error {
	s.wg.Add(1)
	defer s.wg.Done()

	// get spaces
	spaces, err := enumerators.ToSlice(s.GetSpaces(ctx))
	if err != nil {
		return err
	}
	offsets := make(map[string]lexkey.LexKey)
	for _, space := range spaces {
		offset, err := s.GetSpaceOffset(ctx, space)
		if err != nil {
			continue
		}
		offsets[space] = offset
	}
	entries := s.supervisor.Synchronize(ctx, &Synchronize{OffsetsBySpace: offsets})
	return s.synchonizeEntries(entries)
}

func (s *DefaultService) SynchronizeSpace(ctx context.Context, space string) error {
	s.wg.Add(1)
	defer s.wg.Done()
	// get my last space offset

	offset, err := s.GetSpaceOffset(ctx, space)
	if err != nil {
		return err
	}
	entries := s.supervisor.EnumerateSpace(
		ctx,
		&EnumerateSpace{
			Space:  space,
			Offset: offset,
		})
	return s.synchonizeEntries(entries)
}

func (s *DefaultService) SynchronizeSegment(ctx context.Context, space, segment string) error {
	s.wg.Add(1)
	defer s.wg.Done()

	// peek at the last entry so we can determine the next sequence number and transaction number
	lastEntry, err := s.Peek(ctx, space, segment)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return NewTransientError(err.Error())
	}

	entries := s.supervisor.EnumerateSegment(
		ctx,
		&EnumerateSegment{
			Space:       space,
			Segment:     segment,
			MinSequence: lastEntry.Sequence + 1,
		})
	return s.synchonizeEntries(entries)
}

func (s *DefaultService) synchonizeEntries(entries enumerators.Enumerator[*Entry]) error {

	chunks := enumerators.ChunkByCount(entries, 256)
	defer chunks.Dispose()
	for chunks.MoveNext() {
		chunk, err := chunks.Current()
		if err != nil {
			return NewTransientError(err.Error())
		}
		if err := s.synchronizeChunk(chunk); err != nil {
			return err
		}
	}
	return nil
}

func (s *DefaultService) synchronizeChunk(chunk enumerators.Enumerator[*Entry]) error {
	entries := enumerators.Peekable(chunk)
	if !entries.HasNext() {
		return nil
	}

	batch := s.db.NewBatch()
	defer func() {
		if err := batch.Close(); err != nil {
			slog.Error("Error closing batch", "error", err)
		}
	}()
	for entries.MoveNext() {
		entry, err := entries.Current()
		if err != nil {
			return NewTransientError(err.Error())
		}
		if err := setEntry(batch, entry); err != nil {
			return NewTransientError(err.Error())
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return NewTransientError(err.Error())
	}
	return nil
}

//
// Coordinated Operations
//

func (s *DefaultService) coordinatedReadSpace(ctx context.Context, space string) error {

	if s.supervisor.IsSingleInstance() {
		return nil
	}

	currentOffset, err := s.GetSpaceOffset(ctx, space)
	if err != nil {
		slog.Error("Error getting space offset", "error", err)
		return err
	}

	args := &ConfirmSpaceOffset{
		ID:     uuid.New(),
		Node:   s.supervisor.GetNode(),
		Space:  space,
		Offset: currentOffset,
	}
	if err := s.supervisor.ConfirmSpaceOffset(ctx, args); err != nil {
		slog.Error("Error checking space offset", "error", err)
		return err
	}

	return nil
}

func (s *DefaultService) coordinatedReadSegment(ctx context.Context, space, segment string) error {

	if s.supervisor.IsSingleInstance() {
		return nil
	}

	currentOffset, err := s.GetSegmentOffset(ctx, space, segment)
	if err != nil {
		slog.Error("Error getting segment offset", "error", err)
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
		slog.Error("Error checking segment offset", "error", err)
		return err
	}

	return nil
}

func (s *DefaultService) coordinatedWrite(ctx context.Context, transaction *Transaction) error {

	if err := s.supervisor.Write(ctx, transaction); err != nil {
		slog.Error("Error writing transaction", "error", err)
		s.coordinatedRollback(ctx, transaction.TRX, transaction.Space, transaction.Segment)
		return err
	}

	if err := s.Write(ctx, transaction); err != nil {
		slog.Error("Error writing transaction", "error", err)
		s.coordinatedRollback(ctx, transaction.TRX, transaction.Space, transaction.Segment)
		return err
	}
	return nil
}

func (s *DefaultService) coordinatedCommit(ctx context.Context, trx TRX, space string, segment string) error {

	commit := &Commit{TRX: trx, Space: space, Segment: segment}
	if err := s.supervisor.Commit(ctx, commit); err != nil {
		slog.Error("Error committing transaction", "error", err)
		return err
	}
	if err := s.Commit(ctx, commit); err != nil {
		slog.Error("Error committing transaction", "error", err)
		return err
	}
	return nil
}

func (s *DefaultService) coordinatedRollback(ctx context.Context, trx TRX, space string, segment string) {

	rollback := &Rollback{TRX: trx, Space: space, Segment: segment}
	if err := s.supervisor.Rollback(ctx, &Rollback{TRX: trx, Space: space, Segment: segment}); err != nil {
		slog.Error("Error rolling back transaction", "error", err)
	}
	if err := s.Rollback(ctx, rollback); err != nil {
		slog.Error("Error rolling back transaction", "error", err)
	}
}

//
// Helpers
//

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
			return nil, nil // Not an error; transaction doesn't exist.
		}
		slog.Error("failed to get transaction", "key", transactionKey, "error", err)
		return nil, NewTransientError("failed to get transaction")
	}
	if closer != nil {
		defer closer.Close()
	}

	if len(data) == 0 {
		slog.Error("transaction data is empty", "key", transactionKey)
		return nil, NewTransientError("transaction is empty")
	}

	transaction := &Transaction{}
	if err := DecodeTransactionSnappy(data, transaction); err != nil {
		return nil, NewTransientError("failed to unmarshal transaction")
	}

	return transaction, nil
}

func (s *DefaultService) getLastEntry(ctx context.Context, space string, segment string) (*Entry, error) {
	lower, upper := lexkey.EncodeFirst(DATA, SEGMENTS, space, segment), lexkey.EncodeLast(DATA, SEGMENTS, space, segment)

	opts := &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	}

	iter, err := s.db.NewIterWithContext(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	if !iter.SeekLT(upper) {
		err := iter.Error()
		if err == nil || errors.Is(err, pebble.ErrNotFound) {
			return &Entry{}, nil
		}

		return nil, pebble.ErrNotFound
	}
	entry := &Entry{}
	if err := DecodeEntrySnappy(iter.Value(), entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *DefaultService) getLastKey(ctx context.Context, lower, upper lexkey.LexKey) (lexkey.LexKey, error) {
	opts := &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	}
	iter, err := s.db.NewIterWithContext(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	if iter.SeekLT(upper); !iter.Valid() {
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
	created, err := createIfNotExists(s.db, s.cache, lexkey.Encode(INVENTORY, SEGMENTS, space, segment), segment)
	if err != nil {
		return fmt.Errorf("failed to update segment inventory: %w", err)
	}
	if created {
		_, err := createIfNotExists(s.db, s.cache, lexkey.Encode(INVENTORY, SPACES, space), space)
		if err != nil {
			return fmt.Errorf("failed to update space inventory: %w", err)
		}
		return nil
	}
	return nil
}

func createIfNotExists(db *pebble.DB, cache *ExpiringCache, key lexkey.LexKey, value string) (bool, error) {
	keyHex := key.ToHexString()

	if _, ok := cache.Get(keyHex); ok {
		return false, nil
	}
	_, closer, err := db.Get(key)
	if closer != nil {
		defer closer.Close()
	}
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			if err := db.Set(key, []byte(value), pebble.NoSync); err != nil {
				return false, err
			}
			cache.Set(keyHex, struct{}{})
			return true, nil
		} else {
			return false, err
		}
	}
	return false, nil
}

func setEntry(batch *pebble.Batch, entry *Entry) error {
	dataSpaceKey := entry.GetSpaceOffset()
	dataSegmentKey := entry.GetSegmentOffset()
	data, err := EncodeEntrySnappy(entry)
	if err != nil {
		slog.Error("Error marshalling entry", "error", err)
		return NewTransientError("failed to marshal entry")
	}
	if err := batch.Set(dataSpaceKey, data, pebble.NoSync); err != nil {
		slog.Error("Error setting entry in space", "error", err)
		return NewTransientError("failed to set entry in space")
	}
	if err := batch.Set(dataSegmentKey, data, pebble.NoSync); err != nil {
		slog.Error("Error setting entry in segment", "error", err)
		return NewTransientError("failed to set entry in segment")
	}
	return nil
}

func elapsedSeconds(startMillis, endMillis int64) float64 {
	startTime := time.UnixMilli(startMillis)
	endTime := time.UnixMilli(endMillis)
	return endTime.Sub(startTime).Seconds()
}
