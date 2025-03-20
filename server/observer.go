package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/broker"
	"github.com/fgrzl/timestamp"
	"github.com/google/uuid"
)

// Observer defines the interface observing events across nodes.
type Observer interface {

	// Quorum management
	HandleHeartbeat(*NodeHeartbeat)
	HandleNodeShutdown(*NodeShutdown)

	// Read lifecycle
	HandleCheckSpaceOffset(*CheckSpaceOffset)
	HandleCheckSegmentOffset(*CheckSegmentOffset)

	// Transaction lifecycle
	HandleWrite(*Transaction)
	HandleCommit(*Commit)
	HandleRollback(*Rollback)

	// Streaming data
	HandleGetClusterStatus(*GetStatus, broker.BidiStream)
	HandlePeek(*Peek, broker.BidiStream)
	HandleGetSpaces(*GetSpaces, broker.BidiStream)
	HandleConsumeSpace(*ConsumeSpace, broker.BidiStream)
	HandleEnumerateSpace(*EnumerateSpace, broker.BidiStream)
	HandleGetSegments(*GetSegments, broker.BidiStream)
	HandleConsumeSegment(*ConsumeSegment, broker.BidiStream)
	HandleEnumerateSegment(*EnumerateSegment, broker.BidiStream)
	HandleSyncrhronize(*Synchronize, broker.BidiStream)
	HandleProduce(*Produce, broker.BidiStream)

	// Close the observer
	Close()
}

func RegisterHandler[T broker.Routeable](o *DefaultObserver, arg T, handler func(msg T)) error {
	sub, err := o.bus.Subscribe(arg.GetRoute(), func(msg broker.Routeable) {
		select {
		case <-o.ctx.Done():
			// Context is done, don't send to the channel
			return
		default:
			// Ensure msg is of type T before type assertion
			tMsg, ok := msg.(T)
			if !ok {
				slog.Warn("Received message of unexpected type")
				return
			}
			handler(tMsg)

		}
	})

	if err != nil {
		return err
	}
	o.subs = append(o.subs, sub)
	return nil
}

func RegisterStreamHandler[T broker.Routeable](o *DefaultObserver, arg T, handler func(msg T, stream broker.BidiStream)) error {
	sub, err := o.bus.SubscribeToStream(arg.GetRoute(), func(msg broker.Routeable, stream broker.BidiStream) {
		handler(msg.(T), stream)
	})

	if err != nil {
		return err
	}
	o.subs = append(o.subs, sub)
	return nil
}

// NewQuorumObserver initializes the observer with event bus
func NewDefaultObserver(bus broker.Bus, service Service, quorum Quorum) (Observer, error) {

	ctx, cancel := context.WithCancel(context.Background())

	o := &DefaultObserver{
		ctx:     ctx,
		cancel:  cancel,
		quorum:  quorum,
		bus:     bus,
		service: service,
	}

	if err := o.registerHandlers(); err != nil {
		return nil, err
	}

	go o.monitor()
	return o, nil
}

// DefaultObserver manages quorum state and transaction lifecycle
type DefaultObserver struct {
	ctx    context.Context
	cancel context.CancelFunc
	quorum Quorum

	bus      broker.Bus
	service  Service
	subs     []broker.Subscription
	disposed sync.Once
}

func (o *DefaultObserver) registerHandlers() error {
	// Register standard handlers
	standardHandlers := []func() error{
		func() error { return RegisterHandler(o, &NodeHeartbeat{}, o.HandleHeartbeat) },
		func() error { return RegisterHandler(o, &NodeShutdown{}, o.HandleNodeShutdown) },
		func() error { return RegisterHandler(o, &CheckSpaceOffset{}, o.HandleCheckSpaceOffset) },
		func() error { return RegisterHandler(o, &CheckSegmentOffset{}, o.HandleCheckSegmentOffset) },
		func() error { return RegisterHandler(o, &Transaction{}, o.HandleWrite) },
		func() error { return RegisterHandler(o, &Commit{}, o.HandleCommit) },
		func() error { return RegisterHandler(o, &Rollback{}, o.HandleRollback) },
	}

	// Register stream handlers
	streamHandlers := []func() error{
		func() error { return RegisterStreamHandler(o, &GetStatus{}, o.HandleGetClusterStatus) },
		func() error { return RegisterStreamHandler(o, &Synchronize{}, o.HandleSyncrhronize) },
		func() error { return RegisterStreamHandler(o, &Peek{}, o.HandlePeek) },
		func() error { return RegisterStreamHandler(o, &GetSpaces{}, o.HandleGetSpaces) },
		func() error { return RegisterStreamHandler(o, &ConsumeSpace{}, o.HandleConsumeSpace) },
		func() error { return RegisterStreamHandler(o, &EnumerateSpace{}, o.HandleEnumerateSpace) },
		func() error { return RegisterStreamHandler(o, &GetSegments{}, o.HandleGetSegments) },
		func() error { return RegisterStreamHandler(o, &ConsumeSegment{}, o.HandleConsumeSegment) },
		func() error { return RegisterStreamHandler(o, &EnumerateSegment{}, o.HandleEnumerateSegment) },
		func() error { return RegisterStreamHandler(o, &Produce{}, o.HandleProduce) },
	}

	// Execute all standard handlers
	for _, register := range standardHandlers {
		if err := register(); err != nil {
			return err
		}
	}

	// Execute all stream handlers
	for _, register := range streamHandlers {
		if err := register(); err != nil {
			return err
		}
	}

	return nil
}

func (o *DefaultObserver) HandleHeartbeat(args *NodeHeartbeat) {
	if args.Node == o.quorum.GetNode() {
		return
	}
	slog.Debug("Heartbeat", "node", o.quorum.GetNode(), "from_node", args.Node)

	// Make a defensive copy of the Nodes map
	nodes := make(map[uuid.UUID]int64)
	for k, v := range args.Nodes {
		nodes[k] = v
	}
	nodes[args.Node] = timestamp.GetTimestamp()

	var count int
	for node, ts := range nodes {
		if o.quorum.SetOnline(node, ts) {
			count += 1
		}
	}
	if count > 0 {
		o.heartbeat()
	}
}

func (o *DefaultObserver) HandleNodeShutdown(args *NodeShutdown) {
	if args.Node == o.quorum.GetNode() {
		return
	}
	o.quorum.SetOffline(args.Node)
}

//
// Read Handlers
//

func (o *DefaultObserver) HandleCheckSpaceOffset(args *CheckSpaceOffset) {

	if args.Node == o.quorum.GetNode() {
		return
	}

	offset, err := o.service.GetSpaceOffset(o.ctx, args.Space)
	if err != nil {
		slog.Warn("CheckSpaceOffset could not confirm offset retrieval", "error", err)
		return
	}

	result := bytes.Compare(offset, args.Offset)

	if result == 0 {
		// Offsets match, send ACK
		o.bus.Notify(args.ToACK(o.quorum.GetNode()))
		return
	}

	if result < 0 {
		// Node is behind, it cannot confirm, so just log and return
		slog.Warn("CheckSpaceOffset: local offset is behind, cannot confirm", "local_offset", offset, "received_offset", args.Offset)
		o.service.SynchronizeSpace(o.ctx, args.Space)
		return
	}

	o.bus.Notify(args.ToNACK(o.quorum.GetNode()))
}

func (o *DefaultObserver) HandleCheckSegmentOffset(args *CheckSegmentOffset) {
	if args.Node == o.quorum.GetNode() {
		return
	}
	space, segment := args.Space, args.Segment
	for range 3 {

		offset, err := o.service.GetSegmentOffset(o.ctx, space, segment)
		if err != nil {
			slog.Warn("CheckSegmentOffset could not confirm offset retrieval", "error", err)
			return
		}

		result := bytes.Compare(offset, args.Offset)

		if result == 0 {
			// Offsets match, send ACK
			o.bus.Notify(args.ToACK(o.quorum.GetNode()))
			return
		}

		if result < 0 {
			// Node is behind, it cannot confirm, so just log and return
			slog.Warn("CheckSegmentOffset: local offset is behind, cannot confirm", "local_offset", offset, "received_offset", args.Offset)
			o.service.SynchronizeSegment(o.ctx, space, segment)
			break
		}
		o.bus.Notify(args.ToNACK(o.quorum.GetNode()))
	}
}

//
// Transaction Handlers
//

func (o *DefaultObserver) HandleWrite(args *Transaction) {
	if args.TRX.Node == o.quorum.GetNode() {
		return
	}

	err := o.service.Write(o.ctx, args)

	if err == nil {
		o.bus.Notify(args.TRX.ToACK(o.quorum.GetNode()))
		return
	}

	// Check if the error is a known type
	var qErr *StreamsError
	if errors.As(err, &qErr) {
		switch qErr.Code {
		case ErrCodeTransient:
			slog.Warn("transient write operation failure", "error", err)
			return // Do nothing
		case ErrCodePermanent:
			slog.Warn("permanent write operation failure", "error", err)
			o.bus.Notify(args.TRX.ToNACK(o.quorum.GetNode()))
			return
		}
	}

	// Default: Log and ignore unknown errors
	slog.Warn("Write operation failed with unknown error. Not confirming.", "error", err)
}

func (o *DefaultObserver) HandleCommit(args *Commit) {
	if args.TRX.Node == o.quorum.GetNode() {
		return
	}

	err := o.service.Commit(o.ctx, args)

	if err == nil {
		o.bus.Notify(args.TRX.ToACK(o.quorum.GetNode()))
		return
	}

	// Check if the error is a known type
	var qErr *StreamsError
	if errors.As(err, &qErr) {
		switch qErr.Code {
		case ErrCodeTransient:
			slog.Warn("transient commit operation failure", "error", err)
			return // Do nothing
		case ErrCodePermanent:
			slog.Warn("permanent commit operation failure", "error", err)
			o.bus.Notify(args.TRX.ToNACK(o.quorum.GetNode()))
			return
		}
	}

	// Default: Log and ignore unknown errors
	slog.Warn("Commit operation failed with unknown error. Not confirming.", "error", err)
}

func (o *DefaultObserver) HandleRollback(args *Rollback) {
	if args.TRX.Node == o.quorum.GetNode() {
		return
	}
	err := o.service.Rollback(o.ctx, args)

	if err == nil {
		o.bus.Notify(args.TRX.ToACK(o.quorum.GetNode()))
		return
	}

	// Check if the error is a known type
	var qErr *StreamsError
	if errors.As(err, &qErr) {
		switch qErr.Code {
		case ErrCodeTransient:
			slog.Warn("transient rollback operation failure", "error", err)
			return // Do nothing
		case ErrCodePermanent:
			slog.Warn("permanent rollback operation failure", "error", err)
			o.bus.Notify(args.TRX.ToNACK(o.quorum.GetNode()))
			return
		}
	}

	// Default: Log and ignore unknown errors
	slog.Warn("Rollback operation failed with unknown error. Not confirming.", "error", err)
}

//
// Streaming Handlers
//

// HandleEnumerateSpace implements Observer.
func (o *DefaultObserver) HandleGetClusterStatus(args *GetStatus, stream broker.BidiStream) {

	status := o.service.GetClusterStatus()
	if err := stream.Encode(status); err != nil {
		stream.Close(err)
		return
	}
	stream.Close(nil)
}

func (o *DefaultObserver) HandleSyncrhronize(args *Synchronize, stream broker.BidiStream) {
	enumerator := enumerators.FlatMap(
		o.service.GetSpaces(o.ctx),
		func(space string) enumerators.Enumerator[*Entry] {
			offset := args.OffsetsBySpace[space]
			return o.service.EnumerateSpace(o.ctx, &EnumerateSpace{Space: space, Offset: offset})
		})

	streamEntries(stream, enumerator)
}

func (o *DefaultObserver) HandleProduce(args *Produce, stream broker.BidiStream) {
	entries := broker.NewStreamEnumerator[*Record](stream)
	results := o.service.Produce(o.ctx, args, entries)
	defer results.Dispose()
	for results.MoveNext() {
		result, err := results.Current()
		if err != nil {
			stream.CloseSend(err)
			return
		}
		if err := stream.Encode(result); err != nil {
			stream.CloseSend(err)
			return
		}
	}
	stream.CloseSend(nil)
}

func (o *DefaultObserver) HandleGetSpaces(args *GetSpaces, stream broker.BidiStream) {
	enumerator := o.service.GetSpaces(o.ctx)
	streamNames(stream, enumerator)
}

// HandleEnumerateSpace implements Observer.
func (o *DefaultObserver) HandlePeek(args *Peek, stream broker.BidiStream) {

	entry, err := o.service.Peek(o.ctx, args.Space, args.Segment)
	if err != nil {
		stream.Close(err)
	}
	if err := stream.Encode(entry); err != nil {
		stream.Close(err)
		return
	}
	stream.Close(nil)
}

// HandleEnumerateSpace implements Observer.
func (o *DefaultObserver) HandleConsumeSpace(args *ConsumeSpace, stream broker.BidiStream) {
	enumerator := o.service.ConsumeSpace(o.ctx, args)
	streamEntries(stream, enumerator)
}

// HandleEnumerateSpace implements Observer.
func (o *DefaultObserver) HandleEnumerateSpace(args *EnumerateSpace, stream broker.BidiStream) {
	enumerator := o.service.EnumerateSpace(o.ctx, args)
	streamEntries(stream, enumerator)
}

// HandleEnumerateSegment implements Observer.
func (o *DefaultObserver) HandleConsumeSegment(args *ConsumeSegment, stream broker.BidiStream) {
	enumerator := o.service.ConsumeSegment(o.ctx, args)
	streamEntries(stream, enumerator)
}

// HandleEnumerateSegment implements Observer.
func (o *DefaultObserver) HandleEnumerateSegment(args *EnumerateSegment, stream broker.BidiStream) {
	enumerator := o.service.EnumerateSegment(o.ctx, args)
	streamEntries(stream, enumerator)
}

// HandleGetSegments implements Observer.
func (o *DefaultObserver) HandleGetSegments(args *GetSegments, stream broker.BidiStream) {
	enumerator := o.service.GetSegments(o.ctx, args.Space)
	streamNames(stream, enumerator)
}

func (o *DefaultObserver) heartbeat() {
	o.bus.Notify(&NodeHeartbeat{
		Node:  o.quorum.GetNode(),
		Nodes: o.quorum.GetNodes(),
	})
}

func (o *DefaultObserver) shutdown() {
	o.bus.Notify(&NodeShutdown{Node: o.quorum.GetNode()})
}

func streamNames(stream broker.BidiStream, enumerator enumerators.Enumerator[string]) {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		segment, err := enumerator.Current()
		if err != nil {
			stream.CloseSend(err)
			return
		}
		if err := stream.Encode(segment); err != nil {
			stream.CloseSend(err)
			return
		}
	}
	stream.CloseSend(nil)
}

func streamEntries(stream broker.BidiStream, enumerator enumerators.Enumerator[*Entry]) {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		segment, err := enumerator.Current()
		if err != nil {
			stream.CloseSend(err)
			return
		}
		if err := stream.Encode(segment); err != nil {
			stream.CloseSend(err)
			return
		}
	}
	stream.CloseSend(nil)
}

//
// Close
//

func (o *DefaultObserver) Close() {
	o.disposed.Do(func() {
		o.cancel()
		for _, sub := range o.subs {
			sub.Unsubscribe()
		}
		o.subs = nil
		<-o.ctx.Done()
	})
}

func (o *DefaultObserver) monitor() {
	runtime.Gosched()

	// Send initial heartbeat
	o.heartbeat()
	interval := o.quorum.GetTTL() / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-o.ctx.Done():
			o.shutdown()
			return
		case <-ticker.C:
			o.heartbeat()
		}
	}
}
