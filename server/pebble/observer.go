package pebble

import (
	"bytes"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/broker"
	"github.com/fgrzl/streams/server"
	"github.com/fgrzl/timestamp"
	"github.com/google/uuid"
)

// NewQuorumObserver initializes the observer with event bus
func NewPebbleObserver(bus broker.Bus, service Service, quorum Quorum) (*PebbleObserver, error) {

	defaultObserver, err := server.NewDefaultObserver(bus, service)
	if err != nil {
		return nil, err
	}

	o := &PebbleObserver{
		DefaultObserver: defaultObserver,
		quorum:          quorum,
		service:         service,
	}

	if err := o.registerHandlers(); err != nil {
		return nil, err
	}

	go o.monitor()
	return o, nil
}

// PebbleObserver manages quorum state and transaction lifecycle
type PebbleObserver struct {
	*server.DefaultObserver

	quorum   Quorum
	service  Service
	subs     []broker.Subscription
	disposed sync.Once
}

func (o *PebbleObserver) registerHandlers() error {
	// Register standard handlers
	standardHandlers := []func() error{
		func() error { return server.RegisterHandler(o.DefaultObserver, &NodeHeartbeat{}, o.HandleHeartbeat) },
		func() error { return server.RegisterHandler(o.DefaultObserver, &NodeShutdown{}, o.HandleNodeShutdown) },
		func() error {
			return server.RegisterHandler(o.DefaultObserver, &ConfirmSpaceOffset{}, o.HandleConfirmSpaceOffset)
		},
		func() error {
			return server.RegisterHandler(o.DefaultObserver, &ConfirmSegmentOffset{}, o.HandleConfirmSegmentOffset)
		},
		func() error { return server.RegisterHandler(o.DefaultObserver, &Transaction{}, o.HandleWrite) },
		func() error { return server.RegisterHandler(o.DefaultObserver, &Commit{}, o.HandleCommit) },
		func() error { return server.RegisterHandler(o.DefaultObserver, &Rollback{}, o.HandleRollback) },
	}

	// Register stream handlers
	streamHandlers := []func() error{
		func() error {
			return server.RegisterStreamHandler(o.DefaultObserver, &GetStatus{}, o.HandleGetClusterStatus)
		},
		func() error {
			return server.RegisterStreamHandler(o.DefaultObserver, &Synchronize{}, o.HandleSyncrhronize)
		},
		func() error {
			return server.RegisterStreamHandler(o.DefaultObserver, &EnumerateSpace{}, o.HandleEnumerateSpace)
		},
		func() error {
			return server.RegisterStreamHandler(o.DefaultObserver, &EnumerateSegment{}, o.HandleEnumerateSegment)
		},
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

func (o *PebbleObserver) HandleHeartbeat(args *NodeHeartbeat) {
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

func (o *PebbleObserver) HandleNodeShutdown(args *NodeShutdown) {
	if args.Node == o.quorum.GetNode() {
		return
	}
	o.quorum.SetOffline(args.Node)
}

//
// Read Handlers
//

func (o *PebbleObserver) HandleConfirmSpaceOffset(args *ConfirmSpaceOffset) {

	if args.Node == o.quorum.GetNode() {
		return
	}

	offset, err := o.service.GetSpaceOffset(o.Context, args.Space)
	if err != nil {
		slog.Warn("ConfirmSpaceOffset could not confirm offset retrieval", "error", err)
		return
	}

	result := bytes.Compare(offset, args.Offset)

	if result == 0 {
		// Offsets match, send ACK
		o.Bus.Notify(o.Context, args.ToACK(o.quorum.GetNode()))
		return
	}

	if result < 0 {
		// Node is behind, it cannot confirm, so just log and return
		slog.Warn("ConfirmSpaceOffset: local offset is behind, cannot confirm", "local_offset", offset, "received_offset", args.Offset)
		o.service.SynchronizeSpace(o.Context, args.Space)
		return
	}

	o.Bus.Notify(o.Context, args.ToNACK(o.quorum.GetNode()))
}

func (o *PebbleObserver) HandleConfirmSegmentOffset(args *ConfirmSegmentOffset) {
	if args.Node == o.quorum.GetNode() {
		return
	}
	space, segment := args.Space, args.Segment
	for range 3 {

		offset, err := o.service.GetSegmentOffset(o.Context, space, segment)
		if err != nil {
			slog.Warn("ConfirmSegmentOffset could not confirm offset retrieval", "error", err)
			return
		}

		result := bytes.Compare(offset, args.Offset)

		if result == 0 {
			// Offsets match, send ACK
			o.Bus.Notify(o.Context, args.ToACK(o.quorum.GetNode()))
			return
		}

		if result < 0 {
			// Node is behind, it cannot confirm, so just log and return
			slog.Warn("ConfirmSegmentOffset: local offset is behind, cannot confirm", "local_offset", offset, "received_offset", args.Offset)
			o.service.SynchronizeSegment(o.Context, space, segment)
			break
		}
		o.Bus.Notify(o.Context, args.ToNACK(o.quorum.GetNode()))
	}
}

//
// Transaction Handlers
//

func (o *PebbleObserver) HandleWrite(args *Transaction) {
	if args.TRX.Node == o.quorum.GetNode() {
		return
	}

	err := o.service.Write(o.Context, args)

	if err == nil {
		o.Bus.Notify(o.Context, args.TRX.ToACK(o.quorum.GetNode()))
		return
	}

	// Check if the error is a known type
	var qErr *server.StreamsError
	if errors.As(err, &qErr) {
		switch qErr.Code {
		case server.ErrCodeTransient:
			slog.Warn("transient write operation failure", "error", err)
			return // Do nothing
		case server.ErrCodePermanent:
			slog.Warn("permanent write operation failure", "error", err)
			o.Bus.Notify(o.Context, args.TRX.ToNACK(o.quorum.GetNode()))
			return
		}
	}

	// Default: Log and ignore unknown errors
	slog.Warn("Write operation failed with unknown error. Not confirming.", "error", err)
}

func (o *PebbleObserver) HandleCommit(args *Commit) {
	if args.TRX.Node == o.quorum.GetNode() {
		return
	}

	err := o.service.Commit(o.Context, args)

	if err == nil {
		o.Bus.Notify(o.Context, args.TRX.ToACK(o.quorum.GetNode()))
		return
	}

	// Check if the error is a known type
	var qErr *server.StreamsError
	if errors.As(err, &qErr) {
		switch qErr.Code {
		case server.ErrCodeTransient:
			slog.Warn("transient commit operation failure", "error", err)
			return // Do nothing
		case server.ErrCodePermanent:
			slog.Warn("permanent commit operation failure", "error", err)
			o.Bus.Notify(o.Context, args.TRX.ToNACK(o.quorum.GetNode()))
			return
		}
	}

	// Default: Log and ignore unknown errors
	slog.Warn("Commit operation failed with unknown error. Not confirming.", "error", err)
}

func (o *PebbleObserver) HandleRollback(args *Rollback) {
	if args.TRX.Node == o.quorum.GetNode() {
		return
	}
	err := o.service.Rollback(o.Context, args)

	if err == nil {
		o.Bus.Notify(o.Context, args.TRX.ToACK(o.quorum.GetNode()))
		return
	}

	// Check if the error is a known type
	var qErr *server.StreamsError
	if errors.As(err, &qErr) {
		switch qErr.Code {
		case server.ErrCodeTransient:
			slog.Warn("transient rollback operation failure", "error", err)
			return // Do nothing
		case server.ErrCodePermanent:
			slog.Warn("permanent rollback operation failure", "error", err)
			o.Bus.Notify(o.Context, args.TRX.ToNACK(o.quorum.GetNode()))
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
func (o *PebbleObserver) HandleGetClusterStatus(args *GetStatus, stream broker.BidiStream) {

	status := o.service.GetClusterStatus()
	if err := stream.Encode(status); err != nil {
		stream.Close(err)
		return
	}
	stream.Close(nil)
}

func (o *PebbleObserver) HandleSyncrhronize(args *Synchronize, stream broker.BidiStream) {
	enumerator := enumerators.FlatMap(
		o.service.GetSpaces(o.Context),
		func(space string) enumerators.Enumerator[*Entry] {
			offset := args.OffsetsBySpace[space]
			return o.service.EnumerateSpace(o.Context, &EnumerateSpace{Space: space, Offset: offset})
		})

	server.StreamEntries(stream, enumerator)
}

// HandleEnumerateSpace implements Observer.
func (o *PebbleObserver) HandleEnumerateSpace(args *EnumerateSpace, stream broker.BidiStream) {
	enumerator := o.service.EnumerateSpace(o.Context, args)
	server.StreamEntries(stream, enumerator)
}

// HandleEnumerateSegment implements Observer.
func (o *PebbleObserver) HandleEnumerateSegment(args *EnumerateSegment, stream broker.BidiStream) {
	enumerator := o.service.EnumerateSegment(o.Context, args)
	server.StreamEntries(stream, enumerator)
}

func (o *PebbleObserver) heartbeat() {
	o.Bus.Notify(o.Context, &NodeHeartbeat{
		Node:  o.quorum.GetNode(),
		Nodes: o.quorum.GetNodes(),
	})
}

func (o *PebbleObserver) shutdown() {
	o.Bus.Notify(o.Context, &NodeShutdown{Node: o.quorum.GetNode()})
}

//
// Close
//

func (o *PebbleObserver) Close() {
	o.disposed.Do(func() {
		o.Cancel()
		for _, sub := range o.subs {
			sub.Unsubscribe()
		}
		o.subs = nil
		<-o.Context.Done()
	})
}

func (o *PebbleObserver) monitor() {
	runtime.Gosched()

	// Send initial heartbeat
	o.heartbeat()
	interval := o.quorum.GetTTL() / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-o.Context.Done():
			o.shutdown()
			return
		case <-ticker.C:
			o.heartbeat()
		}
	}
}
