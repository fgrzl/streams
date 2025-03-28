package pebble

import (
	"context"
	"fmt"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/broker"
	"github.com/fgrzl/streams/server"
	"github.com/google/uuid"
)

// The supervisor is responsible for coordinating the quorom mechanics.
type Supervisor interface {

	// Get the instance id.
	GetNode() uuid.UUID

	// Get active node count
	GetActiveNodeCount() int

	// IsSingleInstance returns true if the node is the only instance.
	IsSingleInstance() bool

	// Get all the spaces
	GetSpaces(context.Context, *GetSpaces) enumerators.Enumerator[string]

	// Consume the space. This will interleave all of the streams in the space.
	EnumerateSpace(context.Context, *EnumerateSpace) enumerators.Enumerator[*Entry]

	// Get all segments in a space.
	GetSegments(context.Context, *GetSegments) enumerators.Enumerator[string]

	// Consume a segment.
	EnumerateSegment(context.Context, *EnumerateSegment) enumerators.Enumerator[*Entry]

	// Write the trx to other nodes.
	Write(context.Context, *Transaction) error

	// Commit the trx.
	Commit(context.Context, *Commit) error

	// Rollback the trx.
	Rollback(context.Context, *Rollback) error

	// Check read quorum.
	ConfirmSpaceOffset(context.Context, *ConfirmSpaceOffset) error

	// Check read quorum.
	ConfirmSegmentOffset(context.Context, *ConfirmSegmentOffset) error

	// Syncronize
	Synchronize(context.Context, *Synchronize) enumerators.Enumerator[*Entry]

	// Notify
	Notify(context.Context, *SegmentStatus) error
}

func NewDefualtSupervisor(bus broker.Bus, quorum Quorum) Supervisor {
	return &DefualtSupervisor{
		bus:    bus,
		quorum: quorum,
	}
}

type DefualtSupervisor struct {
	bus    broker.Bus
	quorum Quorum
}

func (d *DefualtSupervisor) IsSingleInstance() bool {
	return d.quorum.GetNodeCount() == 1
}

func (d *DefualtSupervisor) GetNode() uuid.UUID {
	return d.quorum.GetNode()
}

func (d *DefualtSupervisor) GetActiveNodeCount() int {
	return d.quorum.GetNodeCount()
}

//
// Read Quorum
//

func (d *DefualtSupervisor) ConfirmSpaceOffset(ctx context.Context, args *ConfirmSpaceOffset) error {
	return d.waitForQuorum(ctx, args.ID, args)
}

func (d *DefualtSupervisor) ConfirmSegmentOffset(ctx context.Context, args *ConfirmSegmentOffset) error {
	return d.waitForQuorum(ctx, args.ID, args)
}

//
// Streams
//

func (d *DefualtSupervisor) Notify(ctx context.Context, args *SegmentStatus) error {
	return d.bus.Notify(args)
}

func (d *DefualtSupervisor) Synchronize(ctx context.Context, args *Synchronize) enumerators.Enumerator[*Entry] {
	stream, err := d.bus.CallStream(args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return broker.NewStreamEnumerator[*Entry](stream)
}

func (d *DefualtSupervisor) GetSpaces(ctx context.Context, args *GetSpaces) enumerators.Enumerator[string] {
	if d.IsSingleInstance() {
		return enumerators.Empty[string]()
	}
	stream, err := d.bus.CallStream(args)
	if err != nil {
		return enumerators.Error[string](err)
	}
	return broker.NewStreamEnumerator[string](stream)
}

func (d *DefualtSupervisor) EnumerateSpace(ctx context.Context, args *EnumerateSpace) enumerators.Enumerator[*Entry] {
	if d.IsSingleInstance() {
		return enumerators.Empty[*Entry]()
	}
	stream, err := d.bus.CallStream(args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return broker.NewStreamEnumerator[*Entry](stream)
}

func (d *DefualtSupervisor) GetSegments(ctx context.Context, args *GetSegments) enumerators.Enumerator[string] {
	if d.IsSingleInstance() {
		return enumerators.Empty[string]()
	}
	stream, err := d.bus.CallStream(args)
	if err != nil {
		return enumerators.Error[string](err)
	}
	return broker.NewStreamEnumerator[string](stream)
}

func (d *DefualtSupervisor) EnumerateSegment(ctx context.Context, args *EnumerateSegment) enumerators.Enumerator[*Entry] {
	if d.IsSingleInstance() {
		return enumerators.Empty[*Entry]()
	}
	stream, err := d.bus.CallStream(args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return broker.NewStreamEnumerator[*Entry](stream)
}

//
// Transaction Quorum
//

func (d *DefualtSupervisor) Write(ctx context.Context, args *Transaction) error {
	return d.waitForQuorum(ctx, args.TRX.ID, args)
}

func (d *DefualtSupervisor) Commit(ctx context.Context, args *Commit) error {
	return d.waitForQuorum(ctx, args.TRX.ID, args)
}

func (d *DefualtSupervisor) Rollback(ctx context.Context, args *Rollback) error {
	return d.waitForQuorum(ctx, args.TRX.ID, args)
}

func (d *DefualtSupervisor) Reconcile(ctx context.Context, args *Reconcile) error {
	return d.waitForQuorum(ctx, args.TRX.ID, args)
}

func (d *DefualtSupervisor) waitForQuorum(ctx context.Context, messageID uuid.UUID, args broker.Routeable) error {

	if d.IsSingleInstance() {
		return nil
	}
	ch := make(chan any)

	replyRoute := server.GetReplyRoute(messageID)
	sub, err := d.bus.Subscribe(replyRoute, func(msg broker.Routeable) { ch <- msg })
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer sub.Unsubscribe()

	if err := d.bus.Notify(args); err != nil {
		return fmt.Errorf("failed to notify: %w", err)
	}

	// The write count for quorum less one, for the node itself
	w := d.quorum.GetWriteCount() - 1
	ackCount := 0
	timeout := time.After(29 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled")
		case msg := <-ch:
			switch t := msg.(type) {
			case *ACK:
				if t.Node == d.quorum.GetNode() {
					// ignore my own ack
					// we should prevent this from happening in the first place
					continue
				}
				ackCount++
			case *NACK:
				return fmt.Errorf("nack")
			}
			if ackCount >= w {
				return nil
			}
		case <-timeout:
			return fmt.Errorf("timeout")
		}
	}
}
