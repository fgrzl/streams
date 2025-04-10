package streams

import (
	"context"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/broker"
	"github.com/fgrzl/streams/server"
)

type Entry = server.Entry
type Record = server.Record
type SegmentStatus = server.SegmentStatus
type ConsumeSpace = server.ConsumeSpace
type Consume = server.Consume
type ConsumeSegment = server.ConsumeSegment
type Produce = server.Produce
type GetSpaces = server.GetSpaces
type GetSegments = server.GetSegments
type Peek = server.Peek
type GetStatus = server.GetStatus
type ClusterStatus = server.ClusterStatus

type Client interface {

	// Get Node Count
	GetClusterStatus(ctx context.Context) (*ClusterStatus, error)

	// Get all the spaces
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]

	// Get all segments in a space.
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]

	// Get the last entry in a stream.
	Peek(ctx context.Context, space, segment string) (*Entry, error)

	// Consume the space. This will interleave all of the streams in the space.
	Consume(ctx context.Context, args *Consume) enumerators.Enumerator[*Entry]

	// Consume the space. This will interleave all of the streams in the space.
	ConsumeSpace(ctx context.Context, args *ConsumeSpace) enumerators.Enumerator[*Entry]

	// Consume a segment.
	ConsumeSegment(ctx context.Context, args *ConsumeSegment) enumerators.Enumerator[*Entry]

	// Produce stream entries.
	Produce(ctx context.Context, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus]

	// Publish one off events.
	Publish(ctx context.Context, space, segment string, payload []byte, metadata map[string]string) error

	// Subscribe to a space.
	SubcribeToSpace(ctx context.Context, space string, handler func(*SegmentStatus)) (broker.Subscription, error)

	// Subscribe to a segment.
	SubcribeToSegment(ctx context.Context, space, segment string, handler func(*SegmentStatus)) (broker.Subscription, error)
}

func NewClient(bus broker.Bus) Client {
	return &DefaultClient{
		bus: bus,
	}
}

type DefaultClient struct {
	bus broker.Bus
}

func (d *DefaultClient) GetClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	stream, err := d.bus.CallStream(ctx, &server.GetStatus{})
	if err != nil {
		return nil, err
	}
	entry := &ClusterStatus{}
	if err := stream.Decode(&entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (d *DefaultClient) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	stream, err := d.bus.CallStream(ctx, &GetSpaces{})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return broker.NewStreamEnumerator[string](stream)
}

func (d *DefaultClient) ConsumeSpace(ctx context.Context, args *ConsumeSpace) enumerators.Enumerator[*Entry] {
	stream, err := d.bus.CallStream(ctx, args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return broker.NewStreamEnumerator[*Entry](stream)
}

func (d *DefaultClient) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	stream, err := d.bus.CallStream(ctx, &GetSegments{Space: space})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return broker.NewStreamEnumerator[string](stream)
}

func (d *DefaultClient) ConsumeSegment(ctx context.Context, args *ConsumeSegment) enumerators.Enumerator[*Entry] {
	stream, err := d.bus.CallStream(ctx, args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return broker.NewStreamEnumerator[*Entry](stream)
}

func (d *DefaultClient) Peek(ctx context.Context, space string, segment string) (*Entry, error) {
	stream, err := d.bus.CallStream(ctx, &Peek{Space: space, Segment: segment})
	if err != nil {
		return nil, err
	}
	entry := &Entry{}
	if err := stream.Decode(&entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (d *DefaultClient) Produce(ctx context.Context, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus] {
	stream, err := d.bus.CallStream(ctx, &Produce{Space: space, Segment: segment})
	if err != nil {
		return enumerators.Error[*SegmentStatus](err)
	}

	go func(s broker.BidiStream, entries enumerators.Enumerator[*Record]) {
		defer entries.Dispose()
		for entries.MoveNext() {
			entry, err := entries.Current()
			if err != nil {
				s.CloseSend(err)
			}
			if err := s.Encode(entry); err != nil {
				s.CloseSend(err)
			}
		}
		s.CloseSend(nil)
	}(stream, entries)

	return broker.NewStreamEnumerator[*SegmentStatus](stream)
}

func (d *DefaultClient) Publish(ctx context.Context, space, segment string, payload []byte, metadata map[string]string) error {
	peek, err := d.Peek(ctx, space, segment)
	if err != nil {
		return err
	}

	stream, err := d.bus.CallStream(ctx, &Produce{Space: space, Segment: segment})
	if err != nil {
		return err
	}

	record := &Record{
		Sequence: peek.Sequence + 1,
		Payload:  payload,
		Metadata: metadata,
	}
	defer stream.CloseSend(nil)
	if err := stream.Encode(record); err != nil {
		stream.CloseSend(err)
	}
	enumerator := broker.NewStreamEnumerator[*SegmentStatus](stream)
	return enumerators.Consume(enumerator)
}

func (d *DefaultClient) Consume(ctx context.Context, args *Consume) enumerators.Enumerator[*Entry] {
	stream, err := d.bus.CallStream(ctx, args)
	if err != nil {
		return enumerators.Error[*Entry](err)
	}
	return broker.NewStreamEnumerator[*Entry](stream)
}

func (d *DefaultClient) SubcribeToSpace(ctx context.Context, space string, handler func(*SegmentStatus)) (broker.Subscription, error) {
	status := &SegmentStatus{Space: space}
	route := status.GetRoute()
	return d.bus.Subscribe(ctx, route, func(r broker.Routeable) {
		if status, ok := r.(*SegmentStatus); ok {
			handler(status)
		}
	})
}

func (d *DefaultClient) SubcribeToSegment(ctx context.Context, space, segment string, handler func(*SegmentStatus)) (broker.Subscription, error) {
	status := &SegmentStatus{Space: space, Segment: segment}
	route := status.GetRoute()
	return d.bus.Subscribe(ctx, route, func(r broker.Routeable) {
		if status, ok := r.(*SegmentStatus); ok {
			handler(status)
		}
	})
}
