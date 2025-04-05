package server

import (
	"context"
	"log/slog"
	"sync"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/broker"
)

func RegisterHandler[T broker.Routeable](o *DefaultObserver, arg T, handler func(msg T)) error {
	sub, err := o.Bus.Subscribe(o.Context, arg.GetRoute(), func(msg broker.Routeable) {
		select {
		case <-o.Context.Done():
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
	sub, err := o.Bus.SubscribeToStream(o.Context, arg.GetRoute(), func(msg broker.Routeable, stream broker.BidiStream) {
		handler(msg.(T), stream)
	})

	if err != nil {
		return err
	}
	o.subs = append(o.subs, sub)
	return nil
}

// NewQuorumObserver initializes the observer with event bus
func NewDefaultObserver(bus broker.Bus, service Service) (*DefaultObserver, error) {

	ctx, cancel := context.WithCancel(context.Background())

	o := &DefaultObserver{
		Context: ctx,
		Cancel:  cancel,
		Bus:     bus,
		service: service,
	}

	if err := o.registerHandlers(); err != nil {
		return nil, err
	}

	return o, nil
}

// DefaultObserver manages quorum state and transaction lifecycle
type DefaultObserver struct {
	Context  context.Context
	Cancel   context.CancelFunc
	Bus      broker.Bus
	service  Service
	subs     []broker.Subscription
	disposed sync.Once
}

func (o *DefaultObserver) registerHandlers() error {
	// Register stream handlers
	streamHandlers := []func() error{
		func() error { return RegisterStreamHandler(o, &Peek{}, o.HandlePeek) },
		func() error { return RegisterStreamHandler(o, &GetSpaces{}, o.HandleGetSpaces) },
		func() error { return RegisterStreamHandler(o, &ConsumeSpace{}, o.HandleConsumeSpace) },
		func() error { return RegisterStreamHandler(o, &GetSegments{}, o.HandleGetSegments) },
		func() error { return RegisterStreamHandler(o, &ConsumeSegment{}, o.HandleConsumeSegment) },
		func() error { return RegisterStreamHandler(o, &Produce{}, o.HandleProduce) },
		func() error { return RegisterStreamHandler(o, &Consume{}, o.HandleConsume) },
	}

	// Execute all stream handlers
	for _, register := range streamHandlers {
		if err := register(); err != nil {
			return err
		}
	}

	return nil
}

func (o *DefaultObserver) HandleProduce(args *Produce, stream broker.BidiStream) {
	entries := broker.NewStreamEnumerator[*Record](stream)
	results := o.service.Produce(o.Context, args, entries)
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

func (o *DefaultObserver) HandleConsume(args *Consume, stream broker.BidiStream) {
	enumerator := o.service.Consume(o.Context, args)
	StreamEntries(stream, enumerator)
}

func (o *DefaultObserver) HandleGetSpaces(args *GetSpaces, stream broker.BidiStream) {
	enumerator := o.service.GetSpaces(o.Context)
	StreamNames(stream, enumerator)
}

func (o *DefaultObserver) HandlePeek(args *Peek, stream broker.BidiStream) {

	entry, err := o.service.Peek(o.Context, args.Space, args.Segment)
	if err != nil {
		stream.Close(err)
	}
	if err := stream.Encode(entry); err != nil {
		stream.Close(err)
		return
	}
	stream.Close(nil)
}

func (o *DefaultObserver) HandleConsumeSpace(args *ConsumeSpace, stream broker.BidiStream) {
	enumerator := o.service.ConsumeSpace(o.Context, args)
	StreamEntries(stream, enumerator)
}

func (o *DefaultObserver) HandleConsumeSegment(args *ConsumeSegment, stream broker.BidiStream) {
	enumerator := o.service.ConsumeSegment(o.Context, args)
	StreamEntries(stream, enumerator)
}

func (o *DefaultObserver) HandleGetSegments(args *GetSegments, stream broker.BidiStream) {
	enumerator := o.service.GetSegments(o.Context, args.Space)
	StreamNames(stream, enumerator)
}

func StreamNames(stream broker.BidiStream, enumerator enumerators.Enumerator[string]) {
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

func StreamEntries(stream broker.BidiStream, enumerator enumerators.Enumerator[*Entry]) {
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

func (o *DefaultObserver) Close() {
	o.disposed.Do(func() {
		o.Cancel()
		for _, sub := range o.subs {
			sub.Unsubscribe()
		}
		o.subs = nil
		<-o.Context.Done()
	})
}
