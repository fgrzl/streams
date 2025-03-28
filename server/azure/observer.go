package azure

import (
	"context"
	"log/slog"
	"sync"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/broker"
	"github.com/fgrzl/streams/server"
)

type Observer interface {
	HandlePeek(*server.Peek, broker.BidiStream)
	HandleGetSpaces(*server.GetSpaces, broker.BidiStream)
	HandleConsumeSpace(*server.ConsumeSpace, broker.BidiStream)
	HandleGetSegments(*server.GetSegments, broker.BidiStream)
	HandleConsumeSegment(*server.ConsumeSegment, broker.BidiStream)
	HandleProduce(*server.Produce, broker.BidiStream)
	HandleConsume(*server.Consume, broker.BidiStream)

	// Close the observer
	Close()
}

// Observer defines the interface observing events across nodes.

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
func NewDefaultObserver(bus broker.Bus, service Service) (Observer, error) {

	ctx, cancel := context.WithCancel(context.Background())

	o := &DefaultObserver{
		ctx:     ctx,
		cancel:  cancel,
		bus:     bus,
		service: service,
	}

	if err := o.registerHandlers(); err != nil {
		return nil, err
	}

	return o, nil
}

// DefaultObserver manages quorum state and transaction lifecycle
type DefaultObserver struct {
	ctx      context.Context
	cancel   context.CancelFunc
	bus      broker.Bus
	service  Service
	subs     []broker.Subscription
	disposed sync.Once
}

func (o *DefaultObserver) registerHandlers() error {
	// Register stream handlers
	streamHandlers := []func() error{
		func() error { return RegisterStreamHandler(o, &server.Peek{}, o.HandlePeek) },
		func() error { return RegisterStreamHandler(o, &server.GetSpaces{}, o.HandleGetSpaces) },
		func() error { return RegisterStreamHandler(o, &server.ConsumeSpace{}, o.HandleConsumeSpace) },
		func() error { return RegisterStreamHandler(o, &server.GetSegments{}, o.HandleGetSegments) },
		func() error { return RegisterStreamHandler(o, &server.ConsumeSegment{}, o.HandleConsumeSegment) },
		func() error { return RegisterStreamHandler(o, &server.Produce{}, o.HandleProduce) },
		func() error { return RegisterStreamHandler(o, &server.Consume{}, o.HandleConsume) },
	}

	// Execute all stream handlers
	for _, register := range streamHandlers {
		if err := register(); err != nil {
			return err
		}
	}

	return nil
}

func (o *DefaultObserver) HandleProduce(args *server.Produce, stream broker.BidiStream) {
	entries := broker.NewStreamEnumerator[*server.Record](stream)
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

func (o *DefaultObserver) HandleConsume(args *server.Consume, stream broker.BidiStream) {
	enumerator := o.service.Consume(o.ctx, args)
	streamEntries(stream, enumerator)
}

func (o *DefaultObserver) HandleGetSpaces(args *server.GetSpaces, stream broker.BidiStream) {
	enumerator := o.service.GetSpaces(o.ctx)
	streamNames(stream, enumerator)
}

func (o *DefaultObserver) HandlePeek(args *server.Peek, stream broker.BidiStream) {

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

func (o *DefaultObserver) HandleConsumeSpace(args *server.ConsumeSpace, stream broker.BidiStream) {
	enumerator := o.service.ConsumeSpace(o.ctx, args)
	streamEntries(stream, enumerator)
}

func (o *DefaultObserver) HandleConsumeSegment(args *server.ConsumeSegment, stream broker.BidiStream) {
	enumerator := o.service.ConsumeSegment(o.ctx, args)
	streamEntries(stream, enumerator)
}

func (o *DefaultObserver) HandleGetSegments(args *server.GetSegments, stream broker.BidiStream) {
	enumerator := o.service.GetSegments(o.ctx, args.Space)
	streamNames(stream, enumerator)
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

func streamEntries(stream broker.BidiStream, enumerator enumerators.Enumerator[*server.Entry]) {
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
		o.cancel()
		for _, sub := range o.subs {
			sub.Unsubscribe()
		}
		o.subs = nil
		<-o.ctx.Done()
	})
}
