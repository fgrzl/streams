package wsstream

import (
	"context"
	"sync"

	"github.com/fgrzl/streams/broker"
	"github.com/google/uuid"
)

type WebSocketBus struct {
	dialer      WebSocketDialer
	clientMux   *WebSocketMuxer
	muxesMu     sync.Mutex
	handlers    map[string]broker.StreamSubscriptionHandler
	handlersMu  sync.RWMutex
	defaultAddr string
}

// NewWebSocketBus creates a StreamBus using a default connection target.
func NewWebSocketBus(dialer WebSocketDialer, defaultAddress string) (broker.StreamBus, error) {
	return &WebSocketBus{
		dialer:      dialer,
		handlers:    make(map[string]broker.StreamSubscriptionHandler),
		defaultAddr: defaultAddress,
	}, nil
}

// CallStream sends an initial message and returns a BidiStream for continued use.
func (b *WebSocketBus) CallStream(ctx context.Context, msg broker.Routeable) (broker.BidiStream, error) {
	muxer := b.getOrCreateClientMuxer(ctx)
	stream := muxer.Register(uuid.New())

	if err := stream.Encode(msg); err != nil {
		stream.Close(err)
		return nil, err
	}

	return stream, nil
}

// SubscribeToStream registers a handler for inbound stream messages.
func (b *WebSocketBus) SubscribeToStream(ctx context.Context, route string, handler broker.StreamSubscriptionHandler) (broker.Subscription, error) {
	b.handlersMu.Lock()
	defer b.handlersMu.Unlock()

	b.handlers[route] = handler

	return &wsSub{
		bus:   b,
		route: route,
	}, nil
}

func (b *WebSocketBus) getOrCreateClientMuxer(ctx context.Context) *WebSocketMuxer {
	b.muxesMu.Lock()
	defer b.muxesMu.Unlock()

	if b.clientMux != nil {
		return b.clientMux
	}

	conn, err := b.dialer.Dial(ctx, b.defaultAddr)
	if err != nil {
		panic("dial failed: " + err.Error())
	}

	mux := NewWebSocketMuxer(conn)
	b.clientMux = mux
	return mux
}

type wsSub struct {
	bus   *WebSocketBus
	route string
	once  sync.Once
}

// Unsubscribe removes the handler from the WebSocketBus for this route.
func (s *wsSub) Unsubscribe() {
	s.once.Do(func() {
		s.bus.handlersMu.Lock()
		defer s.bus.handlersMu.Unlock()
		delete(s.bus.handlers, s.route)
	})
}
