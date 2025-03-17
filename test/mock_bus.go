package streams_test

import (
	"math/rand"
	"sync"

	"github.com/fgrzl/streams/broker"
)

var _ broker.Bus = &mockBus{}

type mockBus struct {
	mu             sync.Mutex
	handlers       map[string]map[*TestSubscription]broker.SubscriptionHandler
	streamHandlers map[string]map[*TestSubscription]broker.StreamSubscriptionHandler
}

// NewMockBus initializes the bus with an empty handler map.
func NewMockBus() broker.Bus {
	return &mockBus{
		handlers:       make(map[string]map[*TestSubscription]broker.SubscriptionHandler),
		streamHandlers: make(map[string]map[*TestSubscription]broker.StreamSubscriptionHandler),
	}
}

// Notify finds all the handlers for the type of args and calls them.
func (t *mockBus) Notify(args broker.Routeable) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	route := args.GetRoute()
	if subs, exists := t.handlers[route]; exists {
		for _, handler := range subs {
			go func(h broker.SubscriptionHandler) {
				h(args)
			}(handler)
		}
	}

	return nil
}

// Subscribe registers a new handler for a specific type of message.
func (t *mockBus) Subscribe(route string, handler broker.SubscriptionHandler) (broker.Subscription, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Ensure the key exists in handlers map
	if _, exists := t.handlers[route]; !exists {
		t.handlers[route] = make(map[*TestSubscription]broker.SubscriptionHandler)
	}

	subscription := &TestSubscription{bus: t, key: route}
	t.handlers[route][subscription] = handler
	return subscription, nil
}

func (t *mockBus) CallStream(args broker.Routeable) (broker.BidiStream, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	route := args.GetRoute()
	var handlers []broker.StreamSubscriptionHandler
	if subs, exists := t.streamHandlers[route]; exists {
		for _, handler := range subs {
			handlers = append(handlers, handler)
		}
	}

	client := NewMockBidiStream()

	if len(handlers) > 0 {
		server := NewMockBidiStream()
		LinkStreams(client, server)
		randomHandler := handlers[rand.Intn(len(handlers))]
		go randomHandler(args, server)

	} else {
		// If no handlers, just close the stream immediately
		client.Close(nil)
	}

	return client, nil
}

// SubscribeToRequest implements server.Bus.
func (t *mockBus) SubscribeToStream(route string, handler broker.StreamSubscriptionHandler) (broker.Subscription, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Ensure the key exists in handlers map
	if _, exists := t.streamHandlers[route]; !exists {
		t.streamHandlers[route] = make(map[*TestSubscription]broker.StreamSubscriptionHandler)
	}

	subscription := &TestSubscription{bus: t, key: route}
	t.streamHandlers[route][subscription] = handler
	return subscription, nil

}

var _ broker.Subscription = &TestSubscription{}

type TestSubscription struct {
	bus *mockBus
	key string
}

// Unsubscribe removes the handler from mockBus.
func (s *TestSubscription) Unsubscribe() {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()

	if subs, exists := s.bus.handlers[s.key]; exists {
		delete(subs, s)
		if len(subs) == 0 {
			delete(s.bus.handlers, s.key) // Clean up if no more subscriptions
		}
	}
}
