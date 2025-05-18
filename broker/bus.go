package broker

import (
	"context"
	"errors"

	"github.com/fgrzl/json/polymorphic"
)

var (
	ErrNoResponders = errors.New("nats: no responders available for request")
)

type Routeable interface {
	polymorphic.Polymorphic
	GetRoute() string
}

type SubscriptionHandler func(Routeable)
type StreamSubscriptionHandler func(Routeable, BidiStream)

type Bus interface {
	NotificationBus
	StreamBus
}

type NotificationBus interface {
	Notify(context.Context, Routeable) error
	Subscribe(context.Context, string, SubscriptionHandler) (Subscription, error)
}

type StreamBus interface {
	// CallStream initiates a bidirectional stream to a remote handler.
	// It returns a BidiStream that allows peeking, producing, and consuming messages.
	// This is distinct from domain-level data streams.
	CallStream(ctx context.Context, msg Routeable) (BidiStream, error)

	// SubscribeToStream registers a handler to serve inbound bidirectional streams.
	// The provided handler will be invoked for each incoming stream request matching the given route key.
	SubscribeToStream(ctx context.Context, route string, handler StreamSubscriptionHandler) (Subscription, error)
}

type Subscription interface {
	Unsubscribe()
}
