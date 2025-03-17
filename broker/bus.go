package broker

import "github.com/fgrzl/json/polymorphic"

type Routeable interface {
	polymorphic.Polymorphic
	GetRoute() string
}

type SubscriptionHandler func(Routeable)
type StreamSubscriptionHandler func(Routeable, BidiStream)

type Bus interface {
	Notify(Routeable) error
	Subscribe(string, SubscriptionHandler) (Subscription, error)

	CallStream(Routeable) (BidiStream, error)
	SubscribeToStream(string, StreamSubscriptionHandler) (Subscription, error)
}

type Subscription interface {
	Unsubscribe()
}
