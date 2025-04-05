package server

import (
	"github.com/fgrzl/streams/broker"
)

type Node interface {
	GetBus() broker.Bus
	GetStreamBus() broker.StreamBus
	GetNotificationBus() broker.NotificationBus
}
