package http2

import (
	"net/http"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streams/broker"
)

func HandleStream(bus *HTTP2StreamBus, w http.ResponseWriter, r *http.Request) {
	stream, err := NewHTTP2ServerStream(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer stream.Close(nil)

	envelope := &polymorphic.Envelope{}
	if err := stream.Decode(envelope); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msg, ok := envelope.Content.(broker.Routeable)
	if !ok {
		http.Error(w, "Invalid message type", http.StatusBadRequest)
		return
	}
	route := msg.GetRoute()
	if route == "" {
		http.Error(w, "Route not found", http.StatusBadRequest)
		return
	}

	value, ok := bus.streamHandlers.Load(route)
	if !ok {
		http.Error(w, "Handler not found", http.StatusNotFound)
		return
	}

	handler, ok := value.(broker.StreamSubscriptionHandler)
	if !ok {
		http.Error(w, "Invalid handler type", http.StatusInternalServerError)
		return
	}

	handler(msg, stream)
}
