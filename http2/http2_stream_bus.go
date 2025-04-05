package http2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/fgrzl/json/polymorphic"

	"github.com/fgrzl/streams/broker"
)

var (
	_ broker.StreamBus    = (*HTTP2StreamBus)(nil)
	_ broker.Subscription = (*sub)(nil)
)

func NewHTTP2StreamBus(client *http.Client) *HTTP2StreamBus {
	return &HTTP2StreamBus{
		client: client,
	}
}

type HTTP2StreamBus struct {
	client         *http.Client
	streamHandlers sync.Map
}

func (b *HTTP2StreamBus) CallStream(ctx context.Context, args broker.Routeable) (broker.BidiStream, error) {

	url, ok := StreamEndpointFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("stream endpoint not found in context")
	}

	pr, pw := io.Pipe()
	req, _ := http.NewRequestWithContext(ctx, "POST", url, pr)
	req.Header.Set("Content-Type", "application/json")

	if jwt, ok := JWTFromContext(ctx); ok {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	stream := &HTTP2ClientStream{
		encoder:  json.NewEncoder(pw),
		decoder:  json.NewDecoder(resp.Body),
		reqBody:  pw,
		respBody: resp.Body,
	}

	msg := polymorphic.NewEnvelope(args)
	if err := stream.Encode(msg); err != nil {
		stream.Close(err)
		return nil, err
	}

	return stream, nil
}

func (b *HTTP2StreamBus) SubscribeToStream(ctx context.Context, route string, handler broker.StreamSubscriptionHandler) (broker.Subscription, error) {
	_, ok := b.streamHandlers.LoadOrStore(route, handler)
	if !ok {
		return nil, broker.ErrNoResponders
	}
	return &sub{
		bus: b,
		key: route,
	}, nil
}

var _ broker.Subscription = &sub{}

type sub struct {
	bus *HTTP2StreamBus
	key string
}

// Unsubscribe removes the handler from mockBus.
func (t *sub) Unsubscribe() {
	t.bus.streamHandlers.Delete(t.key)
}
