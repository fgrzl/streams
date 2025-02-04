package clients

import (
	"io"

	"github.com/fgrzl/enumerators"
	"google.golang.org/grpc"
)

type clientEnumerator[TRes any] struct {
	stream  grpc.ServerStreamingClient[TRes]
	current *TRes
	err     error
}

func (e *clientEnumerator[TRes]) MoveNext() bool {
	current, err := e.stream.Recv()
	if err == io.EOF {
		e.current = nil
		return false
	}
	if err != nil {
		e.err = err
		return false
	}
	e.current = current
	return true
}

func (e *clientEnumerator[TRes]) Current() (*TRes, error) {
	return e.current, e.err
}

func (e *clientEnumerator[TRes]) Err() error {
	return e.err
}

func (e *clientEnumerator[TRes]) Dispose() {
	// no-op
}

type clientResponseEnumerator[TReq any, TRes any] struct {
	stream  grpc.BidiStreamingClient[TReq, TRes]
	current *TRes
	err     error
}

func (e *clientResponseEnumerator[TReq, TRes]) MoveNext() bool {
	current, err := e.stream.Recv()
	if err == io.EOF {
		e.current = nil
		return false
	}
	if err != nil {
		e.err = err
		return false
	}
	e.current = current
	return true
}

func (e *clientResponseEnumerator[TReq, TRes]) Current() (*TRes, error) {
	return e.current, e.err
}

func (e *clientResponseEnumerator[TReq, TRes]) Err() error {
	return e.err
}

func (e *clientResponseEnumerator[TReq, TRes]) Dispose() {
	// no-op
}

// A new enumerator of the responses
func NewServerStreamingClientResponseEnumerator[TRes any](stream grpc.ServerStreamingClient[TRes]) enumerators.Enumerator[*TRes] {
	return &clientEnumerator[TRes]{stream: stream}
}

// A new enumerator of the responses
func NewBidiStreamingClientResponseEnumerator[TReq any, TRes any](stream grpc.BidiStreamingClient[TReq, TRes]) enumerators.Enumerator[*TRes] {
	return &clientResponseEnumerator[TReq, TRes]{stream: stream}
}
