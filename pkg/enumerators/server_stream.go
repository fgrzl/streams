package enumerators

import (
	"io"

	"google.golang.org/grpc"
)

type serverEnumerator[TReq any, TRes any] struct {
	Enumerator[TReq]
	stream  grpc.BidiStreamingServer[TReq, TRes]
	current TReq
	err     error
}

func (e *serverEnumerator[TReq, TRes]) MoveNext() bool {
	current, err := e.stream.Recv()
	if err == io.EOF {
		var zero TReq
		e.current = zero
		return false
	}
	if err != nil {
		e.err = err
		return false
	}
	e.current = *current
	return true
}

func (e *serverEnumerator[TReq, TRes]) Current() (*TReq, error) {
	return &e.current, e.err
}

func (e *serverEnumerator[TReq, TRes]) Err() error {
	return e.err
}

func (e *serverEnumerator[TReq, TRes]) Dispose() {
	// no-op
}

// A new enumerator of the requests
func NewBidiStreamingServerEnumerator[TReq any, TRes any](stream grpc.BidiStreamingServer[TReq, TRes]) Enumerator[*TReq] {
	return &serverEnumerator[TReq, TRes]{stream: stream}
}
