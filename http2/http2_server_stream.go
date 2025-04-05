package http2

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
)

type HTTP2ServerStream struct {
	rw        http.ResponseWriter
	req       *http.Request
	decoder   *json.Decoder
	encoder   *json.Encoder
	closeOnce sync.Once
	closeErr  error
	writeMu   sync.Mutex
}

// NewHTTP2ServerStream upgrades the connection and initializes the stream.
func NewHTTP2ServerStream(w http.ResponseWriter, r *http.Request) (*HTTP2ServerStream, error) {
	// Ensure request is HTTP/2
	if r.ProtoMajor != 2 {
		return nil, errors.New("not an HTTP/2 request")
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("response writer does not support flushing")
	}

	// Set headers
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Wrap the connection for reading/writing
	reader := r.Body
	writer := w

	return &HTTP2ServerStream{
		rw:      w,
		req:     r,
		decoder: json.NewDecoder(reader),
		encoder: json.NewEncoder(writer),
	}, nil
}

func (s *HTTP2ServerStream) Encode(m any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closeErr != nil {
		return s.closeErr
	}
	err := s.encoder.Encode(m)
	if f, ok := s.rw.(http.Flusher); ok {
		f.Flush()
	}
	return err
}

func (s *HTTP2ServerStream) Decode(m any) error {
	if s.closeErr != nil {
		return s.EndOfStreamError()
	}
	err := s.decoder.Decode(m)
	if err == io.EOF {
		return s.EndOfStreamError()
	}
	return err
}

func (s *HTTP2ServerStream) CloseSend(err error) error {
	s.Close(err)
	return err
}

func (s *HTTP2ServerStream) Close(err error) {
	s.closeOnce.Do(func() {
		s.closeErr = err
		s.req.Body.Close()
	})
}

func (s *HTTP2ServerStream) EndOfStreamError() error {
	return io.EOF
}
