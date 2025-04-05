package http2

import (
	"encoding/json"
	"io"
	"sync"
)

type HTTP2ClientStream struct {
	encoder   *json.Encoder
	decoder   *json.Decoder
	reqBody   io.WriteCloser
	respBody  io.ReadCloser
	closeOnce sync.Once
	closeErr  error
	writeMu   sync.Mutex
}

func (s *HTTP2ClientStream) Encode(m any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closeErr != nil {
		return s.closeErr
	}
	return s.encoder.Encode(m)
}

func (s *HTTP2ClientStream) Decode(m any) error {
	if s.closeErr != nil {
		return s.EndOfStreamError()
	}
	err := s.decoder.Decode(m)
	if err == io.EOF {
		return s.EndOfStreamError()
	}
	return err
}

func (s *HTTP2ClientStream) CloseSend(err error) error {
	s.Close(err)
	return err
}

func (s *HTTP2ClientStream) Close(err error) {
	s.closeOnce.Do(func() {
		s.closeErr = err
		s.reqBody.Close()
		s.respBody.Close()
	})
}

func (s *HTTP2ClientStream) EndOfStreamError() error {
	return io.EOF
}
