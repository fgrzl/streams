package server

type StreamsError struct {
	Code    int
	Message string
}

func (e *StreamsError) Error() string {
	return e.Message
}

const (
	ErrCodeTransient = 1
	ErrCodePermanent = 2
)

// Error helpers
func NewTransientError(msg string) error {
	return &StreamsError{Code: ErrCodeTransient, Message: msg}
}

func NewPermanentError(msg string) error {
	return &StreamsError{Code: ErrCodePermanent, Message: msg}
}
