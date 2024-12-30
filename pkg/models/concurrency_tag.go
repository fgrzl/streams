package models

import "fmt"

type ConcurrencyTag interface{}

// ConcurrencyError represents an error related to concurrency issues.
type ConcurrencyError struct {
	Message string
}

// Error implements the error interface for ConcurrencyError.
func (e *ConcurrencyError) Error() string {
	return fmt.Sprintf("Concurrency Error: %s", e.Message)
}

// NewConcurrencyError is a constructor function for creating a new ConcurrencyError.
func NewConcurrencyError(message string) error {
	return &ConcurrencyError{
		Message: message,
	}
}
