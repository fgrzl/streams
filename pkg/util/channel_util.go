package util

import (
	"sync"

	"golang.org/x/exp/constraints"
)

// Define a generic ChannelPair type
type ChannelPair[T any] struct {
	DataChannel  <-chan T
	ErrorChannel <-chan error
}

// ChainReader takes a slice of ChannelPair[T] and returns two channels: one for data and one for errors.
func ChainReader[T any](readers []ChannelPair[T]) (<-chan T, <-chan error) {
	// Create the output channels
	out := make(chan T)
	errCh := make(chan error)

	// Start a goroutine to handle the chaining logic
	go func() {
		defer close(out)
		defer close(errCh)

		// Iterate over each reader pair in the slice
		for _, reader := range readers {
			// Handle both data and error channels concurrently for each reader
			go func(r ChannelPair[T]) {
				// Forward data from DataChannel to out channel
				for data := range r.DataChannel {
					out <- data
				}

				// Forward errors from ErrorChannel to errCh channel
				for err := range r.ErrorChannel {
					errCh <- err
				}
			}(reader)
		}
	}()

	return out, errCh
}

// Chunk splits a source channel into smaller chunks, each of size 'size'.
// It returns a channel of channels, each of which contains a chunk of 'size' elements.
func Chunk[T any](ch <-chan T, size int) <-chan chan T {
	chunks := make(chan chan T, 1) // Channel of channels to return

	go func() {
		defer close(chunks)

		var chunk chan T
		var counter int

		for r := range ch {

			if chunk == nil {
				chunk = make(chan T, 1)
				chunks <- chunk
			}

			chunk <- r

			counter++

			if counter >= size {
				close(chunk)
				chunk = nil
				counter = 0
			}
		}

		if chunk != nil {
			close(chunk)
		}
	}()

	return chunks
}

func ChunkBy[T any, TSize constraints.Integer](ch <-chan T, targetSize TSize, currentSize func(current T) TSize) chan chan T {
	chunks := make(chan chan T, 1)
	go func() {
		defer close(chunks)

		var chunk chan T
		var size TSize

		for r := range ch {

			if chunk == nil {
				chunk = make(chan T, 1)
				chunks <- chunk
			}

			chunk <- r

			size += currentSize(r)

			if size > targetSize {
				close(chunk)
				chunk = nil
				size = 0
			}
		}

		if chunk != nil {
			close(chunk)
		}
	}()
	return chunks
}

func Map[TIn any, TOut any](in <-chan TIn, convert func(item TIn) TOut) <-chan TOut {
	out := make(chan TOut, 1)

	go func() {
		defer close(out) // Ensure the output channel is closed when the goroutine exits

		for item := range in {
			// Process and send the converted item to the output channel
			out <- convert(item)
		}
	}()

	return out
}

func MergeErrors(chans ...<-chan error) <-chan error {
	out := make(chan error, 1)

	go func() {
		defer close(out) // Ensure the output channel is closed when done

		var wg sync.WaitGroup
		for _, ch := range chans {
			wg.Add(1)
			go func(ch <-chan error) {
				defer wg.Done()
				for {
					select {
					case err, ok := <-ch:
						if !ok {
							return // Channel closed
						}
						select {
						case out <- err:
						}
					}
				}
			}(ch)
		}

		wg.Wait() // Wait for all goroutines to finish
	}()

	return out
}

func Drain[T any](outCh <-chan T, errCh <-chan error) error {
	for {
		select {
		case result, ok := <-outCh:
			if !ok {
				// Output channel closed; stop listening to it
				outCh = nil
			} else {
				// Handle result if needed
				_ = result
			}

		case err, ok := <-errCh:
			if !ok {
				// Error channel closed; stop listening to it
				errCh = nil
			} else if err != nil {
				// Return or handle the error
				return err
			}
		}

		// Exit when both channels are drained
		if outCh == nil && errCh == nil {
			return nil
		}
	}
}

func ToSlice[T any](ch <-chan T) []T {
	var slice []T
	for r := range ch {
		slice = append(slice, r)
	}
	return slice
}
