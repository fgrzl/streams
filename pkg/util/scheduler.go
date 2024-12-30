package util

import (
	"sync"
	"time"
)

type TaskFunc[T any] func() (T, error)
type ChannelTaskFunc[T any] func() (<-chan T, <-chan error)

// Worker represents the worker that processes tasks for a specific key.
type worker[T any] struct {
	queue       chan func() (T, error)
	stop        chan struct{}
	lastActive  time.Time
	idleTimeout time.Duration
}

// Scheduler manages workers and schedules tasks.
type Scheduler[K comparable, T any] struct {
	Disposable
	workers sync.Map
}

// NewScheduler initializes a new Scheduler.
func NewScheduler[K comparable, T any]() *Scheduler[K, T] {
	return &Scheduler[K, T]{}
}

// Schedule schedules a task and returns the result and error channels.
// It supports both signatures: (func() (T, error)) and (func() (<-chan T, <-chan error)).
func (s *Scheduler[K, T]) Schedule(key K, task interface{}) (<-chan T, <-chan error) {
	// Retrieve or create the worker for the key
	value, _ := s.workers.LoadOrStore(key, s.newWorker(key))
	w := value.(*worker[T])

	// Create channels to return the result and error
	resultChan := make(chan T, 1)
	errorChan := make(chan error, 1)

	// Enqueue the task in the worker's queue
	switch t := task.(type) {
	case TaskFunc[T]:
		// Handle the signature func() (T, error)
		w.queue <- func() (T, error) {
			result, err := t()
			if err != nil {
				errorChan <- err
			} else {
				resultChan <- result
			}
			// Update the last active time
			w.lastActive = time.Now()
			return result, err
		}
	case ChannelTaskFunc[T]:
		// Handle the signature func() (<-chan T, <-chan error)
		w.queue <- func() (T, error) {
			// Execute the ChannelTaskFunc and handle channels
			rCh, eCh := t()
			go func() {
				for {
					select {
					case r, ok := <-rCh:
						if !ok {
							rCh = nil // Stop listening on this channel
						} else {
							resultChan <- r
						}
					case e, ok := <-eCh:
						if !ok {
							eCh = nil // Stop listening on this channel
						} else {
							errorChan <- e
						}
					}
					// Stop if both channels are closed
					if rCh == nil && eCh == nil {
						break
					}
				}
			}()
			// Update the last active time
			w.lastActive = time.Now()
			var emptyVal T
			return emptyVal, nil
		}
	default:
		// Handle unexpected task types
		close(resultChan)
		close(errorChan)
	}

	// Return channels to receive the result or error
	return resultChan, errorChan
}

// newWorker creates a new worker for a given key and starts its goroutine.
func (s *Scheduler[K, T]) newWorker(key K) *worker[T] {
	w := &worker[T]{
		queue:       make(chan func() (T, error), 100), // Buffer size can be adjusted
		stop:        make(chan struct{}),
		lastActive:  time.Now(),
		idleTimeout: 10 * time.Second, // Example timeout for idle workers
	}

	go s.runWorker(key, w)
	return w
}

// runWorker processes tasks for a worker serially and checks for idle time.
func (s *Scheduler[K, T]) runWorker(key K, w *worker[T]) {
	for {
		select {
		case task := <-w.queue:
			// Execute the task
			task()
		case <-w.stop:
			// Cleanup and remove worker
			close(w.queue)
			s.workers.Delete(key)
			return
		case <-time.After(1 * time.Second): // Check for idle time periodically
			if time.Since(w.lastActive) > w.idleTimeout {
				// If the worker is idle for too long, clean it up
				close(w.stop)
				s.workers.Delete(key)
				return
			}
		}
	}
}

// StopWorker shuts down a specific worker gracefully.
func (s *Scheduler[K, T]) StopWorker(key K) {
	if value, ok := s.workers.LoadAndDelete(key); ok {
		w := value.(*worker[T])
		close(w.stop)
	}
}

// Stop shuts down all workers gracefully.
func (s *Scheduler[K, T]) Stop() {
	s.workers.Range(func(key, value any) bool {
		w := value.(*worker[T])
		close(w.stop)
		return true
	})
}

func (s *Scheduler[K, T]) Dispose() {
	s.Stop()
}
