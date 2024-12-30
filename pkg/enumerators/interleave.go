package enumerators

import (
	"container/heap"

	"golang.org/x/exp/constraints"
)

// interleaveEnumerator represents the merged enumerator
type interleaveEnumerator[T any, TOrdered constraints.Ordered] struct {
	enumerators []Enumerator[T]
	key         func(T) TOrdered
	queue       *priorityQueue[T, TOrdered]
	current     T
	err         error
}

func (e *interleaveEnumerator[T, TOrdered]) MoveNext() bool {

	if e.queue.Len() == 0 {
		return false
	}

	// Pop the next item from the queue
	top := heap.Pop(e.queue).(queueItem[T, TOrdered])
	position := top.position
	e.current = top.item

	// Advance the enumerator and add its next item to the queue, if available
	if e.enumerators[position].MoveNext() {
		nextItem, err := e.enumerators[position].Current()
		if err == nil {
			heap.Push(e.queue, queueItem[T, TOrdered]{
				position: position,
				item:     nextItem,
				key:      e.key,
			})
		}
	}

	return true
}

func (e *interleaveEnumerator[T, TOrdered]) Current() (T, error) {
	return e.current, e.err
}
func (e *interleaveEnumerator[T, TOrdered]) Err() error {
	return e.err
}

func (e *interleaveEnumerator[T, TOrdered]) Dispose() {
	for _, enumerator := range e.enumerators {
		enumerator.Dispose()
	}
	e.queue = nil // Clear the queue for memory safety
}

// queueItem wraps an entry along with its originating stream index
type queueItem[T any, TOrdered constraints.Ordered] struct {
	position int
	item     T
	key      func(T) TOrdered
}

// priorityQueue implements heap.Interface for queueItem based on the key ordering
type priorityQueue[T any, TOrdered constraints.Ordered] []queueItem[T, TOrdered]

func (q priorityQueue[T, TOrdered]) Len() int {
	return len(q)
}

func (q priorityQueue[T, TOrdered]) Less(i, j int) bool {
	left := q[i]
	right := q[j]
	return left.key(left.item) < right.key(right.item)
}

func (q priorityQueue[T, TOrdered]) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *priorityQueue[T, TOrdered]) Push(x interface{}) {
	*q = append(*q, x.(queueItem[T, TOrdered]))
}

func (q *priorityQueue[T, TOrdered]) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[:n-1]
	return item
}

// Interleave creates a new interleave enumerator that merges multiple enumerators
// based on the ordering provided by the key function.
func Interleave[T any, TOrdered constraints.Ordered](
	enumerators []Enumerator[T],
	key func(T) TOrdered,
) Enumerator[T] {
	// Handle the edge case where no enumerators are provided
	if len(enumerators) == 0 {
		return Empty[T]()
	}

	// Initialize the priority queue
	q := &priorityQueue[T, TOrdered]{}
	for i, enumerator := range enumerators {
		if enumerator.MoveNext() {
			item, err := enumerator.Current()
			if err == nil {
				heap.Push(q, queueItem[T, TOrdered]{
					position: i,
					item:     item,
					key:      key,
				})
			}
		}
	}

	return &interleaveEnumerator[T, TOrdered]{
		enumerators: enumerators,
		key:         key,
		queue:       q,
	}
}
