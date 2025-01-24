package enumerators

import (
	"errors"

	"golang.org/x/exp/constraints"
)

type chunkEnumerator[T any, TSize constraints.Ordered] struct {
	base         Enumerator[T]
	target       TSize
	compute      func(item T) (TSize, error)
	currentChunk *innerChunkEnumerator[T, TSize]
	err          error
	exhausted    bool
}

func (e *chunkEnumerator[T, TSize]) Dispose() {
	if e.currentChunk != nil {
		e.currentChunk.Dispose()
	}
	if e.base != nil {
		e.base.Dispose()
	}
}

func (e *chunkEnumerator[T, TSize]) MoveNext() bool {

	if e.exhausted {
		return false
	}

	if e.currentChunk == nil {
		if !e.base.MoveNext() {
			e.exhausted = true
			return false
		}
		e.currentChunk = &innerChunkEnumerator[T, TSize]{
			base:    e.base,
			compute: e.compute,
			target:  e.target,
			pending: true,
		}
		return true
	}

	if e.currentChunk.exhausted && !e.currentChunk.pending {
		e.err = e.currentChunk.err
		return false
	}

	for !e.currentChunk.exhausted {
		for e.currentChunk.MoveNext() {
			// no-op
		}
	}

	e.currentChunk = &innerChunkEnumerator[T, TSize]{
		base:    e.base,
		compute: e.compute,
		target:  e.target,
		pending: true,
	}
	return true
}

func (e *chunkEnumerator[T, TSize]) Current() (Enumerator[T], error) {
	if e.currentChunk == nil {
		return nil, errors.New("no current chunk")
	}
	return e.currentChunk, e.currentChunk.err
}

func (e *chunkEnumerator[T, TSize]) Err() error {
	return e.currentChunk.err
}

type innerChunkEnumerator[T any, TSize constraints.Ordered] struct {
	base       Enumerator[T]
	compute    func(item T) (TSize, error)
	target     TSize
	cumulative TSize
	current    T
	err        error
	exhausted  bool
	pending    bool
	count      int
}

func (e *innerChunkEnumerator[T, TSize]) Dispose() {
	// no-op
}

func (e *innerChunkEnumerator[T, TSize]) MoveNext() bool {
	if e.exhausted {
		return false
	}

	if !e.pending {
		if !e.base.MoveNext() {
			e.exhausted = true
			e.err = e.base.Err()
			return false
		}
	}

	item, err := e.base.Current()
	if err != nil {
		e.err = err
		return false
	}

	size, err := e.compute(item)

	if err != nil {
		e.err = err
		return false
	}

	if e.cumulative+size > e.target && e.count > 0 {
		e.exhausted = true
		e.pending = true
		return false
	}

	e.cumulative += size
	e.current = item
	e.pending = false
	e.count++
	return true
}

func (e *innerChunkEnumerator[T, TSize]) Current() (T, error) {
	return e.current, e.err
}

func (c *innerChunkEnumerator[T, TSize]) Err() error {
	return c.err
}

func Chunk[T any, TSize constraints.Ordered](
	in Enumerator[T],
	target TSize,
	compute func(item T) (TSize, error),
) Enumerator[Enumerator[T]] {
	if in == nil {
		return &chunkEnumerator[T, TSize]{exhausted: true}
	}
	return &chunkEnumerator[T, TSize]{
		base:    in,
		target:  target,
		compute: compute,
	}
}

func ChunkByCount[T any](in Enumerator[T], count int) Enumerator[Enumerator[T]] {
	return Chunk(in, count, func(item T) (int, error) {
		return 1, nil
	})
}

// Collect gathers all chunks into a slice of slices
func Collect[T any](enumerator Enumerator[Enumerator[T]]) ([][]T, error) {
	var chunks [][]T
	var err error
	for enumerator.MoveNext() {
		chunk, err := enumerator.Current()
		if err != nil {
			return chunks, err
		}

		var chunkItems []T
		for chunk.MoveNext() {
			item, err := chunk.Current()
			if err != nil {
				return chunks, err
			}
			chunkItems = append(chunkItems, item)
		}
		chunks = append(chunks, chunkItems)

		_, err = enumerator.Current()
		if err != nil {
			return chunks, err
		}
	}
	return chunks, err
}
