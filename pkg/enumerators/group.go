package enumerators

import (
	"errors"
)

type Grouping[T any, G comparable] struct {
	Enumerator *innerGroupEnumerator[T, G]
	Key        G
}

type GroupingSlice[T any, G comparable] struct {
	Items []T
	Group G
}

type GroupEnumerator[T any, G comparable] struct {
	base         Enumerator[T]
	compute      func(item T) (G, error)
	currentChunk *Grouping[T, G]
	err          error
	exhausted    bool
}

func (e *GroupEnumerator[T, G]) Dispose() {
	if e.currentChunk != nil {
		e.currentChunk.Enumerator.Dispose()
	}
	if e.base != nil {
		e.base.Dispose()
	}
}

func (e *GroupEnumerator[T, G]) MoveNext() bool {

	if e.exhausted {
		return false
	}

	if e.currentChunk == nil {
		inner := &innerGroupEnumerator[T, G]{
			base:    e.base,
			compute: e.compute,
			pending: false,
		}

		if !inner.MoveNext() {
			e.err = inner.Err()
			if e.err != nil {
				return false
			}
			if inner.exhausted && !inner.pending {
				// no more items
				return false
			}
		}
		// reset exhausted flag
		inner.exhausted = false
		e.currentChunk = &Grouping[T, G]{
			Enumerator: inner,
			Key:        inner.group,
		}
		return true
	}

	inner := e.currentChunk.Enumerator
	if inner == nil {
		e.err = errors.New("no inner enumerator")
		return false
	}

	if inner.err != nil {
		e.err = inner.err
		return false
	}

	if inner.exhausted && !inner.pending {
		return false
	}

	for !e.currentChunk.Enumerator.exhausted {
		for e.currentChunk.Enumerator.MoveNext() {
			// no-op
		}
	}

	inner.exhausted = false
	inner.pending = true
	e.currentChunk = &Grouping[T, G]{
		Enumerator: inner,
		Key:        inner.group,
	}
	return true
}

func (e *GroupEnumerator[T, G]) Current() (*Grouping[T, G], error) {
	if e.currentChunk == nil {
		return nil, errors.New("no current chunk")
	}
	return e.currentChunk, e.currentChunk.Enumerator.err
}

func (e *GroupEnumerator[T, G]) Err() error {
	if e.currentChunk == nil {
		return errors.New("no current chunk")
	}
	return e.currentChunk.Enumerator.err
}

type innerGroupEnumerator[T any, G comparable] struct {
	base      Enumerator[T]
	compute   func(item T) (G, error)
	group     G
	current   T
	err       error
	exhausted bool
	pending   bool
	count     int
}

func (e *innerGroupEnumerator[T, G]) Dispose() {
	// no-op
}

func (e *innerGroupEnumerator[T, G]) MoveNext() bool {
	if e.exhausted {
		return false
	}
	if e.pending {
		e.current, e.err = e.base.Current()
		e.pending = false
		return true
	}

	if !e.base.MoveNext() {
		e.exhausted = true
		e.err = e.base.Err()
		return false
	}

	item, err := e.base.Current()
	if err != nil {
		e.err = err
		return false
	}

	group, err := e.compute(item)

	if err != nil {
		e.err = err
		return false
	}

	if e.group != group {
		e.group = group
		e.pending = true
		e.exhausted = true
		return false
	}

	e.group = group
	e.current = item
	e.pending = false
	e.count++
	return true
}

func (e *innerGroupEnumerator[T, G]) Current() (T, error) {
	return e.current, e.err
}

func (c *innerGroupEnumerator[T, G]) Err() error {
	return c.err
}

func Group[T any, G comparable](
	in Enumerator[T],
	compute func(item T) (G, error),
) Enumerator[*Grouping[T, G]] {
	return &GroupEnumerator[T, G]{base: in, compute: compute}

}

// Collect gathers all chunks into a slice of slices
func CollectGroupingSlices[T any, G comparable](enumerator Enumerator[*Grouping[T, G]]) (groupSlices []*GroupingSlice[T, G], err error) {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {

		grouping, err := enumerator.Current()
		if err != nil {
			return groupSlices, err
		}

		groupSlice := &GroupingSlice[T, G]{
			Group: grouping.Key,
		}
		for grouping.Enumerator.MoveNext() {
			item, err := grouping.Enumerator.Current()
			if err != nil {
				return groupSlices, err
			}
			groupSlice.Items = append(groupSlice.Items, item)
		}
		groupSlices = append(groupSlices, groupSlice)
	}
	return groupSlices, err
}
