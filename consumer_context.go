package streams

import (
	"context"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/tickle"
)

// Create a new consumer context
func NewConsumerContext(ctx context.Context, client Client) ConsumerContext {
	// Create a new cancelContext with cancel function
	cancelContext, cancel := context.WithCancel(ctx)

	return &consumerContext{
		Context: cancelContext,
		cancel:  cancel,
		client:  client,
		tickler: tickle.NewTickler(),
	}
}

type ConsumerContext interface {
	context.Context

	// Enumerate multiple spaces from a given minimum time (inclusive).
	NewMultiSpaceEnumeratorFromTime(spaces []string, min int64) (enumerators.Enumerator[*Entry], *tickle.Subscription)

	// Enumerate multiple spaces using specific offsets for each space (exclusive).
	NewMultiSpaceEnumeratorFromOffsets(offsetsBySpace map[string]lexkey.LexKey) (enumerators.Enumerator[*Entry], *tickle.Subscription)

	// Enumerate a single space from a given minimum time (inclusive).
	NewSpaceEnumeratorFromTime(space string, min int64) (enumerators.Enumerator[*Entry], *tickle.Subscription)

	// Enumerate a single space starting from a specific offset (exclusive).
	NewSpaceEnumeratorFromOffset(space string, offset lexkey.LexKey) (enumerators.Enumerator[*Entry], *tickle.Subscription)

	// Enumerate a single segment from a given minimum sequence (exclusive).
	NewSegmentEnumeratorFromSequence(space, segment string, sequence uint64) (enumerators.Enumerator[*Entry], *tickle.Subscription)

	// Enumerate a single segment from a given minimum time (inclusive).
	NewSegmentEnumeratorFromTime(space, segment string, min int64) (enumerators.Enumerator[*Entry], *tickle.Subscription)

	// Cancel the context
	Cancel()
}

type consumerContext struct {
	context.Context
	cancel  context.CancelFunc
	client  Client
	tickler *tickle.Tickler
}

// NewMultiSpaceEnumeratorFromOffsets implements ConsumerContext.
func (c *consumerContext) NewMultiSpaceEnumeratorFromOffsets(offsetsBySpace map[string]lexkey.LexKey) (enumerators.Enumerator[*Entry], *tickle.Subscription) {
	if len(offsetsBySpace) == 0 {
		return enumerators.Empty[*Entry](), nil
	}

	spaces := make([]string, 0, len(offsetsBySpace))
	for space := range offsetsBySpace {
		spaces = append(spaces, space)
	}

	args := &Consume{
		Offsets: offsetsBySpace,
	}
	enumerator := c.client.Consume(c, args)
	sub := c.setupSpaceTickler(spaces...)

	return enumerator, sub
}

// NewMultiSpaceEnumeratorFromTime implements ConsumerContext.
func (c *consumerContext) NewMultiSpaceEnumeratorFromTime(spaces []string, min int64) (enumerators.Enumerator[*Entry], *tickle.Subscription) {
	if len(spaces) == 0 {
		return enumerators.Empty[*Entry](), nil
	}

	offsetsBySpace := make(map[string]lexkey.LexKey, len(spaces))
	for _, space := range spaces {
		offsetsBySpace[space] = lexkey.LexKey{}
	}
	args := &Consume{
		MinTimestamp: min,
		Offsets:      offsetsBySpace,
	}

	enumerator := c.client.Consume(c, args)
	sub := c.setupSpaceTickler(spaces...)
	return enumerator, sub
}

// NewSpaceEnumeratorFromOffset implements ConsumerContext.
func (c *consumerContext) NewSpaceEnumeratorFromOffset(space string, offset lexkey.LexKey) (enumerators.Enumerator[*Entry], *tickle.Subscription) {
	if space == "" {
		return enumerators.Empty[*Entry](), nil
	}

	args := &ConsumeSpace{
		Space:  space,
		Offset: offset,
	}

	enumerator := c.client.ConsumeSpace(c, args)
	sub := c.setupSpaceTickler(space)
	return enumerator, sub
}

// NewSpaceEnumeratorFromTime implements ConsumerContext.
func (c *consumerContext) NewSpaceEnumeratorFromTime(space string, min int64) (enumerators.Enumerator[*Entry], *tickle.Subscription) {
	if space == "" {
		return enumerators.Empty[*Entry](), nil
	}

	args := &ConsumeSpace{
		Space:        space,
		MinTimestamp: min,
	}

	enumerator := c.client.ConsumeSpace(c, args)
	sub := c.setupSpaceTickler(space)
	return enumerator, sub
}

// NewSpaceEnumeratorFromTime implements ConsumerContext.
func (c *consumerContext) NewSegmentEnumeratorFromSequence(space, segment string, sequence uint64) (enumerators.Enumerator[*Entry], *tickle.Subscription) {
	if space == "" || segment == "" {
		return enumerators.Empty[*Entry](), nil
	}

	args := &ConsumeSegment{
		Space:       space,
		Segment:     segment,
		MinSequence: sequence,
	}

	enumerator := c.client.ConsumeSegment(c, args)
	sub := c.setupSegmentTickler(space, segment)
	return enumerator, sub
}

// NewSpaceEnumeratorFromTime implements ConsumerContext.
func (c *consumerContext) NewSegmentEnumeratorFromTime(space, segment string, min int64) (enumerators.Enumerator[*Entry], *tickle.Subscription) {
	if space == "" || segment == "" {
		return enumerators.Empty[*Entry](), nil
	}

	args := &ConsumeSegment{
		Space:        space,
		Segment:      segment,
		MinTimestamp: min,
	}

	enumerator := c.client.ConsumeSegment(c, args)
	sub := c.setupSegmentTickler(space, segment)
	return enumerator, sub
}

func (c *consumerContext) Cancel() {
	c.cancel()
}

func (c *consumerContext) setupSpaceTickler(spaces ...string) *tickle.Subscription {
	for _, space := range spaces {
		c.client.SubcribeToSpace(c, space, c.tickle)
	}
	sub := c.tickler.Subscribe(c, spaces...)
	return sub
}

func (c *consumerContext) setupSegmentTickler(space, segment string) *tickle.Subscription {
	c.client.SubcribeToSegment(c, space, segment, c.tickle)
	token := space + "." + segment
	sub := c.tickler.Subscribe(c, token)
	return sub
}

func (c *consumerContext) tickle(s *SegmentStatus) {
	c.tickler.Tickle(s.Space, s.Space+"."+s.Segment)
}
