package server

import (
	"context"

	"github.com/fgrzl/enumerators"
)

type Service interface {
	GetClusterStatus() *ClusterStatus
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]
	ConsumeSpace(ctx context.Context, args *ConsumeSpace) enumerators.Enumerator[*Entry]
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]
	ConsumeSegment(ctx context.Context, args *ConsumeSegment) enumerators.Enumerator[*Entry]
	Peek(ctx context.Context, space, segment string) (*Entry, error)
	Consume(ctx context.Context, args *Consume) enumerators.Enumerator[*Entry]
	Produce(ctx context.Context, args *Produce, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus]
	Close() error
}
