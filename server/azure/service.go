package azure

import (
	"context"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/server"
)

type Service interface {
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]
	ConsumeSpace(ctx context.Context, args *server.ConsumeSpace) enumerators.Enumerator[*server.Entry]
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]
	ConsumeSegment(ctx context.Context, args *server.ConsumeSegment) enumerators.Enumerator[*server.Entry]
	Peek(ctx context.Context, space, segment string) (*server.Entry, error)
	Consume(ctx context.Context, args *server.Consume) enumerators.Enumerator[*server.Entry]
	Produce(ctx context.Context, args *server.Produce, entries enumerators.Enumerator[*server.Record]) enumerators.Enumerator[*server.SegmentStatus]
	Close() error
}

func NewAzureService(path string) (Service, error) {
	return &azureService{}, nil
}

type azureService struct {
}

// Close implements Service.
func (a *azureService) Close() error {
	panic("unimplemented")
}

// Consume implements Service.
func (a *azureService) Consume(ctx context.Context, args *server.Consume) enumerators.Enumerator[*server.Entry] {
	panic("unimplemented")
}

// ConsumeSegment implements Service.
func (a *azureService) ConsumeSegment(ctx context.Context, args *server.ConsumeSegment) enumerators.Enumerator[*server.Entry] {
	panic("unimplemented")
}

// ConsumeSpace implements Service.
func (a *azureService) ConsumeSpace(ctx context.Context, args *server.ConsumeSpace) enumerators.Enumerator[*server.Entry] {
	panic("unimplemented")
}

// GetSegments implements Service.
func (a *azureService) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	panic("unimplemented")
}

// GetSpaces implements Service.
func (a *azureService) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	panic("unimplemented")
}

// Peek implements Service.
func (a *azureService) Peek(ctx context.Context, space string, segment string) (*server.Entry, error) {
	panic("unimplemented")
}

// Produce implements Service.
func (a *azureService) Produce(ctx context.Context, args *server.Produce, entries enumerators.Enumerator[*server.Record]) enumerators.Enumerator[*server.SegmentStatus] {
	panic("unimplemented")
}
