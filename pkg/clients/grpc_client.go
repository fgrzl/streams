package clients

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/fgrzl/enumerators"

	"github.com/fgrzl/streams/pkg/grpcservices"
	"github.com/fgrzl/streams/pkg/models"
)

func NewGrpcClient(target string) WoolfClient {
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	internalClient := grpcservices.NewWoolfClient(cc)

	return &grpcClient{
		cc:       cc,
		internal: internalClient,
	}
}

type grpcClient struct {
	cc       *grpc.ClientConn
	internal grpcservices.WoolfClient
}

func (g *grpcClient) CreatePartition(ctx context.Context, args *models.CreatePartitionArgs) (*models.StatusResponse, error) {
	ctx, req := mapCreatePartitionArgs(ctx, args)
	return g.internal.CreatePartition(ctx, req)
}

func (g *grpcClient) GetStatus(ctx context.Context, args *models.GetStatusArgs) (*models.StatusResponse, error) {
	ctx, req := mapGetStatusArgs(ctx, args)
	return g.internal.GetStatus(ctx, req)
}

// ConsumeSpace implements Client.
func (g *grpcClient) ConsumeSpace(ctx context.Context, args *models.ConsumeSpaceArgs) enumerators.Enumerator[*models.EntryEnvelope] {
	ctx, req := mapConsumeSpaceArgs(ctx, args)
	res, err := g.internal.ConsumeSpace(ctx, req)
	if err != nil {
		return enumerators.Error[*models.EntryEnvelope](err)
	}
	return NewServerStreamingClientResponseEnumerator(res)
}

func (g *grpcClient) ConsumePartition(ctx context.Context, args *models.ConsumePartitionArgs) enumerators.Enumerator[*models.EntryEnvelope] {
	ctx, req := mapConsumePartitionArgs(ctx, args)
	res, err := g.internal.ConsumePartition(ctx, req)
	if err != nil {
		return enumerators.Error[*models.EntryEnvelope](err)
	}
	return NewServerStreamingClientResponseEnumerator(res)
}

func (g *grpcClient) GetSpaces(ctx context.Context, args *models.GetSpacesArgs) enumerators.Enumerator[*models.SpaceDescriptor] {
	ctx, req := mapGetSpacesArgs(ctx)
	res, err := g.internal.GetSpaces(ctx, req)
	if err != nil {
		return enumerators.Error[*models.SpaceDescriptor](err)
	}
	return NewServerStreamingClientResponseEnumerator(res)
}

func (g *grpcClient) GetPartitions(ctx context.Context, args *models.GetPartitionsArgs) enumerators.Enumerator[*models.PartitionDescriptor] {
	ctx, req := mapGetPartitionsArgs(ctx, args)
	res, err := g.internal.GetPartitions(ctx, req)
	if err != nil {
		return enumerators.Error[*models.PartitionDescriptor](err)
	}
	return NewServerStreamingClientResponseEnumerator(res)
}

func (g *grpcClient) Peek(ctx context.Context, args *models.PeekArgs) (*models.EntryEnvelope, error) {
	ctx, req := mapPeekArgs(ctx, args)
	return g.internal.Peek(ctx, req)
}

func (g *grpcClient) Produce(ctx context.Context, args *models.ProduceArgs, entries enumerators.Enumerator[*models.Entry]) enumerators.Enumerator[*models.PageDescriptor] {
	ctx = addHeaders(ctx, args.Space, args.Partition)

	stream, err := g.internal.Produce(ctx)
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}

	// Example to send a message
	go func() {
		defer entries.Dispose()
		for entries.MoveNext() {
			entry, err := entries.Current()
			if err != nil {
				return
			}

			if err := stream.Send(entry); err != nil {
				log.Fatalf("Failed to send message: %v", err)
			}
		}
		stream.CloseSend() // Close sending after sending all messages
	}()

	return NewBidiStreamingClientResponseEnumerator(stream)
}

func (g *grpcClient) Merge(ctx context.Context, args *models.MergeArgs) enumerators.Enumerator[*models.PageDescriptor] {
	ctx, req := mapMergeArgs(ctx, args)
	res, err := g.internal.Merge(ctx, req)
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}
	return NewServerStreamingClientResponseEnumerator(res)
}

func (g *grpcClient) Prune(ctx context.Context, args *models.PruneArgs) enumerators.Enumerator[*models.PageDescriptor] {
	ctx, req := mapPruneArgs(ctx, args)
	res, err := g.internal.Prune(ctx, req)
	if err != nil {
		return enumerators.Error[*models.PageDescriptor](err)
	}
	return NewServerStreamingClientResponseEnumerator(res)
}

func (g *grpcClient) Rebuild(ctx context.Context, args *models.RebuildArgs) enumerators.Enumerator[*models.RebuildResponse] {
	ctx, req := mapRebuildArgs(ctx, args)
	res, err := g.internal.Rebuild(ctx, req)
	if err != nil {
		return enumerators.Error[*models.RebuildResponse](err)
	}
	return NewServerStreamingClientResponseEnumerator(res)
}

func (g *grpcClient) Dispose() {
	err := g.cc.Close()
	if err != nil {
		log.Fatalf("Failed to close: %v", err)
	}
}

//
// Internal
//

func addHeaders(ctx context.Context, space string, stream string) context.Context {
	headers := metadata.Pairs(
		"woolf-space", space,
		"woolf-stream", stream,
	)
	return metadata.NewOutgoingContext(ctx, headers)
}

func mapCreatePartitionArgs(ctx context.Context, args *models.CreatePartitionArgs) (context.Context, *models.CreatePartitionRequest) {
	ctx = addHeaders(ctx, args.Space, args.Partition)
	return ctx, &models.CreatePartitionRequest{}
}

func mapGetStatusArgs(ctx context.Context, args *models.GetStatusArgs) (context.Context, *models.GetStatusRequest) {
	ctx = addHeaders(ctx, args.Space, args.Partition)
	return ctx, &models.GetStatusRequest{}
}

func mapGetSpacesArgs(ctx context.Context) (context.Context, *models.GetSpacesRequest) {
	return ctx, &models.GetSpacesRequest{}
}

func mapGetPartitionsArgs(ctx context.Context, args *models.GetPartitionsArgs) (context.Context, *models.GetPartitionsRequest) {
	ctx = addHeaders(ctx, args.Space, "")
	return ctx, &models.GetPartitionsRequest{}
}

func mapPeekArgs(ctx context.Context, args *models.PeekArgs) (context.Context, *models.PeekRequest) {
	ctx = addHeaders(ctx, args.Space, args.Partition)
	return ctx, &models.PeekRequest{}
}

func mapMergeArgs(ctx context.Context, args *models.MergeArgs) (context.Context, *models.MergeRequest) {
	ctx = addHeaders(ctx, args.Space, args.Partition)
	return ctx, &models.MergeRequest{
		Tier: args.Tier,
	}
}

func mapPruneArgs(ctx context.Context, args *models.PruneArgs) (context.Context, *models.PruneRequest) {
	ctx = addHeaders(ctx, args.Space, args.Partition)
	return ctx, &models.PruneRequest{
		Tier: args.Tier,
	}
}

func mapRebuildArgs(ctx context.Context, args *models.RebuildArgs) (context.Context, *models.RebuildRequest) {
	ctx = addHeaders(ctx, args.Space, args.Partition)
	return ctx, &models.RebuildRequest{}
}

func mapConsumeSpaceArgs(ctx context.Context, args *models.ConsumeSpaceArgs) (context.Context, *models.ConsumeSpaceRequest) {
	ctx = addHeaders(ctx, args.Space, "")
	return ctx, &models.ConsumeSpaceRequest{
		Offsets:      args.Offsets,
		MinTimestamp: args.MinTimestamp,
		MaxTimestamp: args.MaxTimestamp,
	}
}

func mapConsumePartitionArgs(ctx context.Context, args *models.ConsumePartitionArgs) (context.Context, *models.ConsumePartitionRequest) {
	ctx = addHeaders(ctx, args.Space, args.Partition)
	return ctx, &models.ConsumePartitionRequest{
		MinSequence:  args.MinSequence,
		MinTimestamp: args.MinTimestamp,
		MaxSequence:  args.MaxSequence,
		MaxTimestamp: args.MaxTimestamp,
	}
}
