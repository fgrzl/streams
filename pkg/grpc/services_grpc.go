package grpc

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/pkg/services"
	"github.com/fgrzl/streams/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// StartServer starts the gRPC server.
func StartServer(ctx context.Context, ready chan struct{}, hosts ...string) {
	var listeners []net.Listener
	grpcServer := grpc.NewServer()

	woolfServer := NewWoolfServer()
	defer woolfServer.Dispose()

	RegisterWoolfServer(grpcServer, woolfServer)

	// Create a channel to handle OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Helper function to ensure correct host format
	normalizeHost := func(host string) string {
		if !strings.Contains(host, ":") {
			return ":" + host
		}
		return host
	}

	// Start a listener for each host
	for _, host := range hosts {
		host = normalizeHost(host) // Ensure host contains ':'

		listener, err := net.Listen("tcp", host)
		if err != nil {
			log.Fatalf("Failed to listen on host %s: %v", host, err)
		}
		listeners = append(listeners, listener)

		go func(listener net.Listener, port string) {
			log.Printf("Server is listening on %s", port)
			if err := grpcServer.Serve(listener); err != nil {
				log.Fatalf("Failed to serve on %s: %v", port, err)
			}
		}(listener, host)
	}

	close(ready)

	// Wait for termination signal or context cancellation
	select {
	case <-stop:
		log.Println("Received termination signal. Shutting down server...")
	case <-ctx.Done():
		log.Println("Context canceled. Shutting down server...")
	}

	// Gracefully stop the server and close all listeners
	grpcServer.GracefulStop()
	for _, listener := range listeners {
		listener.Close()
	}
}

type WoolfGrpcServer interface {
	util.Disposable
	WoolfServer
}

// implementedWolfServer implements the WoolfServer interface.
type implementedWolfServer struct {
	util.Disposable
	service services.Service
	UnimplementedWoolfServer
}

// NewWoolfServer creates a new WoolfServerImpl instance.
func NewWoolfServer() WoolfGrpcServer {
	return &implementedWolfServer{service: services.NewService(services.DefaultServiceOptions)}
}

func (s *implementedWolfServer) Dispose() {
	if s.service != nil {
		s.service.Dispose()
	}
}

// GetStatus handles the GetStatus RPC.
func (s *implementedWolfServer) CreatePartition(ctx context.Context, req *models.CreatePartitionRequest) (*models.StatusResponse, error) {
	descriptor, err := extractPartitionDescriptor(ctx)
	if err != nil {
		return nil, err
	}

	return s.service.CreatePartition(ctx, &models.CreatePartitionArgs{
		Tenant:    descriptor.Tenant,
		Space:     descriptor.Space,
		Partition: descriptor.Partition,
	})
}

// GetStatus handles the GetStatus RPC.
func (s *implementedWolfServer) GetStatus(ctx context.Context, req *models.GetStatusRequest) (*models.StatusResponse, error) {
	descriptor, err := extractPartitionDescriptor(ctx)
	if err != nil {
		return nil, err
	}

	return s.service.GetStatus(ctx, &models.GetStatusArgs{
		Tenant:    descriptor.Tenant,
		Space:     descriptor.Space,
		Partition: descriptor.Partition,
	})
}

// Produce handles the Produce bidirectional streaming RPC.
func (s *implementedWolfServer) Produce(stream grpc.BidiStreamingServer[models.Entry, models.PageDescriptor]) error {
	headers, err := extractPartitionDescriptor(stream.Context())
	if err != nil {
		return err
	}

	entries := enumerators.NewBidiStreamingServerEnumerator(stream)
	defer entries.Dispose()

	args := &models.ProduceArgs{
		Tenant:    headers.Tenant,
		Space:     headers.Space,
		Partition: headers.Partition,
	}

	responses := s.service.Produce(stream.Context(), args, entries)
	defer responses.Dispose()
	for responses.MoveNext() {
		r, err := responses.Current()
		if err != nil {
			return err
		}
		if r == nil {
			continue
		}
		if err := stream.Send(r); err != nil {
			return logAndReturnGRPCError("Failed to send response to stream", err)
		}
	}
	return responses.Err()
}

// Peek handles the Peek RPC.
func (s *implementedWolfServer) Peek(ctx context.Context, req *models.PeekRequest) (*models.EntryEnvelope, error) {
	headers, err := extractPartitionDescriptor(ctx)
	if err != nil {
		return nil, err
	}

	args := &models.PeekArgs{
		Tenant:    headers.Tenant,
		Space:     headers.Space,
		Partition: headers.Partition,
	}

	return s.service.Peek(ctx, args)
}

// ConsumeSpace handles the ConsumeSpace streaming RPC.
func (s *implementedWolfServer) ConsumeSpace(req *models.ConsumeSpaceRequest, stream grpc.ServerStreamingServer[models.EntryEnvelope]) error {
	headers, err := extractSpaceDescriptor(stream.Context())
	if err != nil {
		return err
	}

	args := &models.ConsumeSpaceArgs{
		Tenant:       headers.Tenant,
		Space:        headers.Space,
		Offsets:      req.Offsets,
		MaxTimestamp: req.MaxTimestamp,
	}

	return streamEntries(stream, s.service.ConsumeSpace(stream.Context(), args))
}

// ConsumePartition handles the ConsumePartition streaming RPC.
func (s *implementedWolfServer) ConsumePartition(req *models.ConsumePartitionRequest, stream grpc.ServerStreamingServer[models.EntryEnvelope]) error {
	headers, err := extractPartitionDescriptor(stream.Context())
	if err != nil {
		return err
	}

	args := &models.ConsumePartitionArgs{
		Tenant:       headers.Tenant,
		Space:        headers.Space,
		MinSequence:  req.MinSequence,
		MinTimestamp: req.MinTimestamp,
		MaxSequence:  req.MaxSequence,
		MaxTimestamp: req.MaxTimestamp,
	}

	return streamEntries(stream, s.service.ConsumePartition(stream.Context(), args))
}

// Merge handles the Merge streaming RPC.
func (s *implementedWolfServer) Merge(req *models.MergeRequest, stream grpc.ServerStreamingServer[models.PageDescriptor]) error {
	headers, err := extractPartitionDescriptor(stream.Context())
	if err != nil {
		return err
	}

	args := &models.MergeArgs{
		Tenant:    headers.Tenant,
		Space:     headers.Space,
		Partition: headers.Partition,
		Tier:      req.Tier,
	}

	return streamEntries(stream, s.service.Merge(stream.Context(), args))
}

// Prune handles the Prune streaming RPC.
func (s *implementedWolfServer) Prune(req *models.PruneRequest, stream grpc.ServerStreamingServer[models.PageDescriptor]) error {
	headers, err := extractPartitionDescriptor(stream.Context())
	if err != nil {
		return err
	}

	args := &models.PruneArgs{
		Tenant:    headers.Tenant,
		Space:     headers.Space,
		Partition: headers.Partition,
		Tier:      req.Tier,
	}

	return streamEntries(stream, s.service.Prune(stream.Context(), args))
}

// Rebuild handles the Rebuild streaming RPC.
func (s *implementedWolfServer) Rebuild(req *models.RebuildRequest, stream grpc.ServerStreamingServer[models.RebuildResponse]) error {
	headers, err := extractPartitionDescriptor(stream.Context())
	if err != nil {
		return err
	}

	args := &models.RebuildArgs{
		Tenant:    headers.Tenant,
		Space:     headers.Space,
		Partition: headers.Partition,
		Tier:      req.Tier,
	}

	return streamEntries(stream, s.service.Rebuild(stream.Context(), args))
}

// Helper function for streaming enumerator results to a gRPC stream.
func streamEntries[T any](stream grpc.ServerStreamingServer[T], enumerator enumerators.Enumerator[*T]) error {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		entry, err := enumerator.Current()
		if err != nil {
			return err
		}
		if err := stream.Send(entry); err != nil {
			return logAndReturnGRPCError("Failed to send entry", err)
		}
	}
	return nil
}

// Helper function for structured logging and gRPC error wrapping.
func logAndReturnGRPCError(logMessage string, err error) error {
	log.Printf("%s: %v", logMessage, err)
	return status.Errorf(codes.Internal, "%s: %v", logMessage, err)
}

// Extracts metadata for SpaceDescriptor.
func extractSpaceDescriptor(ctx context.Context) (*models.SpaceDescriptor, error) {
	headers, err := getRequiredHeaders(ctx, "woolf-tenant", "woolf-space")
	if err != nil {
		return nil, err
	}

	return &models.SpaceDescriptor{
		Tenant: headers["woolf-tenant"],
		Space:  headers["woolf-space"],
	}, nil
}

// Extracts metadata for PartitionDescriptor.
func extractPartitionDescriptor(ctx context.Context) (*models.PartitionDescriptor, error) {
	headers, err := getRequiredHeaders(ctx, "woolf-tenant", "woolf-space", "woolf-stream")
	if err != nil {
		return nil, err
	}

	return &models.PartitionDescriptor{
		Tenant:    headers["woolf-tenant"],
		Space:     headers["woolf-space"],
		Partition: headers["woolf-stream"],
	}, nil
}

// Extracts required headers from metadata.
func getRequiredHeaders(ctx context.Context, keys ...string) (map[string]string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing metadata")
	}

	headers := make(map[string]string)
	for _, key := range keys {
		value, err := getHeader(md, key)
		if err != nil {
			return nil, err
		}
		headers[key] = value
	}
	return headers, nil
}

// Gets a single header value from metadata.
func getHeader(md metadata.MD, key string) (string, error) {
	vals, ok := md[key]
	if !ok || len(vals) == 0 {
		return "", status.Errorf(codes.InvalidArgument, "Missing %s", key)
	}
	return vals[0], nil
}
