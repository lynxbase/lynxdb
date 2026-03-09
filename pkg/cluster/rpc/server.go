// Package rpc provides the gRPC transport layer for LynxDB cluster communication.
package rpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// Server wraps a gRPC server with LynxDB-specific lifecycle management.
type Server struct {
	grpc   *grpc.Server
	addr   string
	logger *slog.Logger
}

// NewServer creates a new gRPC server listening on the given address.
// The server is created with OTel tracing interceptors and any additional
// options provided by the caller.
func NewServer(addr string, logger *slog.Logger, opts ...grpc.ServerOption) *Server {
	// Prepend OTel interceptors so they wrap all registered services.
	// The custom tracing interceptor enriches otelgrpc spans with LynxDB
	// business attributes (shard_id, node_id) extracted from request messages.
	defaultOpts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(NewTracingUnaryServerInterceptor()),
	}
	allOpts := append(defaultOpts, opts...)

	return &Server{
		grpc:   grpc.NewServer(allOpts...),
		addr:   addr,
		logger: logger,
	}
}

// GRPCServer returns the underlying *grpc.Server for service registration.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpc
}

// Start begins listening and serving gRPC requests.
// Blocks until the context is canceled or an error occurs.
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("rpc.Server.Start: listen %s: %w", s.addr, err)
	}

	s.logger.Info("gRPC server started", "addr", ln.Addr().String())

	// Graceful shutdown on context cancellation.
	go func() {
		<-ctx.Done()
		s.logger.Info("gRPC server shutting down")
		s.grpc.GracefulStop()
	}()

	if err := s.grpc.Serve(ln); err != nil {
		return fmt.Errorf("rpc.Server.Start: serve: %w", err)
	}

	return nil
}

// Stop initiates a graceful shutdown of the gRPC server.
func (s *Server) Stop() {
	s.grpc.GracefulStop()
}
