package rpc

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientPool manages a pool of gRPC client connections.
// Connections are created lazily and cached by address.
// Thread-safe.
type ClientPool struct {
	mu   sync.RWMutex
	conns map[string]*grpc.ClientConn
	opts  []grpc.DialOption
}

// NewClientPool creates a new connection pool with the given dial options.
// Default options include OTel tracing interceptors and insecure transport
// (TLS for inter-node communication is a future enhancement).
func NewClientPool(opts ...grpc.DialOption) *ClientPool {
	defaultOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(NewTracingUnaryClientInterceptor()),
	}

	return &ClientPool{
		conns: make(map[string]*grpc.ClientConn),
		opts:  append(defaultOpts, opts...),
	}
}

// GetConn returns a cached connection or creates a new one for the given address.
func (p *ClientPool) GetConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	// Fast path: read-locked lookup.
	p.mu.RLock()
	if conn, ok := p.conns[addr]; ok {
		p.mu.RUnlock()

		return conn, nil
	}
	p.mu.RUnlock()

	// Slow path: create connection under write lock.
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock.
	if conn, ok := p.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr, p.opts...)
	if err != nil {
		return nil, fmt.Errorf("rpc.ClientPool.GetConn: dial %s: %w", addr, err)
	}

	p.conns[addr] = conn

	return conn, nil
}

// Close closes all cached connections.
func (p *ClientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("rpc.ClientPool.Close: close %s: %w", addr, err)
		}

		delete(p.conns, addr)
	}

	return firstErr
}
