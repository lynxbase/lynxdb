package rpc

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/lynxbase/lynxdb/pkg/cluster/tracing"
)

// NewTracingUnaryServerInterceptor returns a gRPC unary server interceptor that
// enriches the otelgrpc-created span with LynxDB business attributes extracted
// from the request message. It does not replace otelgrpc — it layers on top.
func NewTracingUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() {
			enrichSpanFromRequest(span, req)
		}

		return handler(ctx, req)
	}
}

// NewTracingUnaryClientInterceptor returns a gRPC unary client interceptor that
// enriches the otelgrpc-created span with target node context.
func NewTracingUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() {
			span.SetAttributes(
				attribute.String("rpc.target", cc.Target()),
			)
			enrichSpanFromRequest(span, req)
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// enrichSpanFromRequest extracts known LynxDB attributes from request messages
// using interface assertions. This avoids reflection and keeps the hot path fast.
func enrichSpanFromRequest(span trace.Span, req interface{}) {
	type shardIDer interface{ GetShardId() string }
	type nodeIDer interface{ GetNodeId() string }

	if s, ok := req.(shardIDer); ok {
		if id := s.GetShardId(); id != "" {
			span.SetAttributes(attribute.String(tracing.AttrShardID, id))
		}
	}

	if n, ok := req.(nodeIDer); ok {
		if id := n.GetNodeId(); id != "" {
			span.SetAttributes(attribute.String(tracing.AttrNodeID, id))
		}
	}
}
