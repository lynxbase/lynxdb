// Package otlpgrpc implements the canonical OTLP/gRPC log receiver.
package otlpgrpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/otlpcommon"
	"github.com/lynxbase/lynxdb/pkg/ingest/staging"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricscollector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracecollector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const DefaultMaxRecvBytes = 16 << 20

type SubmitFunc func(context.Context, []*event.Event) error

type Metrics interface {
	RecordOTLPRequest(signal, encoding, result string, bytes int)
	RecordOTLPRecords(signal, result string, count int)
	RecordDroppedRecords(source, reason string, count int)
}

type Config struct {
	Listen         string
	MaxRecvBytes   int
	ObserveShipper func(ctx context.Context, userAgent, endpoint, remote string, eventCount int)
}

type Receiver struct {
	cfg       Config
	submit    SubmitFunc
	logger    *slog.Logger
	metrics   Metrics
	server    *grpc.Server
	listen    atomic.Value
	ready     chan struct{}
	readyOnce sync.Once
	startErr  atomic.Value
}

func New(cfg Config, submit SubmitFunc, logger *slog.Logger, metrics Metrics) (*Receiver, error) {
	if cfg.Listen == "" {
		return nil, nil
	}
	if _, _, err := net.SplitHostPort(cfg.Listen); err != nil {
		return nil, fmt.Errorf("otlp grpc listen: %w", err)
	}
	if cfg.MaxRecvBytes <= 0 {
		cfg.MaxRecvBytes = DefaultMaxRecvBytes
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &Receiver{
		cfg:     cfg,
		submit:  submit,
		logger:  logger,
		metrics: metrics,
		ready:   make(chan struct{}),
	}
	r.server = grpc.NewServer(grpc.MaxRecvMsgSize(cfg.MaxRecvBytes))
	logscollector.RegisterLogsServiceServer(r.server, &logsService{receiver: r})
	tracecollector.RegisterTraceServiceServer(r.server, traceService{})
	metricscollector.RegisterMetricsServiceServer(r.server, metricsService{})
	return r, nil
}

func (r *Receiver) Start(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", r.cfg.Listen)
	if err != nil {
		r.markReady(err)
		return fmt.Errorf("otlp grpc listen: %w", err)
	}
	r.listen.Store(ln.Addr().String())
	go func() {
		<-ctx.Done()
		r.server.GracefulStop()
	}()
	r.markReady(nil)
	r.logger.Info("OTLP gRPC receiver started", "addr", r.Addr())
	if err := r.server.Serve(ln); err != nil {
		return err
	}
	return nil
}

func (r *Receiver) Stop(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		r.server.Stop()
		return ctx.Err()
	}
}

func (r *Receiver) WaitReady() {
	<-r.ready
}

func (r *Receiver) ReadyError() error {
	if v := r.startErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (r *Receiver) Addr() string {
	if v := r.listen.Load(); v != nil {
		return v.(string)
	}
	return r.cfg.Listen
}

func (r *Receiver) markReady(err error) {
	if err != nil {
		r.startErr.Store(err)
	}
	r.readyOnce.Do(func() { close(r.ready) })
}

type logsService struct {
	logscollector.UnimplementedLogsServiceServer
	receiver *Receiver
}

func (s *logsService) Export(ctx context.Context, req *logscollector.ExportLogsServiceRequest) (*logscollector.ExportLogsServiceResponse, error) {
	total := otlpcommon.CountLogRecords(req.GetResourceLogs())
	events := otlpcommon.LogsToEvents(req.GetResourceLogs())
	dropped := total - len(events)
	s.receiver.recordRecords("logs", "accepted", len(events))
	s.receiver.recordRecords("logs", "dropped", dropped)
	s.receiver.recordDropped("otlp", "empty_body", dropped)
	if len(events) > 0 {
		if err := s.receiver.submit(ctx, events); err != nil {
			s.receiver.recordRequest("logs", "protobuf", "error", 0)
			if errors.Is(err, staging.ErrBufferOverflow) || memgov.IsMemoryExhausted(err) {
				return nil, status.Error(codes.ResourceExhausted, err.Error())
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		if s.receiver.cfg.ObserveShipper != nil {
			s.receiver.cfg.ObserveShipper(ctx, grpcUserAgent(ctx), "/v1/logs", grpcRemoteAddr(ctx), len(events))
		}
	}
	s.receiver.recordRequest("logs", "protobuf", "success", 0)
	return &logscollector.ExportLogsServiceResponse{}, nil
}

func grpcUserAgent(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get("user-agent")
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func grpcRemoteAddr(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	return p.Addr.String()
}

type traceService struct {
	tracecollector.UnimplementedTraceServiceServer
}

func (traceService) Export(context.Context, *tracecollector.ExportTraceServiceRequest) (*tracecollector.ExportTraceServiceResponse, error) {
	return &tracecollector.ExportTraceServiceResponse{}, nil
}

type metricsService struct {
	metricscollector.UnimplementedMetricsServiceServer
}

func (metricsService) Export(context.Context, *metricscollector.ExportMetricsServiceRequest) (*metricscollector.ExportMetricsServiceResponse, error) {
	return &metricscollector.ExportMetricsServiceResponse{}, nil
}

func (r *Receiver) recordRequest(signal, encoding, result string, bytes int) {
	if r.metrics != nil {
		r.metrics.RecordOTLPRequest(signal, encoding, result, bytes)
	}
}

func (r *Receiver) recordRecords(signal, result string, count int) {
	if r.metrics != nil {
		r.metrics.RecordOTLPRecords(signal, result, count)
	}
}

func (r *Receiver) recordDropped(source, reason string, count int) {
	if r.metrics != nil {
		r.metrics.RecordDroppedRecords(source, reason, count)
	}
}
