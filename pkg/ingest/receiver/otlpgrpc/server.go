// Package otlpgrpc implements the OTLP/gRPC log receiver.
package otlpgrpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/otlpcommon"
	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip"
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
	Listen       string
	MaxRecvBytes int
	// UnaryInterceptor, when set, runs before every call.
	UnaryInterceptor grpc.UnaryServerInterceptor
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
	opts := []grpc.ServerOption{grpc.MaxRecvMsgSize(cfg.MaxRecvBytes)}
	if cfg.UnaryInterceptor != nil {
		opts = append(opts, grpc.UnaryInterceptor(cfg.UnaryInterceptor))
	}
	r.server = grpc.NewServer(opts...)
	// Only the logs service is registered: an export of traces or metrics
	// fails with Unimplemented.
	logscollector.RegisterLogsServiceServer(r.server, &logsService{receiver: r})
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
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Stop(stopCtx)
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
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	s.receiver.recordRequest("logs", "protobuf", "success", 0)
	return &logscollector.ExportLogsServiceResponse{}, nil
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
