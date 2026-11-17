package syslog

import (
	"bufio"
	"errors"
	"io"
	"net"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func (r *Receiver) serveTCP(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		cfg := r.currentConfig()
		if cfg.TCPMaxConns > 0 && r.activeConn.Load() >= int64(cfg.TCPMaxConns) {
			r.metrics.IncDropped("tcp", "conn_limit")
			_ = conn.SetDeadline(time.Now().Add(100 * time.Millisecond))
			_ = conn.Close()
			continue
		}
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.handleTCPConn(conn)
		}()
	}
}

func (r *Receiver) handleTCPConn(conn net.Conn) {
	active := r.activeConn.Add(1)
	r.metrics.SetActiveConnections(active)
	defer func() {
		_ = conn.Close()
		active := r.activeConn.Add(-1)
		r.metrics.SetActiveConnections(active)
	}()

	cfg := r.currentConfig()
	reader := bufio.NewReaderSize(conn, cfg.MaxMessageBytes+64)
	fr, err := newFrameReader(reader, cfg.Framing, cfg.Trailer, cfg.MaxMessageBytes)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			r.logger.Warn("syslog TCP framing detection failed", "remote", conn.RemoteAddr(), "error", err)
		}
		return
	}

	batch := make([]*event.Event, 0, cfg.BatchSize)
	timer := newStoppedTimer()
	defer timer.Stop()

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		err := r.processBatch(batch)
		if err != nil {
			r.metrics.IncDropped("tcp", dropReason(err))
			r.logger.Warn("syslog TCP ingest failed", "remote", conn.RemoteAddr(), "error", err, "batch_size", len(batch))
			batch = batch[:0]
			stopTimer(timer)
			return false
		}
		batch = batch[:0]
		stopTimer(timer)
		return true
	}

	for {
		cfg = r.currentConfig()
		deadline := time.Time{}
		if cfg.TCPIdleTimeout.Duration() > 0 {
			deadline = time.Now().Add(cfg.TCPIdleTimeout.Duration())
		}
		if len(batch) > 0 && cfg.BatchTimeout.Duration() > 0 {
			batchDeadline := time.Now().Add(cfg.BatchTimeout.Duration())
			if deadline.IsZero() || batchDeadline.Before(deadline) {
				deadline = batchDeadline
			}
		}
		if !deadline.IsZero() {
			_ = conn.SetReadDeadline(deadline)
		}
		msg, err := fr.Next()
		if err != nil {
			if errors.Is(err, errFrameTooLarge) {
				r.metrics.IncDropped("tcp", "toolarge")
				continue
			}
			if isTimeout(err) && len(batch) > 0 {
				if !flush() {
					return
				}
				continue
			}
			if !errors.Is(err, io.EOF) && !isTimeout(err) && !isClosedNetworkError(err) {
				r.logger.Debug("syslog TCP connection closed", "remote", conn.RemoteAddr(), "error", err)
			}
			flush()
			return
		}

		source := ""
		if cfg.UsePeerAsSource && conn.RemoteAddr() != nil {
			source = "tcp://" + conn.RemoteAddr().String()
		}
		e, dialect := newParser(cfg).parse(msg, source, time.Now())
		r.metrics.IncReceived("tcp", dialect)
		if e.ParseError {
			r.metrics.IncParseError(dialect)
		}
		if len(batch) == 0 {
			resetTimer(timer, cfg.BatchTimeout.Duration())
		}
		batch = append(batch, e)
		if len(batch) >= cfg.BatchSize && !flush() {
			return
		}

		select {
		case <-timer.C:
			if !flush() {
				return
			}
		default:
		}
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func newStoppedTimer() *time.Timer {
	timer := time.NewTimer(time.Hour)
	stopTimer(timer)
	return timer
}

func resetTimer(timer *time.Timer, d time.Duration) {
	stopTimer(timer)
	if d > 0 {
		timer.Reset(d)
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
