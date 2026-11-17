package usecases

import (
	"context"
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/server"
)

// Histogram computes event count buckets over a time range from segment
// metadata, without loading events.
func (s *QueryService) Histogram(ctx context.Context, req HistogramRequest) (*HistogramResult, error) {
	fromTime, toTime, interval, err := histogramRange(req)
	if err != nil {
		return nil, err
	}
	bucketCount := histogramBuckets(req)
	srvBuckets := make([]server.HistogramBucket, bucketCount)
	for i := 0; i < bucketCount; i++ {
		srvBuckets[i] = server.HistogramBucket{
			Time: fromTime.Add(time.Duration(i) * interval),
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	total, err := s.engine.HistogramFromMetadata(ctx, req.Index, fromTime, toTime, interval, srvBuckets)
	if err != nil {
		return nil, err
	}
	buckets := make([]HistogramBucket, len(srvBuckets))
	for i, b := range srvBuckets {
		buckets[i] = HistogramBucket{Time: b.Time, Count: b.Count}
	}
	return &HistogramResult{Interval: interval.String(), Buckets: buckets, Total: total}, nil
}

// GroupedHistogram computes event count buckets grouped by a field value.
func (s *QueryService) GroupedHistogram(ctx context.Context, req HistogramRequest) (*GroupedHistogramResult, error) {
	fromTime, toTime, interval, err := histogramRange(req)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	srvBuckets, total, err := s.engine.HistogramByFieldFromMetadata(
		ctx, req.Index, fromTime, toTime, interval, req.GroupBy, histogramBuckets(req),
	)
	if err != nil {
		return nil, err
	}
	buckets := make([]GroupedHistogramBucket, len(srvBuckets))
	for i, b := range srvBuckets {
		buckets[i] = GroupedHistogramBucket{Time: b.Time, Counts: b.Counts}
	}
	return &GroupedHistogramResult{Interval: interval.String(), Buckets: buckets, Total: total}, nil
}

func histogramBuckets(req HistogramRequest) int {
	if req.Buckets <= 0 {
		return 60
	}
	return req.Buckets
}

func histogramRange(req HistogramRequest) (from, to time.Time, interval time.Duration, err error) {
	now := time.Now()
	fromStr, toStr := req.From, req.To
	if fromStr == "" {
		fromStr = "-1h"
	}
	if toStr == "" {
		toStr = "now"
	}
	if from, err = ParseTimeParam(fromStr, now); err != nil {
		return from, to, 0, fmt.Errorf("%w: %w", ErrInvalidFrom, err)
	}
	if to, err = ParseTimeParam(toStr, now); err != nil {
		return from, to, 0, fmt.Errorf("%w: %w", ErrInvalidTo, err)
	}
	total := to.Sub(from)
	if total <= 0 {
		return from, to, 0, ErrFromBeforeTo
	}
	return from, to, SnapInterval(time.Duration(total.Nanoseconds() / int64(histogramBuckets(req)))), nil
}

// FieldValues returns the top values for a given field name.
func (s *QueryService) FieldValues(ctx context.Context, req FieldValuesRequest) (*FieldValuesResult, error) {
	now := time.Now()
	var from, to time.Time
	if req.From != "" {
		if t, err := ParseTimeParam(req.From, now); err == nil {
			from = t
		}
	}
	if req.To != "" {
		if t, err := ParseTimeParam(req.To, now); err == nil {
			to = t
		}
	}
	srvResult, err := s.engine.FieldValuesFromMetadata(ctx, req.FieldName, req.Index, from, to, req.Limit)
	if err != nil {
		return nil, err
	}
	values := make([]FieldValue, len(srvResult.Values))
	for i, v := range srvResult.Values {
		values[i] = FieldValue{Value: v.Value, Count: v.Count, Percent: v.Percent}
	}
	return &FieldValuesResult{
		Field:       srvResult.Field,
		Values:      values,
		UniqueCount: srvResult.UniqueCount,
		TotalCount:  srvResult.TotalCount,
	}, nil
}

// ListSources returns all distinct event sources.
func (s *QueryService) ListSources(ctx context.Context) (*SourcesResult, error) {
	srvResult, err := s.engine.ListSourcesFromMetadata(ctx, "", time.Time{}, time.Time{})
	if err != nil {
		return nil, err
	}
	result := make([]SourceInfo, len(srvResult.Sources))
	for i, si := range srvResult.Sources {
		result[i] = SourceInfo{
			Name:       si.Name,
			EventCount: si.EventCount,
			FirstEvent: si.FirstEvent,
			LastEvent:  si.LastEvent,
		}
	}
	return &SourcesResult{Sources: result}, nil
}
