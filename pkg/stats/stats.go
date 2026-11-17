// Package stats holds the execution statistics of a query and formats them
// for the CLI.
package stats

import "time"

// QueryStats holds the statistics of one query run.
type QueryStats struct {
	// TotalDuration is end-to-end from query parse start to final result written.
	TotalDuration time.Duration
	// ParseDuration is the time spent compiling the query to an optimized plan.
	ParseDuration time.Duration
	// ExecDuration is the time spent running the compiled query.
	ExecDuration time.Duration
	// SerializeDuration is the time spent formatting results (JSON/table/CSV).
	SerializeDuration time.Duration
	// ScannedRows is the number of rows the query ran over.
	ScannedRows int64
	// ResultRows is the number of rows the query returned.
	ResultRows int64
}

// FromMeta builds QueryStats from the took_ms and row counts of a REST API
// response.
func FromMeta(tookMS float64, scanned, rowsReturned int64) *QueryStats {
	return &QueryStats{
		ExecDuration: timeDurFromMS(tookMS),
		ScannedRows:  scanned,
		ResultRows:   rowsReturned,
	}
}

// timeDurFromMS converts milliseconds (float64) to time.Duration.
func timeDurFromMS(ms float64) time.Duration {
	return time.Duration(ms * float64(time.Millisecond))
}

// ScanRateRowsPerSec returns the scan throughput in rows per second.
func (s *QueryStats) ScanRateRowsPerSec() float64 {
	if s.ExecDuration <= 0 {
		return 0
	}

	return float64(s.ScannedRows) / s.ExecDuration.Seconds()
}
