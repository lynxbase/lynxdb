package rest

import (
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// histogramIntervals are the bucket widths a histogram snaps to, with their
// PPL span literals.
var histogramIntervals = []struct {
	d    time.Duration
	span string
}{
	{time.Second, "1s"},
	{5 * time.Second, "5s"},
	{30 * time.Second, "30s"},
	{time.Minute, "1m"},
	{5 * time.Minute, "5m"},
	{15 * time.Minute, "15m"},
	{time.Hour, "1h"},
	{6 * time.Hour, "6h"},
	{24 * time.Hour, "1d"},
}

// snapInterval returns the standard bucket width nearest to d and its span
// literal; a d above a day becomes a whole number of days, at least d, so
// that a long range keeps about the buckets it asks for.
func snapInterval(d time.Duration) (time.Duration, string) {
	const day = 24 * time.Hour
	if d > day {
		days := (d + day - 1) / day
		return days * day, fmt.Sprintf("%dd", days)
	}
	best := histogramIntervals[0]
	bestDiff := math.Abs(float64(d - best.d))
	for _, iv := range histogramIntervals[1:] {
		if diff := math.Abs(float64(d - iv.d)); diff < bestDiff {
			best, bestDiff = iv, diff
		}
	}

	return best.d, best.span
}

// handleHistogram counts the events of an index in buckets of time over
// [from, to), by default the last hour; buckets without events count zero.
func (s *Server) handleHistogram(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	bucketCount := parseIntParam(r, "buckets", 60)
	if bucketCount <= 0 {
		bucketCount = 60
	}
	if bucketCount > 1000 {
		bucketCount = 1000
	}
	tr, err := timerange.ParseRange(
		firstNonEmpty(r.URL.Query().Get("from"), "-1h"), firstNonEmpty(r.URL.Query().Get("to"), "now"), start)
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		return
	}
	total := tr.Latest.Sub(tr.Earliest)
	if total <= 0 {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "from must be before to")
		return
	}
	index, err := pplName(firstNonEmpty(r.URL.Query().Get("index"), "*"))
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		return
	}
	interval, span := snapInterval(total / time.Duration(bucketCount))

	src := fmt.Sprintf("source=%s | stats count() as count by span(_time, %s) as _time", index, span)
	res, err := s.runInternalQuery(r, src, tr.Earliest, tr.Latest)
	if err != nil {
		respondQueryFailure(w, err)
		return
	}
	counts := map[int64]int64{}
	sum := int64(0)
	for _, row := range res.Maps() {
		t, ok := row["_time"]
		c, okc := row["count"]
		if !ok || !okc {
			continue
		}
		counts[t.AsTimestamp().UnixNano()] += c.AsInt()
		sum += c.AsInt()
	}

	// span() starts its buckets at multiples of the interval since the epoch.
	first := tr.Earliest.UnixNano()
	first -= first % int64(interval)
	if first > tr.Earliest.UnixNano() {
		first -= int64(interval)
	}
	buckets := make([]map[string]interface{}, 0, 2*bucketCount+1)
	for t := time.Unix(0, first); t.Before(tr.Latest); t = t.Add(interval) {
		buckets = append(buckets, map[string]interface{}{
			"time":  t.UTC().Format(time.RFC3339),
			"count": counts[t.UnixNano()],
		})
	}

	respondData(w, http.StatusOK, map[string]interface{}{
		"interval": interval.String(),
		"buckets":  buckets,
		"total":    sum,
	}, WithTook(time.Since(start)))
}
