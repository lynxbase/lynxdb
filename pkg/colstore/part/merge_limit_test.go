package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// heldSink takes the first write and holds the second until released.
type heldSink struct {
	buf     bytes.Buffer
	writes  int
	reached chan struct{}
	release chan struct{}
}

func (s *heldSink) Write(p []byte) (int, error) {
	if s.writes++; s.writes == 2 {
		close(s.reached)
		<-s.release
	}

	return s.buf.Write(p)
}

// TestALoweredMergeLeavesTheRestOfItsRangeToAnother requires that a merge
// whose upper bound is lowered while it runs, and a merge of the rest of
// its range, hold together the rows of the whole merge, and that a bound the
// merge has passed is refused.
func TestALoweredMergeLeavesTheRestOfItsRangeToAnother(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	services := []string{"api", "billing", "cache", "db", "edge", "front", "gateway", "ingest"}
	var srcs []*Part
	for src := 0; src < 4; src++ {
		var docs []string
		var times []int64
		for seq := 0; seq < 3000; seq++ {
			docs = append(docs, fmt.Sprintf(`{"src":%d,"seq":%d,"svc":"%s","level":%d}`, src, seq, services[r.Intn(len(services))], r.Intn(40)))
			times = append(times, int64(r.Intn(100)))
		}
		srcs = append(srcs, mergeParts(t, []*Part{orderedPart(t, docs, times)}, 256))
	}
	whole := decodeRows(t, mergeParts(t, srcs, 128))
	cuts := SplitKeys(srcs, 4)
	if len(cuts) != 3 {
		t.Fatalf("%d cuts, want 3", len(cuts))
	}

	sink := &heldSink{reached: make(chan struct{}), release: make(chan struct{})}
	limit := &RangeLimit{}
	w := NewWriter(sink, Options{Ordered: true, SortKey: mergeKey})
	merged := make(chan error, 1)
	go func() { merged <- MergeRangeLimit(w, srcs, 128, nil, nil, limit) }()
	<-sink.reached
	lowered := make(chan bool, 1)
	go func() { lowered <- limit.Lower(cuts[2]) }()
	for !limit.pending.Load() {
		time.Sleep(time.Millisecond)
	}
	close(sink.release)
	if !<-lowered {
		t.Fatal("the merge refused a bound it had not reached")
	}
	if err := <-merged; err != nil {
		t.Fatal(err)
	}
	if limit.Lower(cuts[1]) {
		t.Fatal("a merge that ended took a bound")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	head, err := Open(sink.buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got := limit.Rows(); got != int64(head.Rows) {
		t.Fatalf("the limit counts %d rows written, the part holds %d", got, head.Rows)
	}
	joined := append(decodeRows(t, head), decodeRows(t, mergeRange(t, srcs, 128, cuts[2], nil))...)
	if len(joined) != len(whole) {
		t.Fatalf("the two merges hold %d rows, the whole merge %d", len(joined), len(whole))
	}
	for i := range whole {
		if joined[i].text != whole[i].text || joined[i].time != whole[i].time {
			t.Fatalf("row %d differs from the whole merge:\n%s\n%s", i, joined[i].text, whole[i].text)
		}
	}
}
