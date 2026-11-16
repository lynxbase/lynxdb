package colstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// benchServer puts a store behind HTTP so that the load generators and
// query clients used for other systems can drive it the same way.
type benchServer struct {
	t        testing.TB
	store    *Store
	buffers  sync.Pool
	accepted atomic.Int64
	rejected atomic.Int64
	stop     chan struct{}
	once     sync.Once
}

const serverChunk = 4 << 20

// insert takes a body of NDJSON in pieces that end at line ends, so that a
// large request is never held whole.
func (s *benchServer) insert(w http.ResponseWriter, r *http.Request) {
	bufp := s.buffers.Get().(*[]byte)
	defer s.buffers.Put(bufp)
	buf := (*bufp)[:0]
	var lines, rejected int64
	flush := func(data []byte) error {
		lines += int64(bytes.Count(data, []byte{'\n'}))
		if data[len(data)-1] != '\n' {
			// The last line of a body may end without a line break.
			lines++
		}
		n, err := s.store.Append(data)
		rejected += n

		return err
	}
	for {
		n, err := r.Body.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if len(buf) == cap(buf) || err != nil {
			cut := len(buf)
			if err == nil {
				if cut = bytes.LastIndexByte(buf, '\n') + 1; cut == 0 {
					// One line longer than the buffer.
					grown := make([]byte, len(buf), 2*cap(buf))
					copy(grown, buf)
					buf = grown

					continue
				}
			}
			if cut > 0 {
				if ferr := flush(buf[:cut]); ferr != nil {
					http.Error(w, ferr.Error(), http.StatusInternalServerError)

					return
				}
			}
			buf = buf[:copy(buf, buf[cut:])]
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}
	}
	*bufp = buf[:0]
	if r.URL.Query().Get("sync") != "" {
		if err := s.store.Sync(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
	}
	s.accepted.Add(lines - rejected)
	s.rejected.Add(rejected)
	fmt.Fprintf(w, `{"accepted":%d,"rejected":%d}`+"\n", lines-rejected, rejected)
}

// stats reports what the table holds and what is still being merged.
func (s *benchServer) stats(w http.ResponseWriter, _ *http.Request) {
	snap := s.store.Table().Snapshot()
	defer snap.Release()
	var rows uint64
	var size int64
	runs := map[uint64]struct{}{}
	for _, h := range snap.Parts() {
		rows += h.Rows
		size += int64(h.Size())
		runs[h.Run] = struct{}{}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"rows": rows, "bytes": size, "parts": len(snap.Parts()), "runs": len(runs),
		"accepted": s.accepted.Load(), "rejected": s.rejected.Load(),
	})
}

// query runs the benchmark query named by q and returns its answer size and
// the time the server spent on it.
func (s *benchServer) query(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("q")
	cores := runtime.GOMAXPROCS(0)
	snap := s.store.Table().Snapshot()
	defer snap.Release()
	paths := []string{"kind", "commit.operation", "commit.collection"}
	created := snap.Narrow(KeyFilter{Paths: paths[:2], Values: [][]byte{[]byte("commit"), []byte("create")}})
	defer created.Release()
	posts := snap.Narrow(KeyFilter{Paths: paths, Values: [][]byte{[]byte("commit"), []byte("create"), []byte("app.bsky.feed.post")}})
	defer posts.Release()

	cpuBefore, begin := cpuTime(), time.Now()
	var answer any
	var err error
	switch name {
	case "Q1":
		answer, err = queryTopEvents(snap, cores)
	case "Q2":
		answer, err = queryEventsWithUsers(created, cores, false)
	case "Q3":
		answer, err = queryEventsByHour(created, cores)
	case "Q4", "Q5":
		var first, longest []userSpan
		first, longest, err = queryPostTimes(posts, cores, 3)
		answer = map[string]any{"first": first, "longest": longest}
	default:
		searches := benchSearches
		if strings.HasPrefix(name, "T") {
			searches, _ = benchWindows(s.t, snap)
		}
		found := false
		for _, q := range searches {
			if strings.HasPrefix(q.name, name+" ") {
				answer, err = q.run(snap, cores)
				found = true

				break
			}
		}
		if !found {
			http.Error(w, "unknown query "+name, http.StatusNotFound)

			return
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
	w.Header().Set("X-Query-Seconds", fmt.Sprintf("%.6f", time.Since(begin).Seconds()))
	w.Header().Set("X-Cpu-Seconds", fmt.Sprintf("%.6f", (cpuTime()-cpuBefore).Seconds()))
	_ = json.NewEncoder(w).Encode(map[string]any{"query": name, "answer": fmt.Sprint(answer)})
}

func (s *benchServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /insert", s.insert)
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("GET /query", s.query)
	step := func(fn func() error) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			begin := time.Now()
			if err := fn(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)

				return
			}
			fmt.Fprintf(w, `{"seconds":%.3f}`+"\n", time.Since(begin).Seconds())
		}
	}
	mux.HandleFunc("POST /sync", step(s.store.Sync))
	mux.HandleFunc("POST /settle", step(func() error {
		if err := s.store.Sync(); err != nil {
			return err
		}

		return s.store.Settle()
	}))
	mux.HandleFunc("POST /compact", step(s.store.Compact))
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, _ *http.Request) {
		s.once.Do(func() { close(s.stop) })
		fmt.Fprintln(w, "{}")
	})

	return mux
}

func newBenchServer(t testing.TB, store *Store) *benchServer {
	s := &benchServer{t: t, store: store, stop: make(chan struct{})}
	s.buffers.New = func() any {
		b := make([]byte, 0, serverChunk)

		return &b
	}

	return s
}

// TestJSONBenchServer serves the store in LYNXDB_BENCH_DIR on the address
// LYNXDB_BENCH_LISTEN until POST /shutdown: POST /insert takes NDJSON
// (?sync=1 answers once it is durable), POST /sync, /settle and /compact
// wait for those states, GET /stats describes the table, and GET /query?q=
// runs a benchmark query by its short name (Q1 to Q5, S1 to S7, T1 to T4).
// The store is configured by the settings of TestJSONBenchIngest.
func TestJSONBenchServer(t *testing.T) {
	addr, dir := os.Getenv("LYNXDB_BENCH_LISTEN"), os.Getenv("LYNXDB_BENCH_DIR")
	if addr == "" || dir == "" {
		t.Skip("set LYNXDB_BENCH_LISTEN and LYNXDB_BENCH_DIR")
	}
	store, err := OpenStore(dir, ingestOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := newBenchServer(t, store)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: s.handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(listener) }()
	t.Logf("serving %s on %s", dir, listener.Addr())
	<-s.stop
	if err := server.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
}

// TestBenchServerTakesInsertsAndAnswersQueries drives the handler the way a
// load generator does.
func TestBenchServerTakesInsertsAndAnswersQueries(t *testing.T) {
	store, err := OpenStore(t.TempDir(), StoreOptions{SortKey: []string{"svc"}, FlushBytes: 2 << 20, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := newBenchServer(t, store)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: s.handler(), ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	base := "http://" + listener.Addr().String()

	// A body several times the read chunk, with a line that is not JSON.
	const docs = 250000
	body := append(storeDocs(0, docs), "oops"...)
	if len(body) < 2*serverChunk {
		t.Fatalf("the body of %d bytes does not span several chunks", len(body))
	}
	resp, err := http.Post(base+"/insert?sync=1", "application/x-ndjson", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var result struct{ Accepted, Rejected int64 }
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if result.Accepted != docs || result.Rejected != 1 {
		t.Fatalf("insert answered %+v", result)
	}
	if rows, sum := rowsAndSumOf(t, store.Table()); rows != docs || sum != (docs-1)*docs/2 {
		t.Fatalf("%d rows, sum %d after a synced insert", rows, sum)
	}
	// An empty body and one long line are both whole requests.
	long := `{"n":0,"svc":"s00","pad":"` + strings.Repeat("x", serverChunk+100) + `"}`
	for i, payload := range []string{"", long} {
		resp, err := http.Post(base+"/insert", "application/x-ndjson", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || result.Accepted != int64(i) || result.Rejected != 0 {
			t.Fatalf("insert of %d bytes answered %+v: %v", len(payload), result, err)
		}
		resp.Body.Close()
	}
	for _, path := range []string{"/settle", "/compact"} {
		resp, err := http.Post(base+path, "", nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %v %v", path, err, resp)
		}
		resp.Body.Close()
	}
	resp, err = http.Get(base + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var stats struct{ Rows, Runs, Accepted, Rejected int64 }
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if stats.Rows != docs+1 || stats.Runs != 1 || stats.Accepted != docs+1 || stats.Rejected != 1 {
		t.Fatalf("stats %+v", stats)
	}
	resp, err = http.Get(base + "/query?q=S5")
	if err != nil || resp.StatusCode != http.StatusOK || resp.Header.Get("X-Query-Seconds") == "" {
		t.Fatalf("query: %v %v", err, resp)
	}
	resp.Body.Close()
	if resp, err = http.Get(base + "/query?q=nope"); err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown query: %v %v", err, resp)
	}
	resp.Body.Close()
}
