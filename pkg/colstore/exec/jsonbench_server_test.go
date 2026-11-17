package exec_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/flow"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// flowServer puts a store behind HTTP and answers LynxFlow queries through
// the executor, so that the load generators and query clients used for other
// systems can drive the whole path from query text to answer.
type flowServer struct {
	t        testing.TB
	store    *colstore.Store
	buffers  sync.Pool
	accepted atomic.Int64
	rejected atomic.Int64
	stop     chan struct{}
	once     sync.Once

	named     sync.Once
	plans     map[string]benchPlan
	plansFail error
}

const (
	flowServerChunk = 4 << 20
	// answerRows bounds the rows of an answer that a response spells out.
	answerRows = 20
)

func newFlowServer(t testing.TB, store *colstore.Store) *flowServer {
	s := &flowServer{t: t, store: store, stop: make(chan struct{})}
	s.buffers.New = func() any {
		b := make([]byte, 0, flowServerChunk)

		return &b
	}

	return s
}

// insert takes a body of NDJSON in pieces that end at line ends, so that a
// large request is never held whole.
func (s *flowServer) insert(w http.ResponseWriter, r *http.Request) {
	bufp := s.buffers.Get().(*[]byte)
	defer s.buffers.Put(bufp)
	buf := (*bufp)[:0]
	var lines, rejected int64
	flush := func(data []byte) error {
		lines += int64(bytes.Count(data, []byte{'\n'}))
		if data[len(data)-1] != '\n' {
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

func (s *flowServer) stats(w http.ResponseWriter, _ *http.Request) {
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

// benchPlan returns the benchmark plan with the short name: the first word
// of its name, followed by i when it disregards letter case. The time windows
// are those of the table at the first call.
func (s *flowServer) benchPlan(snap *colstore.Snapshot, short string) (benchPlan, bool, error) {
	s.named.Do(func() {
		win, err := windowsFromEnv()
		if err != nil {
			s.plansFail = err

			return
		}
		if win == nil {
			found := windowsOf(s.t, snap)
			win = &found
		}
		s.plans = map[string]benchPlan{}
		for _, q := range benchPlans(*win) {
			name := strings.Fields(q.name)[0]
			if strings.HasPrefix(name, "S") && strings.HasSuffix(q.name, "any case") {
				name += "i"
			}
			s.plans[name] = q
		}
	})
	q, ok := s.plans[short]

	return q, ok, s.plansFail
}

// windowsFromEnv returns the time windows when the environment gives both,
// checked here because a handler must not end the test.
func windowsFromEnv() (*benchWindows, error) {
	var bounds [4]int64
	for i, env := range []string{"LYNXDB_BENCH_T10_US", "LYNXDB_BENCH_T60_US"} {
		v := os.Getenv(env)
		if v == "" {
			return nil, nil
		}
		a, b, ok := strings.Cut(v, ",")
		from, err1 := strconv.ParseInt(a, 10, 64)
		to, err2 := strconv.ParseInt(b, 10, 64)
		if !ok || err1 != nil || err2 != nil || to <= from {
			return nil, fmt.Errorf("%s=%q is not lo,hi in microseconds", env, v)
		}
		bounds[2*i], bounds[2*i+1] = from*1000, to*1000-1
	}

	return &benchWindows{tenLo: bounds[0], tenHi: bounds[1], hourLo: bounds[2], hourHi: bounds[3]}, nil
}

func renderValue(v exec.Value) string {
	switch v.Kind {
	case vec.KindInt:
		return strconv.FormatInt(v.Int, 10)
	case vec.KindFloat:
		return strconv.FormatFloat(v.Float, 'g', -1, 64)
	case vec.KindBool:
		return strconv.FormatBool(v.Bool)
	case vec.KindString, vec.KindVariant:
		return string(v.Bytes)
	}

	return "null"
}

// query answers flow=, a query in LynxFlow, or q=, a benchmark query by its
// short name, which runs as its plan or with form=text as its LynxFlow text.
func (s *flowServer) query(w http.ResponseWriter, r *http.Request) {
	args := r.URL.Query()
	workers := 0
	if v := args.Get("workers"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			http.Error(w, "workers must be a positive number", http.StatusBadRequest)

			return
		}
		workers = n
	}
	snap := s.store.Table().Snapshot()
	defer snap.Release()

	text := args.Get("flow")
	var plan exec.Node
	if name := args.Get("q"); name != "" {
		q, ok, err := s.benchPlan(snap, name)
		switch {
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		case !ok:
			http.Error(w, "unknown query "+name, http.StatusNotFound)

			return
		case args.Get("form") != "text":
			plan = q.plan
		case q.text == "":
			http.Error(w, name+" has no text form", http.StatusNotFound)

			return
		default:
			text = q.text
		}
	}
	cpuBefore, begin := cpuTime(), time.Now()
	if plan == nil {
		var err error
		if plan, err = flow.Compile(text, flow.Options{}); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}
	}
	res, err := exec.Run(r.Context(), snap, plan, exec.Options{Workers: workers})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
	w.Header().Set("X-Query-Seconds", fmt.Sprintf("%.6f", time.Since(begin).Seconds()))
	w.Header().Set("X-Cpu-Seconds", fmt.Sprintf("%.6f", (cpuTime()-cpuBefore).Seconds()))
	answer := make([][]string, 0, min(res.Rows, answerRows))
	var row []exec.Value
	for i := 0; i < res.Rows && i < answerRows; i++ {
		row = res.Row(i, row[:0])
		cells := make([]string, len(row))
		for j, v := range row {
			cells[j] = renderValue(v)
		}
		answer = append(answer, cells)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"rows": res.Rows, "answer": answer, "in_key_order": res.Stats.Ordered, "blocks": res.Stats.Blocks,
		"counted_from_index": res.Stats.CountedByIndex, "hashed_for": res.Stats.HashedFor,
	})
}

func (s *flowServer) handler() http.Handler {
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
	mux.HandleFunc("POST /evict", step(func() error {
		s.store.ReleasePages()

		return nil
	}))
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, _ *http.Request) {
		s.once.Do(func() { close(s.stop) })
		fmt.Fprintln(w, "{}")
	})

	return mux
}

// flowStoreOptions configures the store of the benchmark: the benchmark's
// sort key unless LYNXDB_BENCH_SORT names another ("none" for arrival order),
// and time partitions of LYNXDB_BENCH_PARTITION when set.
func flowStoreOptions(t testing.TB) colstore.StoreOptions {
	t.Helper()
	opts := colstore.StoreOptions{TimeField: benchTimeField, BlockRows: benchBlockRows, SortKey: benchSortKey}
	switch v := os.Getenv("LYNXDB_BENCH_SORT"); v {
	case "", "default":
	case "none":
		opts.SortKey = nil
	default:
		opts.SortKey = strings.Split(v, ",")
	}
	if v := os.Getenv("LYNXDB_BENCH_PARTITION"); v != "" {
		width, err := time.ParseDuration(v)
		if err != nil || width <= 0 {
			t.Fatalf("LYNXDB_BENCH_PARTITION=%q is not a positive duration", v)
		}
		opts.Partition = width
	}

	return opts
}

// TestJSONBenchFlowServer serves the store in LYNXDB_BENCH_DIR on the address
// LYNXDB_BENCH_LISTEN until POST /shutdown. POST /insert takes NDJSON
// (?sync=1 answers once it is durable); POST /sync, /settle and /compact wait
// for those states; POST /evict lets go of the mapped pages of the part
// files, so that dropping the page cache afterwards makes the next query a
// cold one; GET /stats describes the table. GET /query?flow= runs a query
// written in LynxFlow; GET /query?q= runs a benchmark query by its short name
// (Q1 to Q5, S1 to S7, S2i to S5i, A1 to A3, T1 to T4) as its plan, or as its
// LynxFlow text with form=text. A query takes one goroutine per CPU unless
// workers= says otherwise.
func TestJSONBenchFlowServer(t *testing.T) {
	addr, dir := os.Getenv("LYNXDB_BENCH_LISTEN"), os.Getenv("LYNXDB_BENCH_DIR")
	if addr == "" || dir == "" {
		t.Skip("set LYNXDB_BENCH_LISTEN and LYNXDB_BENCH_DIR")
	}
	store, err := colstore.OpenStore(dir, flowStoreOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := newFlowServer(t, store)
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

// loadFlowServer sends data through the insert handler from several streams
// and waits until the table has settled.
func loadFlowServer(t testing.TB, handler http.Handler, data []byte, streams int) {
	t.Helper()
	pieces := make(chan []byte, streams)
	var wg sync.WaitGroup
	for range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for piece := range pieces {
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/insert", bytes.NewReader(piece)))
				if rec.Code != http.StatusOK {
					t.Errorf("insert answered %d: %s", rec.Code, rec.Body)
				}
			}
		}()
	}
	for len(data) > 0 {
		cut := min(len(data), 8<<20)
		if i := bytes.IndexByte(data[cut-1:], '\n'); i >= 0 {
			cut += i
		} else {
			cut = len(data)
		}
		pieces <- data[:cut]
		data = data[cut:]
	}
	close(pieces)
	wg.Wait()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/settle", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("settle answered %d: %s", rec.Code, rec.Body)
	}
}

func TestFlowServerTakesInsertsAndAnswersQueries(t *testing.T) {
	store, err := colstore.OpenStore(t.TempDir(), colstore.StoreOptions{SortKey: []string{"svc"}, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	handler := newFlowServer(t, store).handler()
	var body bytes.Buffer
	const docs = 5000
	for i := range docs {
		fmt.Fprintf(&body, `{"n":%d,"svc":"s%02d","msg":"request %d done"}`+"\n", i, i%7, i)
	}
	body.WriteString("oops")
	loadFlowServer(t, handler, body.Bytes(), 2)

	get := func(target string) (int, string) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))

		return rec.Code, strings.TrimSpace(rec.Body.String())
	}
	query := func(text string) string {
		code, body := get("/query?flow=" + url.QueryEscape(text))
		if code != http.StatusOK {
			t.Fatalf("%s answered %d: %s", text, code, body)
		}
		var answer struct {
			Rows   int
			Answer [][]string
		}
		if err := json.Unmarshal([]byte(body), &answer); err != nil {
			t.Fatal(err)
		}

		return fmt.Sprint(answer.Rows, answer.Answer)
	}
	if got, want := query(`from logs | stats count()`), fmt.Sprintf("1 [[%d]]", docs); got != want {
		t.Errorf("count: %s, want %s", got, want)
	}
	if got, want := query(`from logs | where svc == "s03" | stats count() as n, max(n) as top`), "1 [[714 4994]]"; got != want {
		t.Errorf("one service: %s, want %s", got, want)
	}
	if got, want := query(`from logs "request 4242 done" | stats count()`), "1 [[1]]"; got != want {
		t.Errorf("a phrase in any field: %s, want %s", got, want)
	}
	if got, want := query(`from logs | where n < 3 | keep n, svc | sort n`), "3 [[0 s00] [1 s01] [2 s02]]"; got != want {
		t.Errorf("rows: %s, want %s", got, want)
	}
	if got, want := query(`from logs | where matches(msg, r"request 42\d\d d(one|ead)") | stats count()`), "1 [[100]]"; got != want {
		t.Errorf("a regular expression: %s, want %s", got, want)
	}
	if code, body := get("/query?flow=" + url.QueryEscape(`from logs | where matches(msg, r"(") | stats count()`)); code != http.StatusBadRequest {
		t.Errorf("an expression that does not compile answered %d: %s", code, body)
	}
	if code, body := get("/query?flow=from%20logs%20%7C%20nonsense"); code != http.StatusBadRequest {
		t.Errorf("a query that does not parse answered %d: %s", code, body)
	}
	if code, body := get("/query?q=nope"); code != http.StatusNotFound {
		t.Errorf("an unknown name answered %d: %s", code, body)
	}
	if code, body := get("/query?q=Q1&workers=0"); code != http.StatusBadRequest {
		t.Errorf("no workers answered %d: %s", code, body)
	}
	for _, target := range []string{"/query?q=Q1", "/query?q=Q1&form=text&workers=2", "/query?q=S2i&form=text", "/query?q=S2", "/query?q=T4"} {
		if code, body := get(target); code != http.StatusOK {
			t.Errorf("%s answered %d: %s", target, code, body)
		}
	}
	if code, body := get("/query?q=S2&form=text"); code != http.StatusNotFound {
		t.Errorf("a query without a text form answered %d: %s", code, body)
	}
	var stats struct{ Rows, Accepted, Rejected int64 }
	if _, body := get("/stats"); json.Unmarshal([]byte(body), &stats) != nil || stats.Rows != docs || stats.Accepted != docs || stats.Rejected != 1 {
		t.Errorf("stats %+v from %s", stats, body)
	}
}

// TestJSONBenchConcurrent loads LYNXDB_BENCH_NDJSON into a store and, when
// LYNXDB_BENCH_CONCURRENT is set, runs each query of LYNXDB_BENCH_QUERY (short
// names, as LynxFlow text where the query has one) from each number of
// clients in LYNXDB_BENCH_CLIENTS at once for LYNXDB_BENCH_SECONDS, every
// query on LYNXDB_BENCH_QUERY_WORKERS goroutines (one per CPU when unset).
// With LYNXDB_BENCH_INGEST_STREAMS that many streams keep inserting the file
// meanwhile. It reports queries per second, latency, and the CPU time and
// heap allocation of the process per query, which include the inserts.
func TestJSONBenchConcurrent(t *testing.T) {
	if os.Getenv("LYNXDB_BENCH_CONCURRENT") == "" {
		t.Skip("set LYNXDB_BENCH_CONCURRENT and LYNXDB_BENCH_NDJSON")
	}
	data := benchData(t)
	store, err := colstore.OpenStore(t.TempDir(), flowStoreOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := newFlowServer(t, store)
	handler := server.handler()
	loadFlowServer(t, handler, data, 8)
	if os.Getenv("LYNXDB_BENCH_COMPACT") != "" {
		if err := store.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	number := func(env string, fallback int) int {
		v := os.Getenv(env)
		if v == "" {
			return fallback
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			t.Fatalf("%s=%q", env, v)
		}

		return n
	}
	list := func(env, fallback string) []string {
		if v := os.Getenv(env); v != "" {
			return strings.Split(v, ",")
		}

		return strings.Split(fallback, ",")
	}
	seconds := time.Duration(number("LYNXDB_BENCH_SECONDS", 2)) * time.Second
	workers, streams := number("LYNXDB_BENCH_QUERY_WORKERS", 0), number("LYNXDB_BENCH_INGEST_STREAMS", 0)
	snap := store.Table().Snapshot()
	t.Logf("%d CPUs; %d parts; %d goroutines per query (0 is one per CPU); %d streams inserting meanwhile",
		runtime.GOMAXPROCS(0), len(snap.Parts()), workers, streams)
	snap.Release()
	t.Logf("%-4s %7s %9s %9s %9s %9s %7s %9s %6s %9s", "", "clients", "per sec", "p50 ms", "p99 ms", "CPU ms", "CPUs", "alloc KB", "GCs", "docs/s")
	for _, name := range list("LYNXDB_BENCH_QUERY", "Q1,Q2,Q4,S1,S2i,S6,S7,A1,T4") {
		target := "/query?q=" + name
		if snap := store.Table().Snapshot(); true {
			q, ok, err := server.benchPlan(snap, name)
			snap.Release()
			if err != nil || !ok {
				t.Fatalf("query %s: known %v, %v", name, ok, err)
			}
			if q.text != "" {
				target += "&form=text"
			}
		}
		if workers > 0 {
			target += "&workers=" + strconv.Itoa(workers)
		}
		for _, field := range list("LYNXDB_BENCH_CLIENTS", "1,4,16,64") {
			clients, err := strconv.Atoi(field)
			if err != nil || clients < 1 {
				t.Fatalf("LYNXDB_BENCH_CLIENTS holds %q", field)
			}
			taken := make([][]time.Duration, clients)
			var failed sync.Once
			var wg, inserting sync.WaitGroup
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			acceptedBefore := server.accepted.Load()
			cpuBefore, begin := cpuTime(), time.Now()
			deadline := begin.Add(seconds)
			for range streams {
				inserting.Add(1)
				go func() {
					defer inserting.Done()
					for at := 0; time.Now().Before(deadline); {
						end := min(len(data), at+(1<<20))
						if i := bytes.IndexByte(data[end-1:], '\n'); i >= 0 {
							end += i
						} else {
							end = len(data)
						}
						rec := httptest.NewRecorder()
						handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/insert", bytes.NewReader(data[at:end])))
						if at = end; at == len(data) {
							at = 0
						}
					}
				}()
			}
			for c := range clients {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for time.Now().Before(deadline) {
						start := time.Now()
						rec := httptest.NewRecorder()
						handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
						if rec.Code != http.StatusOK {
							failed.Do(func() { t.Errorf("%s answered %d: %s", name, rec.Code, rec.Body) })

							return
						}
						taken[c] = append(taken[c], time.Since(start))
					}
				}()
			}
			wg.Wait()
			wall, cpu := time.Since(begin), cpuTime()-cpuBefore
			runtime.ReadMemStats(&after)
			inserting.Wait()
			all := slices.Concat(taken...)
			if len(all) == 0 {
				t.Fatalf("%s: no query finished", name)
			}
			slices.Sort(all)
			t.Logf("%-4s %7d %9.0f %9.2f %9.2f %9.2f %7.1f %9.1f %6d %9.0f", name, clients, float64(len(all))/wall.Seconds(),
				millis(all[len(all)/2]), millis(all[len(all)*99/100]), millis(cpu)/float64(len(all)), cpu.Seconds()/wall.Seconds(),
				float64(after.TotalAlloc-before.TotalAlloc)/1e3/float64(len(all)), after.NumGC-before.NumGC,
				float64(server.accepted.Load()-acceptedBefore)/wall.Seconds())
		}
	}
}
