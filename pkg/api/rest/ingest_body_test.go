package rest

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/database"
)

// indexRows returns the rows an index holds once what was appended is
// written.
func indexRows(t *testing.T, srv *Server, index string) uint64 {
	t.Helper()
	if err := srv.DB().Sync(); err != nil {
		t.Fatal(err)
	}
	st, _ := srv.DB().Index(index)

	return st.Rows
}

func TestIngestRefusesOrTellsOfABodyThatDoesNotDecode(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()
	body := ndjsonBody(20000)
	var gz, zs bytes.Buffer
	gw := gzip.NewWriter(&gz)
	gw.Write(body)
	gw.Close()
	zw, err := zstd.NewWriter(&zs)
	if err != nil {
		t.Fatal(err)
	}
	zw.Write(body)
	zw.Close()
	badSum := bytes.Clone(gz.Bytes())
	badSum[len(badSum)-8] ^= 1
	if len(body) <= bodyPiece {
		t.Fatalf("a body of %d bytes fits one piece", len(body))
	}
	for _, c := range []struct {
		encoding string
		body     []byte
	}{
		{"gzip", gz.Bytes()[:gz.Len()/2]},
		{"gzip", gz.Bytes()[:gz.Len()-4]},
		{"gzip", badSum},
		{"zstd", zs.Bytes()[:zs.Len()/2]},
	} {
		index := fmt.Sprintf("cut-%s-%d", c.encoding, len(c.body))
		req, err := http.NewRequest(http.MethodPost, "http://"+srv.Addr()+"/api/v1/ingest?index="+index, bytes.NewReader(c.body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Encoding", c.encoding)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		out, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		rows := indexRows(t, srv, index)
		if resp.StatusCode == http.StatusOK || rows != 0 && !strings.Contains(string(out), "were taken") {
			t.Errorf("%s body of %d bytes that does not decode: %d %s, %d rows taken", c.encoding, len(c.body), resp.StatusCode, out, rows)
		}
	}
}

func TestIngestRefusesABodyShorterThanItsLength(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sent := `{"n":1}` + "\n" + `{"n":2}` + "\n" + `{"n":`
	fmt.Fprintf(conn, "POST /api/v1/ingest?index=short HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-ndjson\r\nContent-Length: %d\r\n\r\n%s", len(sent)+100, sent)
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		// The server may drop the connection, which leaves nothing taken.
		if rows := indexRows(t, srv, "short"); rows != 0 {
			t.Fatalf("no reply, %d rows taken", rows)
		}

		return
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("a body cut short of its length: %d %s, %d rows taken", resp.StatusCode, out, indexRows(t, srv, "short"))
	}
}

func TestIngestOverTheDecodedLimitTellsOfTheLinesTaken(t *testing.T) {
	var cfg Config
	cfg.Ingest.MaxBodySize = config.ByteSize(3 * bodyPiece)
	srv, cleanup := startTestServerWithConfig(t, cfg)
	defer cleanup()
	body := ndjsonBody(4 * bodyPiece / 60)
	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	gw.Write(body)
	gw.Close()
	if gz.Len() >= 3*bodyPiece || len(body) <= 3*bodyPiece {
		t.Fatalf("%d bytes gzipped to %d do not cross the limit decoded only", len(body), gz.Len())
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Addr()+"/api/v1/ingest?index=decoded", bytes.NewReader(gz.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("a body over the limit decoded: %d %s", resp.StatusCode, out)
	}
	if rows := indexRows(t, srv, "decoded"); rows != 0 && !strings.Contains(string(out), "were taken") {
		t.Fatalf("a refused body left %d rows and the reply does not tell: %s", rows, out)
	}
}

func TestIngestOverTheLimitTellsHowManyLinesWereTaken(t *testing.T) {
	var cfg Config
	cfg.Ingest.MaxBodySize = config.ByteSize(3*bodyPiece + bodyPiece/2)
	srv, cleanup := startTestServerWithConfig(t, cfg)
	defer cleanup()
	// A body of unknown length is refused only once it is read past the
	// limit.
	resp, err := http.Post("http://"+srv.Addr()+"/api/v1/ingest?index=over", "application/x-ndjson",
		struct{ io.Reader }{bytes.NewReader(ndjsonBody(5 * bodyPiece / 60))})
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	rows := indexRows(t, srv, "over")
	if resp.StatusCode != http.StatusRequestEntityTooLarge || rows == 0 || !strings.Contains(string(out), fmt.Sprintf("the %d lines before the limit were taken", rows)) {
		t.Fatalf("%d %s with %d rows kept", resp.StatusCode, out, rows)
	}
}

func TestIngestCutShortByACloseTellsOfTheLinesTaken(t *testing.T) {
	dir := t.TempDir()
	srv, cleanup := startTestServerWithConfig(t, Config{DataDir: dir})
	defer cleanup()
	type reply struct {
		code int
		body string
		err  error
	}
	replies := make(chan reply, 1)
	body, send := io.Pipe()
	go func() {
		resp, err := http.Post("http://"+srv.Addr()+"/api/v1/ingest?index=cut", "application/x-ndjson", body)
		if err != nil {
			replies <- reply{err: err}

			return
		}
		out, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		replies <- reply{code: resp.StatusCode, body: string(out)}
	}()
	if _, err := send.Write(ndjsonBody(3 * bodyPiece / 60)); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		if _, ok := srv.DB().Index("cut"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first lines were not appended")
		}
	}
	// The database closes, as when a shutdown outlasts its drain, while the
	// body is still being read.
	if err := srv.DB().Close(); err != nil {
		t.Fatal(err)
	}
	send.Write(ndjsonBody(bodyPiece / 60))
	send.Close()
	r := <-replies
	if r.err != nil {
		t.Fatal(r.err)
	}
	db, err := database.Open(database.Config{Dir: indexDir(dir)})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st, _ := db.Index("cut")
	if r.code != http.StatusOK && st.Rows != 0 && !strings.Contains(r.body, "were taken") {
		t.Fatalf("a reply of %d %s for a body of which %d rows were kept", r.code, r.body, st.Rows)
	}
}
