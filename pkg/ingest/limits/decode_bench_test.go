package limits

import (
	"bytes"
	stdgzip "compress/gzip"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/lynxbase/lynxdb/internal/benchdata"
)

// BenchmarkDecodeBody decodes 64 MB of generated events compressed as a
// client sends them, through WrapBody and, for comparison, through the
// standard library's gzip; MB/s is of decoded bytes. Run it with -cpu 1 for
// the speed of one core.
func BenchmarkDecodeBody(b *testing.B) {
	data := benchdata.New(1, 1<<20).Append(nil, 150000)
	var gz, zs bytes.Buffer
	gw, _ := stdgzip.NewWriterLevel(&gz, stdgzip.DefaultCompression)
	_, _ = gw.Write(data)
	_ = gw.Close()
	zw, err := zstd.NewWriter(&zs)
	if err != nil {
		b.Fatal(err)
	}
	_, _ = zw.Write(data)
	_ = zw.Close()
	b.Logf("gzip %.1f MB, zstd %.1f MB of %.1f MB", float64(gz.Len())/1e6, float64(zs.Len())/1e6, float64(len(data))/1e6)

	run := func(b *testing.B, open func(io.Reader) (io.ReadCloser, error), body []byte) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			r, err := open(bytes.NewReader(body))
			if err != nil {
				b.Fatal(err)
			}
			if n, err := io.Copy(io.Discard, r); err != nil || n != int64(len(data)) {
				b.Fatalf("decoded %d bytes: %v", n, err)
			}
			_ = r.Close()
		}
	}
	wrap := func(encoding string) func(io.Reader) (io.ReadCloser, error) {
		return func(r io.Reader) (io.ReadCloser, error) {
			return WrapBody(io.NopCloser(r), encoding, Config{MaxDecompressedBytes: 1 << 30}, nil, nil)
		}
	}
	b.Run("gzip", func(b *testing.B) { run(b, wrap("gzip"), gz.Bytes()) })
	b.Run("gzip-stdlib", func(b *testing.B) {
		run(b, func(r io.Reader) (io.ReadCloser, error) { return stdgzip.NewReader(r) }, gz.Bytes())
	})
	b.Run("zstd", func(b *testing.B) { run(b, wrap("zstd"), zs.Bytes()) })
}
