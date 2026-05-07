package sigmaqueries

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func FuzzParseEmittedSPL2(f *testing.F) {
	seedGoldenCorpus(f)

	f.Fuzz(func(t *testing.T, input string) {
		_, _ = spl2.ParseProgram(input)
	})
}

func FuzzPlanEmittedSPL2(f *testing.F) {
	seedGoldenCorpus(f)

	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			t.Skip()
		}

		prog, err := spl2.ParseProgram(input)
		if err != nil {
			t.Skip()
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		iter, err := pipeline.BuildProgram(ctx, prog, &pipeline.ServerIndexStore{}, 0)
		if err != nil {
			return
		}
		if iter != nil {
			_ = iter.Close()
		}
	})
}

type corpusSeed interface {
	Add(args ...any)
	Fatalf(format string, args ...any)
}

func seedGoldenCorpus(f corpusSeed) {
	fixtures, err := filepath.Glob(filepath.Join("testdata", "golden", "*.spl2"))
	if err != nil {
		f.Fatalf("glob golden fixtures: %v", err)
	}
	if len(fixtures) == 0 {
		f.Fatalf("no golden SPL2 fixtures discovered")
	}

	for _, fixture := range fixtures {
		data, err := os.ReadFile(fixture)
		if err != nil {
			f.Fatalf("read %s: %v", fixture, err)
		}
		for _, raw := range strings.Split(string(data), "\n") {
			line := strings.TrimSpace(raw)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			f.Add(line)
		}
	}
}
