package rest

import (
	"cmp"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

type usageShare struct {
	Name  string  `json:"name"`
	Bytes int64   `json:"bytes"`
	Share float64 `json:"share"`
}

type usageAmount struct {
	Name     string `json:"name"`
	Blocks   int    `json:"blocks"`
	Values   uint64 `json:"values"`
	Bytes    int64  `json:"bytes"`
	RawBytes int64  `json:"raw_bytes,omitempty"`
}

type usageColumn struct {
	Path           string        `json:"path"`
	Kind           string        `json:"kind"`
	Bytes          int64         `json:"bytes"`
	Share          float64       `json:"share"`
	ChunkBytes     int64         `json:"chunk_bytes"`
	DirectoryBytes int64         `json:"directory_bytes"`
	FilterBytes    int64         `json:"filter_bytes"`
	Blocks         int           `json:"blocks"`
	Values         uint64        `json:"values"`
	BytesPerValue  float64       `json:"bytes_per_value"`
	Encodings      []usageAmount `json:"encodings"`
	Sections       []usageAmount `json:"sections,omitempty"`
}

type usageTier struct {
	Tier  int    `json:"tier"`
	Parts int    `json:"parts"`
	Rows  uint64 `json:"rows"`
	Bytes int64  `json:"bytes"`
}

type indexUsage struct {
	Index      string `json:"index"`
	Parts      int    `json:"parts"`
	Blocks     int    `json:"blocks"`
	Rows       uint64 `json:"rows"`
	InputBytes uint64 `json:"input_bytes"`
	Bytes      int64  `json:"bytes"`
	// Summary and Breakdown each add up to Bytes, coarsely and finely.
	Summary   []usageShare  `json:"summary"`
	Breakdown []usageShare  `json:"breakdown"`
	Tiers     []usageTier   `json:"tiers"`
	Columns   []usageColumn `json:"columns"`
}

// handleIndexColumns tells where the bytes of an index go, by column and by
// region of the part files, from the footers and directories of its parts
// and, unless sections=false, the heads of the sections of every chunk; with
// explain set to a path, it explains the coding of that column instead.
func (s *Server) handleIndexColumns(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery, auth.ScopeAdmin) {
		return
	}
	sections := true
	if v := r.URL.Query().Get("sections"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, "sections must be true or false")

			return
		}
		sections = b
	}
	index := r.PathValue("index")
	if path := r.URL.Query().Get("explain"); path != "" {
		s.handleExplainColumn(w, r, index, path)

		return
	}
	s.syncForRead()
	u, ok, err := s.db.Usage(index, sections)
	switch {
	case err != nil:
		respondInternalError(w, err.Error())

		return
	case !ok:
		respondError(w, ErrCodeNotFound, http.StatusNotFound, fmt.Sprintf("index %q not found", index))

		return
	}
	out := indexUsage{
		Index: index, Parts: u.Parts, Blocks: u.Blocks, Rows: u.Rows, InputBytes: u.InputBytes, Bytes: u.Bytes,
		Tiers: []usageTier{}, Columns: make([]usageColumn, 0, len(u.Columns)),
	}
	share := func(n int64) float64 {
		if u.Bytes == 0 {
			return 0
		}

		return float64(n) / float64(u.Bytes)
	}
	var chunks, dirs, filters int64
	for key, c := range u.Columns {
		chunks, dirs, filters = chunks+c.ChunkBytes, dirs+c.DirectoryBytes, filters+c.FilterBytes
		col := usageColumn{
			Path: key.Path, Kind: key.Kind.String(), Bytes: c.ChunkBytes + c.DirectoryBytes + c.FilterBytes,
			ChunkBytes: c.ChunkBytes, DirectoryBytes: c.DirectoryBytes, FilterBytes: c.FilterBytes,
			Blocks: c.Blocks, Values: c.Values,
			Encodings: usageAmounts(c.Encodings), Sections: usageAmounts(c.Sections),
		}
		col.Share = share(col.Bytes)
		if c.Values > 0 {
			col.BytesPerValue = float64(col.Bytes) / float64(c.Values)
		}
		out.Columns = append(out.Columns, col)
	}
	slices.SortFunc(out.Columns, func(a, b usageColumn) int {
		return cmp.Or(cmp.Compare(b.Bytes, a.Bytes), cmp.Compare(a.Path, b.Path), cmp.Compare(a.Kind, b.Kind))
	})

	fine := map[string]int64{"column_chunks": chunks, "directories.entries": dirs, "filters.own": filters}
	coarse := map[string]int64{"column_chunks": chunks, "directories": dirs, "token_filters": filters}
	for name, n := range u.Regions {
		fine[name] += n
		group := name
		switch {
		case strings.HasPrefix(name, "footer."):
			group = "footers"
		case strings.HasPrefix(name, "directories."):
			group = "directories"
		case name == "filters.padding":
			group = "filter_padding"
		case strings.HasPrefix(name, "filters."):
			group = "token_filters"
		case name == "header", name == "trailer":
			group = "headers_and_trailers"
		}
		coarse[group] += n
	}
	out.Summary, out.Breakdown = usageShares(coarse, share), usageShares(fine, share)
	for tier, t := range u.Tiers {
		out.Tiers = append(out.Tiers, usageTier{Tier: tier, Parts: t.Parts, Rows: t.Rows, Bytes: t.Bytes})
	}
	slices.SortFunc(out.Tiers, func(a, b usageTier) int { return cmp.Compare(a.Tier, b.Tier) })
	respondData(w, http.StatusOK, out)
}

// usageShares lists the non-zero amounts by bytes, the largest first.
func usageShares(amounts map[string]int64, share func(int64) float64) []usageShare {
	out := make([]usageShare, 0, len(amounts))
	for name, n := range amounts {
		if n != 0 {
			out = append(out, usageShare{Name: name, Bytes: n, Share: share(n)})
		}
	}
	slices.SortFunc(out, func(a, b usageShare) int { return cmp.Or(cmp.Compare(b.Bytes, a.Bytes), cmp.Compare(a.Name, b.Name)) })

	return out
}

func usageAmounts(amounts map[string]*part.Amount) []usageAmount {
	out := make([]usageAmount, 0, len(amounts))
	for name, a := range amounts {
		out = append(out, usageAmount{Name: name, Blocks: a.Blocks, Values: a.Values, Bytes: a.Bytes, RawBytes: a.RawBytes})
	}
	slices.SortFunc(out, func(a, b usageAmount) int { return cmp.Or(cmp.Compare(b.Bytes, a.Bytes), cmp.Compare(a.Name, b.Name)) })

	return out
}
