package rest

import (
	"cmp"
	"fmt"
	"net/http"
	"slices"
	"strconv"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// defaultExplainBlocks is the most blocks an explanation looks at when the
// request does not say.
const defaultExplainBlocks = 300

type explainForm struct {
	Form          string  `json:"form"`
	Blocks        int     `json:"blocks"`
	Values        uint64  `json:"values"`
	Bytes         int64   `json:"bytes"`
	BytesPerValue float64 `json:"bytes_per_value"`
	Smallest      int     `json:"smallest,omitempty"`
}

type explainCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type explainTimed struct {
	Rejected       []explainCount   `json:"rejected"`
	Form           []explainCount   `json:"form"`
	Reference      []explainCount   `json:"reference"`
	Unit           []explainCount   `json:"unit_power_of_ten"`
	Shift          []explainCount   `json:"shift_bits"`
	DifferenceBits []explainCount   `json:"difference_bits"`
	LowDict        []explainCount   `json:"low_dictionary_size"`
	Exceptions     map[string]int64 `json:"exceptions"`
	Sections       map[string]int64 `json:"section_bytes"`
}

type columnExplain struct {
	Index   string         `json:"index"`
	Path    string         `json:"path"`
	Blocks  int            `json:"blocks"`
	Values  uint64         `json:"values"`
	Repeats uint64         `json:"adjacent_repeats"`
	Stored  []explainForm  `json:"stored"`
	Forms   []explainForm  `json:"forms"`
	Timed   explainTimed   `json:"timed"`
	Lost    []explainCount `json:"lost"`
	// Dictionary sums the part dictionaries of the column, which the blocks
	// stored as shared hold codes of.
	Dictionary *usageDictionary `json:"dictionary,omitempty"`
}

// handleExplainColumn re-encodes a sample of the blocks of one string column
// of an index in every form the writer has, and tells, without a value of
// the data, what each takes and why the writer chose as it did.
func (s *Server) handleExplainColumn(w http.ResponseWriter, r *http.Request, index, path string) {
	blocks := defaultExplainBlocks
	if v := r.URL.Query().Get("blocks"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, "blocks must be a positive number")

			return
		}
		blocks = n
	}
	s.syncForRead()
	e, ok, err := s.db.ExplainColumn(index, path, blocks)
	switch {
	case err != nil:
		respondInternalError(w, err.Error())

		return
	case !ok:
		respondError(w, ErrCodeNotFound, http.StatusNotFound, fmt.Sprintf("index %q not found", index))

		return
	}
	out := columnExplain{
		Index: index, Path: path, Blocks: e.Blocks, Values: e.Values, Repeats: e.Repeats,
		Stored: explainForms(e.Stored, nil), Forms: explainForms(e.Forms, e.Smallest), Lost: explainCounts(e.Lost),
		Dictionary: usageDictionaryOf(e.Dictionary),
	}
	t := &e.Timed
	out.Timed = explainTimed{
		Rejected: explainCounts(t.Rejected), Form: explainCounts(t.Form), Reference: explainCounts(t.Reference),
		Unit: explainInts(t.Unit), Shift: explainInts(t.Shift), DifferenceBits: explainInts(t.DifferenceBits),
		LowDict: explainInts(t.LowDict), Sections: t.Sections,
		Exceptions: map[string]int64{"strings": t.Strings, "string_bytes": t.StringBytes, "far": t.Far, "far_bytes": t.FarBytes},
	}
	respondData(w, http.StatusOK, out)
}

func explainForms(sums map[string]*part.FormSum, smallest map[string]int) []explainForm {
	out := make([]explainForm, 0, len(sums))
	for form, f := range sums {
		e := explainForm{Form: form, Blocks: f.Blocks, Values: f.Values, Bytes: f.Bytes, Smallest: smallest[form]}
		if f.Values > 0 {
			e.BytesPerValue = float64(f.Bytes) / float64(f.Values)
		}
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b explainForm) int { return cmp.Or(cmp.Compare(b.Bytes, a.Bytes), cmp.Compare(a.Form, b.Form)) })

	return out
}

func explainCounts(counts map[string]int) []explainCount {
	out := make([]explainCount, 0, len(counts))
	for name, n := range counts {
		out = append(out, explainCount{Name: name, Count: n})
	}
	slices.SortFunc(out, func(a, b explainCount) int { return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.Name, b.Name)) })

	return out
}

func explainInts(counts map[int]int) []explainCount {
	named := make(map[string]int, len(counts))
	for k, n := range counts {
		named[strconv.Itoa(k)] = n
	}

	return explainCounts(named)
}
