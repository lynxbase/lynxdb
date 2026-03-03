package rest

// QueryRequest is the request body for POST /api/v1/query.
type QueryRequest struct {
	Q        string   `json:"q"`
	Query    string   `json:"query"` // alias for q
	From     string   `json:"from"`  // "-1h", "now", ISO 8601
	To       string   `json:"to"`
	Earliest string   `json:"earliest"` // legacy alias for from
	Latest   string   `json:"latest"`   // legacy alias for to
	Limit    int      `json:"limit"`
	Offset   int      `json:"offset"`
	Format   string   `json:"format"`  // json (default)
	Wait     *float64 `json:"wait"`    // nil=sync, 0=async, N=hybrid
	Profile  string   `json:"profile"` // "basic", "full", "trace" — enables profiling in response
}

func (r *QueryRequest) effectiveQuery() string {
	if r.Q != "" {
		return r.Q
	}

	return r.Query
}

func (r *QueryRequest) effectiveFrom() string {
	if r.From != "" {
		return r.From
	}

	return r.Earliest
}

func (r *QueryRequest) effectiveTo() string {
	if r.To != "" {
		return r.To
	}

	return r.Latest
}
