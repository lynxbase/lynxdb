package ppl

import "sort"

// Support is how completely LynxDB implements a PPL feature.
type Support string

const (
	// Full support matches the documented OpenSearch semantics, apart from
	// the deviations listed with the feature.
	Full Support = "full"
	// Partial support executes the feature with the options and limits
	// listed with it; other options fail with an unsupported error.
	Partial Support = "partial"
	// Unsupported features fail with an unsupported error.
	Unsupported Support = "unsupported"
)

// CommandSupport describes LynxDB's support for one PPL command.
type CommandSupport struct {
	Name    string
	Support Support
	// Notes lists limitations and deviations from OpenSearch; for an
	// unsupported command it gives the reason.
	Notes string
}

// commandCatalog is the PPL command inventory with LynxDB's support level.
var commandCatalog = map[string]CommandSupport{
	"search":           {Support: Partial, Notes: "free-text terms match whole tokens of the event's raw text through the term index (OpenSearch searches all fields); quoted phrases match at token boundaries; field comparisons are exact and case-sensitive; field wildcards match the whole value; time modifiers supported; cross-cluster prefixes are read as part of the index name"},
	"where":            {Support: Full},
	"subquery":         {Support: Partial, Notes: "uncorrelated IN, EXISTS and scalar subqueries; a subquery is evaluated once before the outer query; references to outer fields are not supported"},
	"fields":           {Support: Full},
	"table":            {Support: Full},
	"rename":           {Support: Full},
	"eval":             {Support: Full},
	"convert":          {Support: Full, Notes: "auto, num, rmcomma, rmunit, memk, dur2sec, mstime, ctime, mktime, none"},
	"replace":          {Support: Full},
	"fillnull":         {Support: Full, Notes: "without a field list, fills NULL values of the columns each row has"},
	"expand":           {Support: Full},
	"flatten":          {Support: Partial, Notes: "aliases are assigned to members in key order"},
	"stats":            {Support: Partial, Notes: "bucket_nullable supported; partitions, allnum and delim accepted without effect; dedup_splitvalues unsupported; distinct_count is exact up to 10,000 values per group and estimated beyond; output row order is unspecified"},
	"eventstats":       {Support: Partial, Notes: "bucket_nullable=false with a by clause and the window functions row_number, rank, dense_rank, percent_rank, cume_dist, nth and ntile are unsupported"},
	"streamstats":      {Support: Partial, Notes: "current, window and global=false supported; bucket_nullable=false with a by clause, reset_before, reset_after, time_window, and window with global=true and a by clause are unsupported"},
	"bin":              {Support: Partial, Notes: "span= with numeric and time units; bins, minspan, aligntime, start, end and logarithmic spans are unsupported"},
	"timechart":        {Support: Partial, Notes: "span, limit, useother, usenull, nullstr and timefield supported (the default time field is _time); bins and minspan unsupported; per_second, per_minute, per_hour and per_day supported"},
	"chart":            {Support: Partial, Notes: "limit (topN, bottomN), useother, usenull, nullstr, otherstr and span= on splits supported; bins, minspan, start and end unsupported"},
	"trendline":        {Support: Full},
	"sort":             {Support: Full, Notes: "ip(field) sorts by the text form of the address"},
	"reverse":          {Support: Full},
	"head":             {Support: Full},
	"dedup":            {Support: Full},
	"top":              {Support: Full},
	"rare":             {Support: Full},
	"parse":            {Support: Full, Notes: "RE2 regular expressions: no lookaround or backreferences"},
	"grok":             {Support: Partial, Notes: "built-in library of common patterns (pkg/ppl/grok); custom pattern definitions are not supported"},
	"rex":              {Support: Partial, Notes: "extract mode and sed s/regex/replacement/[g]; offset_field unsupported; RE2 syntax"},
	"regex":            {Support: Full, Notes: "RE2 syntax"},
	"spath":            {Support: Partial, Notes: "auto-extract keeps JSON value types; a JSON null stays NULL"},
	"patterns":         {Support: Partial, Notes: "simple_pattern in label and aggregation modes; a by clause only in aggregation mode; brain and show_numbered_token=true unsupported"},
	"join":             {Support: Partial, Notes: "inner, left, right, full, semi, anti and cross joins with equality keys and a residual condition, overwrite and max; the right side is held in memory"},
	"append":           {Support: Full},
	"appendcol":        {Support: Full},
	"appendpipe":       {Support: Full, Notes: "the input is computed twice"},
	"lookup":           {Support: Partial, Notes: "replace, append and output with an explicit output field list"},
	"multisearch":      {Support: Full, Notes: "results are merged newest first"},
	"union":            {Support: Full},
	"describe":         {Support: Partial, Notes: "returns TABLE_NAME, COLUMN_NAME, TYPE_NAME and ORDINAL_POSITION from the values stored in the index"},
	"explain":          {Support: Partial, Notes: "returns LynxDB's logical and optimized IR plans; the simple, standard, cost and extended modes render the same plans"},
	"makeresults":      {Support: Full},
	"addtotals":        {Support: Full},
	"addcoltotals":     {Support: Full},
	"transpose":        {Support: Full},
	"mvcombine":        {Support: Full},
	"nomv":             {Support: Full},
	"mvexpand":         {Support: Full},
	"xyseries":         {Support: Full},
	"fieldformat":      {Support: Unsupported, Notes: "display formatting with prefix and suffix is not implemented; use eval with concat()"},
	"foreach":          {Support: Unsupported, Notes: "templated per-field evaluation is not implemented; write one eval per field"},
	"timewrap":         {Support: Unsupported, Notes: "period-over-period wrapping is not implemented; compute the shifted period with eval and join"},
	"graphlookup":      {Support: Unsupported, Notes: "recursive graph traversal needs an iterative join operator LynxDB does not have"},
	"rest":             {Support: Unsupported, Notes: "it reads OpenSearch cluster management endpoints; use the LynxDB REST API"},
	"show datasources": {Support: Unsupported, Notes: "LynxDB has no external data sources; every index is local"},
	"ml":               {Support: Unsupported, Notes: "it runs the OpenSearch ML Commons plugin, which LynxDB does not have"},
	"kmeans":           {Support: Unsupported, Notes: "it runs the OpenSearch ML Commons plugin, which LynxDB does not have"},
	"ad":               {Support: Unsupported, Notes: "it runs the OpenSearch ML Commons plugin, which LynxDB does not have"},
}

func init() {
	for name, c := range commandCatalog {
		c.Name = name
		commandCatalog[name] = c
	}
}

// Commands lists the PPL command inventory sorted by name.
func Commands() []CommandSupport {
	out := make([]CommandSupport, 0, len(commandCatalog))
	for _, c := range commandCatalog {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// LookupCommand returns the support entry of a command.
func LookupCommand(name string) (CommandSupport, bool) {
	c, ok := commandCatalog[name]
	return c, ok
}
