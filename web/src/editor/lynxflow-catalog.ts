import type { Completion } from "@codemirror/autocomplete";

export const BUILTIN_FIELDS: readonly string[] = [
  "_time",
  "_raw",
  "_source",
  "_sourcetype",
  "_timestamp",
  "host",
  "source",
  "sourcetype",
  "index",
];

export const COMMANDS: readonly string[] = [
  "from", "index", "search", "where", "stats", "eval", "sort", "head", "tail",
  "timechart", "rex", "fields", "table", "dedup", "rename", "bin",
  "streamstats", "eventstats", "join", "append", "multisearch", "transaction",
  "xyseries", "top", "rare", "fillnull", "materialize", "views",
  "dropview", "unpack_json", "unpack_logfmt", "unpack_syslog", "unpack_combined",
  "unpack_clf", "unpack_nginx_error", "unpack_cef", "unpack_kv", "unpack_docker",
  "unpack_redis", "unpack_apache_error", "unpack_postgres", "unpack_mysql_slow",
  "unpack_haproxy", "unpack_leef", "unpack_w3c", "unpack_pattern", "json",
  "unroll", "pack_json", "tee",
  "let", "keep", "omit", "select", "group", "every", "bucket", "order", "take",
  "rank", "topby", "bottomby", "bottom", "running", "enrich", "parse", "explode",
  "pack", "lookup", "latency", "errors", "rate", "percentiles", "slowest",
  "glimpse", "describe", "use", "outliers", "compare", "patterns", "trace",
  "rollup", "correlate", "sessionize", "topology",
];

export const COMMAND_DOCS: Record<string, string> = {
  from: "source scope",
  index: "source scope alias",
  search: "SPL search filter",
  where: "boolean filter",
  stats: "aggregate rows",
  eval: "compute fields",
  sort: "sort rows",
  head: "first rows",
  tail: "last rows",
  timechart: "time-bucket aggregate",
  rex: "regex extraction",
  fields: "keep/remove fields",
  table: "project columns",
  dedup: "deduplicate rows",
  rename: "rename fields",
  bin: "bucket values",
  streamstats: "running statistics",
  eventstats: "add aggregate fields",
  join: "join datasets",
  append: "append subsearch",
  multisearch: "combine subsearches",
  transaction: "group events",
  xyseries: "pivot rows",
  top: "frequent values",
  rare: "least frequent values",
  fillnull: "fill missing values",
  materialize: "create materialized view",
  views: "inspect views",
  dropview: "drop materialized view",
  json: "extract JSON paths",
  unroll: "expand array values",
  pack_json: "pack fields as JSON",
  tee: "copy stream to sink",
  let: "eval alias",
  keep: "fields + alias",
  omit: "fields - alias",
  select: "table with aliases",
  group: "stats sugar",
  every: "timechart sugar",
  bucket: "bin sugar",
  order: "sort sugar",
  take: "head sugar",
  rank: "sort/head sugar",
  topby: "rank groups by metric",
  bottomby: "rank groups ascending",
  bottom: "bottom metric ranking",
  running: "streamstats sugar",
  enrich: "eventstats sugar",
  parse: "structured extraction",
  explode: "unroll sugar",
  pack: "pack_json sugar",
  lookup: "left lookup join",
  latency: "p50/p95/p99 timechart",
  errors: "level error/fatal aggregate",
  rate: "count per time bucket",
  percentiles: "p50..p99 aggregate",
  slowest: "sort by duration",
  glimpse: "field/value summary",
  describe: "schema/source metadata",
  use: "named fragment",
  outliers: "mark statistical outliers",
  compare: "previous-window comparison",
  patterns: "message templates",
  trace: "span tree fields",
  rollup: "multiple time resolutions",
  correlate: "field correlation",
  sessionize: "time-gap sessions",
  topology: "edge/node summaries",
};

export const CLAUSES: readonly string[] = [
  "by", "as", "compute", "using", "extract", "if_missing", "per", "on", "into",
  "span", "window", "current", "maxspan", "startswith", "endswith", "type",
  "over", "limit", "cont", "usenull", "useother", "earliest", "latest",
  "_index_earliest", "_index_latest", "time", "asc", "desc", "inner", "outer",
  "left", "right",
];

export const OPERATORS: readonly string[] = [
  "AND", "OR", "NOT", "XOR", "IN", "LIKE", "BETWEEN", "IS", "NULL",
];

export const AGG_FUNCTIONS: readonly string[] = [
  "count()", "sum()", "avg()", "mean()", "min()", "max()", "dc()", "distinct_count()",
  "values()", "list()", "first()", "last()", "earliest()", "latest()", "stdev()",
  "stdevp()", "var()", "varp()", "range()", "median()", "mode()", "perc50()",
  "perc75()", "perc90()", "perc95()", "perc99()", "percentile()", "exactperc95()",
  "upperperc95()", "per_second()", "per_minute()", "per_hour()", "rate()",
];

export const LATENCY_AGG_SHORTHANDS: readonly string[] = [
  "p50", "p75", "p90", "p95", "p99", "avg", "max", "count",
];

export const EVAL_FUNCTIONS: readonly string[] = [
  "if()", "case()", "validate()", "coalesce()", "null()", "nullif()", "in()",
  "match()", "like()", "cidrmatch()", "isnull()", "isnotnull()", "isnum()",
  "isnumeric()", "isint()", "isstr()", "isbool()", "typeof()", "tonumber()",
  "tostring()", "toint()", "todouble()", "tobool()", "printf()", "abs()", "ceil()",
  "ceiling()", "floor()", "round()", "sqrt()", "pow()", "log()", "ln()", "exp()",
  "lower()", "upper()", "len()", "substr()", "replace()", "trim()", "ltrim()",
  "rtrim()", "split()", "mvappend()", "mvcount()", "mvdedup()", "mvfind()",
  "mvindex()", "mvjoin()", "mvsort()", "mvzip()", "now()", "time()",
  "relative_time()", "strftime()", "strptime()", "json_extract()", "json_valid()",
  "json_keys()", "json_array_length()", "json_object()", "json_array()", "json_type()",
  "json_set()", "json_remove()", "json_merge()",
];

export const FUNCTIONS: readonly string[] = [
  ...new Set([
    ...AGG_FUNCTIONS.map((fn) => fn.replace(/\(\)$/, "")),
    ...EVAL_FUNCTIONS.map((fn) => fn.replace(/\(\)$/, "")),
    ...LATENCY_AGG_SHORTHANDS,
  ]),
];

export const QUERY_TEMPLATES: readonly Completion[] = [
  { label: "errors by service", apply: "errors by service", type: "text", detail: "shortcut" },
  { label: "latency duration_ms every 1m", apply: "latency duration_ms every 1m", type: "text", detail: "shortcut" },
  { label: "rate per 1m by service", apply: "rate per 1m by service", type: "text", detail: "shortcut" },
  { label: "percentiles duration_ms by service", apply: "percentiles duration_ms by service", type: "text", detail: "shortcut" },
  { label: "slowest 10 by duration_ms", apply: "slowest 10 by duration_ms", type: "text", detail: "shortcut" },
  { label: "status>=500", apply: "status>=500", type: "text", detail: "free-hand search" },
  { label: "\"connection reset\"", apply: "\"connection reset\"", type: "text", detail: "phrase search" },
];

export const SOURCE_TEMPLATES: readonly Completion[] = [
  { label: "*", type: "variable", detail: "all authorized sources" },
  { label: "logs*", type: "variable", detail: "source glob" },
  { label: "$fragment", type: "variable", detail: "CTE/source fragment" },
];

export const TIME_TEMPLATES: readonly Completion[] = [
  { label: "[-15m]", apply: "[-15m]", type: "constant", detail: "last 15 minutes" },
  { label: "[-1h]", apply: "[-1h]", type: "constant", detail: "last hour" },
  { label: "[-24h]", apply: "[-24h]", type: "constant", detail: "last 24 hours" },
  { label: "[-7d..-1d]", apply: "[-7d..-1d]", type: "constant", detail: "relative range" },
];

export const TIME_VALUES: readonly Completion[] = [
  { label: "-15m", type: "constant", detail: "relative time" },
  { label: "-1h", type: "constant", detail: "relative time" },
  { label: "-24h", type: "constant", detail: "relative time" },
  { label: "-7d@d", type: "constant", detail: "snap to day" },
  { label: "now", type: "constant", detail: "time modifier alias" },
  { label: "now()", type: "function", detail: "time modifier alias" },
];

export const REGEX_TEMPLATES: readonly Completion[] = [
  { label: "\"(?i)error|fatal\"", apply: "\"(?i)error|fatal\"", type: "text", detail: "linear regex" },
  { label: "\"timeout|timed out\"", apply: "\"timeout|timed out\"", type: "text", detail: "linear regex" },
  { label: "\"(?<field>\\\\w+)\"", apply: "\"(?<field>\\\\w+)\"", type: "text", detail: "named capture" },
];
