package spl2

import "fmt"

// TokenType represents the type of a lexer token.
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenPipe
	TokenComma
	TokenLParen
	TokenRParen
	TokenLBracket
	TokenRBracket
	TokenSemicolon
	TokenDollar
	TokenEq
	TokenNeq
	TokenLt
	TokenLte
	TokenGt
	TokenGte
	TokenStar
	TokenPlus
	TokenMinus
	TokenSlash

	// Literals.
	TokenIdent   // identifier (may contain dots, hyphens, underscores)
	TokenString  // "quoted string"
	TokenNumber  // integer or float
	TokenGlob    // wildcard pattern like web-*
	TokenFString // f"..." interpolated string

	// Keywords.
	TokenFrom
	TokenWhere
	TokenSearch
	TokenStats
	TokenEval
	TokenSort
	TokenHead
	TokenTail
	TokenTimechart
	TokenRex
	TokenFields
	TokenTable
	TokenDedup
	TokenRename
	TokenBin
	TokenStreamstats
	TokenEventstats
	TokenJoin
	TokenAppend
	TokenMultisearch
	TokenTransaction
	TokenXyseries
	TokenTop
	TokenRare
	TokenFillnull
	TokenBy
	TokenAs
	TokenAnd
	TokenOr
	TokenNot
	TokenIn
	TokenSpan
	TokenTrue
	TokenFalse
	TokenLike
	TokenTypeKeyword // "type" used in JOIN type=inner
	TokenWindow
	TokenCurrent
	TokenMaxspan
	TokenStartswith
	TokenEndswith
	TokenMaterialize
	TokenViews
	TokenDropview
	TokenUnpackJSON
	TokenUnpackLogfmt
	TokenUnpackSyslog
	TokenUnpackCombined
	TokenUnpackCLF
	TokenUnpackNginxError
	TokenUnpackCEF
	TokenUnpackKV
	TokenUnpackDocker
	TokenUnpackRedis
	TokenUnpackApacheError
	TokenUnpackPostgres
	TokenUnpackMySQLSlow
	TokenUnpackHAProxy
	TokenUnpackLEEF
	TokenUnpackW3C
	TokenUnpackPattern
	TokenJson
	TokenUnroll
	TokenPackJson
	TokenTee

	// Regex operators.
	TokenRegexMatch    // =~
	TokenRegexNotMatch // !~

	// Source selection keywords.
	TokenIndex // INDEX (alias for FROM as source command)

	// Additional keywords.
	TokenBetween // BETWEEN
	TokenIs      // IS
	TokenNull    // NULL

	// Lynx Flow command keywords.
	TokenLet
	TokenKeep
	TokenOmit
	TokenSelect
	TokenGroup
	TokenCompute
	TokenEvery
	TokenBucket
	TokenOrder
	TokenTake
	TokenRank
	TokenTopby
	TokenBottomby
	TokenBottom
	TokenRunning
	TokenEnrich
	TokenParse
	TokenExplode
	TokenPack
	TokenLookup

	// Lynx Flow clause keywords.
	TokenUsing
	TokenExtract
	TokenIfMissing
	TokenPer
	TokenOn
	TokenInto
	TokenAsc
	TokenDesc

	// Lynx Flow domain sugar keywords.
	TokenLatency
	TokenErrors
	TokenRate
	TokenPercentiles
	TokenSlowest
	TokenRollup

	// Schema exploration.
	TokenGlimpse
	TokenDescribe

	// Named pipeline fragments.
	TokenUse

	// Outlier detection.
	TokenOutliers

	// Time comparison.
	TokenCompare

	// Log pattern extraction.
	TokenPatterns

	// Distributed trace analysis.
	TokenTrace

	// Session analysis.
	TokenSessionize

	// Correlation analysis.
	TokenCorrelate

	// Topology analysis.
	TokenTopology

	// Time literals.
	TokenAt       // @
	TokenDuration // relative time like -1h, -7d, -1h@h
	TokenDot      // . (used for range syntax -7d..-1d)

	// Lynx Flow punctuation (NOT in keywords map — lexed directly).
	TokenQuestionMark   // ?
	TokenDoubleQuestion // ??
	TokenDotQuestion    // ?. (optional chaining)
	TokenPercent        // %
)

var tokenNames = map[TokenType]string{
	TokenEOF:               "EOF",
	TokenPipe:              "PIPE",
	TokenComma:             "COMMA",
	TokenLParen:            "LPAREN",
	TokenRParen:            "RPAREN",
	TokenLBracket:          "LBRACKET",
	TokenRBracket:          "RBRACKET",
	TokenSemicolon:         "SEMICOLON",
	TokenDollar:            "DOLLAR",
	TokenEq:                "EQ",
	TokenNeq:               "NEQ",
	TokenLt:                "LT",
	TokenLte:               "LTE",
	TokenGt:                "GT",
	TokenGte:               "GTE",
	TokenStar:              "STAR",
	TokenPlus:              "PLUS",
	TokenMinus:             "MINUS",
	TokenSlash:             "SLASH",
	TokenIdent:             "IDENT",
	TokenString:            "STRING",
	TokenNumber:            "NUMBER",
	TokenGlob:              "GLOB",
	TokenFString:           "FSTRING",
	TokenFrom:              "FROM",
	TokenWhere:             "WHERE",
	TokenSearch:            "SEARCH",
	TokenStats:             "STATS",
	TokenEval:              "EVAL",
	TokenSort:              "SORT",
	TokenHead:              "HEAD",
	TokenTail:              "TAIL",
	TokenTimechart:         "TIMECHART",
	TokenRex:               "REX",
	TokenFields:            "FIELDS",
	TokenTable:             "TABLE",
	TokenDedup:             "DEDUP",
	TokenRename:            "RENAME",
	TokenBin:               "BIN",
	TokenStreamstats:       "STREAMSTATS",
	TokenEventstats:        "EVENTSTATS",
	TokenJoin:              "JOIN",
	TokenAppend:            "APPEND",
	TokenMultisearch:       "MULTISEARCH",
	TokenTransaction:       "TRANSACTION",
	TokenXyseries:          "XYSERIES",
	TokenTop:               "TOP",
	TokenRare:              "RARE",
	TokenFillnull:          "FILLNULL",
	TokenBy:                "BY",
	TokenAs:                "AS",
	TokenAnd:               "AND",
	TokenOr:                "OR",
	TokenNot:               "NOT",
	TokenIn:                "IN",
	TokenSpan:              "SPAN",
	TokenTrue:              "TRUE",
	TokenFalse:             "FALSE",
	TokenLike:              "LIKE",
	TokenMaterialize:       "MATERIALIZE",
	TokenViews:             "VIEWS",
	TokenDropview:          "DROPVIEW",
	TokenUnpackJSON:        "UNPACK_JSON",
	TokenUnpackLogfmt:      "UNPACK_LOGFMT",
	TokenUnpackSyslog:      "UNPACK_SYSLOG",
	TokenUnpackCombined:    "UNPACK_COMBINED",
	TokenUnpackCLF:         "UNPACK_CLF",
	TokenUnpackNginxError:  "UNPACK_NGINX_ERROR",
	TokenUnpackCEF:         "UNPACK_CEF",
	TokenUnpackKV:          "UNPACK_KV",
	TokenUnpackDocker:      "UNPACK_DOCKER",
	TokenUnpackRedis:       "UNPACK_REDIS",
	TokenUnpackApacheError: "UNPACK_APACHE_ERROR",
	TokenUnpackPostgres:    "UNPACK_POSTGRES",
	TokenUnpackMySQLSlow:   "UNPACK_MYSQL_SLOW",
	TokenUnpackHAProxy:     "UNPACK_HAPROXY",
	TokenUnpackLEEF:        "UNPACK_LEEF",
	TokenUnpackW3C:         "UNPACK_W3C",
	TokenUnpackPattern:     "UNPACK_PATTERN",
	TokenJson:              "JSON",
	TokenUnroll:            "UNROLL",
	TokenPackJson:          "PACK_JSON",
	TokenTee:               "TEE",
	TokenRegexMatch:        "REGEX_MATCH",
	TokenRegexNotMatch:     "REGEX_NOT_MATCH",
	TokenIndex:             "INDEX",
	TokenBetween:           "BETWEEN",
	TokenIs:                "IS",
	TokenNull:              "NULL",
	TokenLet:               "LET",
	TokenKeep:              "KEEP",
	TokenOmit:              "OMIT",
	TokenSelect:            "SELECT",
	TokenGroup:             "GROUP",
	TokenCompute:           "COMPUTE",
	TokenEvery:             "EVERY",
	TokenBucket:            "BUCKET",
	TokenOrder:             "ORDER",
	TokenTake:              "TAKE",
	TokenRank:              "RANK",
	TokenTopby:             "TOPBY",
	TokenBottomby:          "BOTTOMBY",
	TokenBottom:            "BOTTOM",
	TokenRunning:           "RUNNING",
	TokenEnrich:            "ENRICH",
	TokenParse:             "PARSE",
	TokenExplode:           "EXPLODE",
	TokenPack:              "PACK",
	TokenLookup:            "LOOKUP",
	TokenUsing:             "USING",
	TokenExtract:           "EXTRACT",
	TokenIfMissing:         "IF_MISSING",
	TokenPer:               "PER",
	TokenOn:                "ON",
	TokenInto:              "INTO",
	TokenAsc:               "ASC",
	TokenDesc:              "DESC",
	TokenLatency:           "LATENCY",
	TokenErrors:            "ERRORS",
	TokenRate:              "RATE",
	TokenPercentiles:       "PERCENTILES",
	TokenSlowest:           "SLOWEST",
	TokenRollup:            "ROLLUP",
	TokenGlimpse:           "GLIMPSE",
	TokenDescribe:          "DESCRIBE",
	TokenAt:                "AT",
	TokenDuration:          "DURATION",
	TokenDot:               "DOT",
	TokenQuestionMark:      "QUESTION",
	TokenDoubleQuestion:    "DOUBLE_QUESTION",
	TokenDotQuestion:       "DOT_QUESTION",
	TokenPercent:           "PERCENT",
	TokenCorrelate:         "CORRELATE",
	TokenSessionize:        "SESSIONIZE",
	TokenTopology:          "TOPOLOGY",
}

func (t TokenType) String() string {
	if s, ok := tokenNames[t]; ok {
		return s
	}

	return fmt.Sprintf("TOKEN(%d)", t)
}

// FStringPart represents a single part of an f-string literal.
// At Literal or Expr is non-empty.
type FStringPart struct {
	Literal string // literal text (non-empty for literal parts)
	Expr    string // expression text (non-empty for interpolated parts)
}

// Token represents a single lexer token.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int           // byte offset in input
	Parts   []FStringPart // populated for TokenFString
}

// String implements fmt.Stringer for debug/error output.
func (t Token) String() string {
	return fmt.Sprintf("{%s %q @%d}", t.Type, t.Literal, t.Pos)
}

var keywords = map[string]TokenType{
	"from":                TokenFrom,
	"index":               TokenIndex,
	"where":               TokenWhere,
	"search":              TokenSearch,
	"stats":               TokenStats,
	"eval":                TokenEval,
	"sort":                TokenSort,
	"head":                TokenHead,
	"tail":                TokenTail,
	"timechart":           TokenTimechart,
	"rex":                 TokenRex,
	"fields":              TokenFields,
	"table":               TokenTable,
	"dedup":               TokenDedup,
	"rename":              TokenRename,
	"bin":                 TokenBin,
	"streamstats":         TokenStreamstats,
	"eventstats":          TokenEventstats,
	"join":                TokenJoin,
	"append":              TokenAppend,
	"multisearch":         TokenMultisearch,
	"transaction":         TokenTransaction,
	"xyseries":            TokenXyseries,
	"top":                 TokenTop,
	"rare":                TokenRare,
	"fillnull":            TokenFillnull,
	"by":                  TokenBy,
	"as":                  TokenAs,
	"and":                 TokenAnd,
	"or":                  TokenOr,
	"not":                 TokenNot,
	"in":                  TokenIn,
	"span":                TokenSpan,
	"true":                TokenTrue,
	"false":               TokenFalse,
	"like":                TokenLike,
	"materialize":         TokenMaterialize,
	"views":               TokenViews,
	"dropview":            TokenDropview,
	"unpack_json":         TokenUnpackJSON,
	"unpack_logfmt":       TokenUnpackLogfmt,
	"unpack_syslog":       TokenUnpackSyslog,
	"unpack_combined":     TokenUnpackCombined,
	"unpack_clf":          TokenUnpackCLF,
	"unpack_nginx_error":  TokenUnpackNginxError,
	"unpack_cef":          TokenUnpackCEF,
	"unpack_kv":           TokenUnpackKV,
	"unpack_docker":       TokenUnpackDocker,
	"unpack_redis":        TokenUnpackRedis,
	"unpack_apache_error": TokenUnpackApacheError,
	"unpack_postgres":     TokenUnpackPostgres,
	"unpack_mysql_slow":   TokenUnpackMySQLSlow,
	"unpack_haproxy":      TokenUnpackHAProxy,
	"unpack_leef":         TokenUnpackLEEF,
	"unpack_w3c":          TokenUnpackW3C,
	"unpack_pattern":      TokenUnpackPattern,
	"json":                TokenJson,
	"unroll":              TokenUnroll,
	"pack_json":           TokenPackJson,
	"tee":                 TokenTee,
	"between":             TokenBetween,
	"is":                  TokenIs,
	"null":                TokenNull,
	"let":                 TokenLet,
	"keep":                TokenKeep,
	"omit":                TokenOmit,
	"select":              TokenSelect,
	"group":               TokenGroup,
	"compute":             TokenCompute,
	"every":               TokenEvery,
	"bucket":              TokenBucket,
	"order":               TokenOrder,
	"take":                TokenTake,
	"rank":                TokenRank,
	"topby":               TokenTopby,
	"bottomby":            TokenBottomby,
	"bottom":              TokenBottom,
	"running":             TokenRunning,
	"enrich":              TokenEnrich,
	"parse":               TokenParse,
	"explode":             TokenExplode,
	"pack":                TokenPack,
	"lookup":              TokenLookup,
	"using":               TokenUsing,
	"extract":             TokenExtract,
	"if_missing":          TokenIfMissing,
	"per":                 TokenPer,
	"on":                  TokenOn,
	"into":                TokenInto,
	"asc":                 TokenAsc,
	"desc":                TokenDesc,
	"latency":             TokenLatency,
	"errors":              TokenErrors,
	"rate":                TokenRate,
	"percentiles":         TokenPercentiles,
	"slowest":             TokenSlowest,
	"rollup":              TokenRollup,
	"glimpse":             TokenGlimpse,
	"describe":            TokenDescribe,
	"use":                 TokenUse,
	"outliers":            TokenOutliers,
	"compare":             TokenCompare,
	"patterns":            TokenPatterns,
	"trace":               TokenTrace,
	"correlate":           TokenCorrelate,
	"sessionize":          TokenSessionize,
	"topology":            TokenTopology,
}

func lookupKeyword(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}

	return TokenIdent
}
