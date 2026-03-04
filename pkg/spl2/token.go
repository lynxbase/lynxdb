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
	TokenIdent  // identifier (may contain dots, hyphens, underscores)
	TokenString // "quoted string"
	TokenNumber // integer or float
	TokenGlob   // wildcard pattern like web-*

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
}

func (t TokenType) String() string {
	if s, ok := tokenNames[t]; ok {
		return s
	}

	return fmt.Sprintf("TOKEN(%d)", t)
}

// Token represents a single lexer token.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int // byte offset in input
}

// String implements fmt.Stringer for debug/error output.
func (t Token) String() string {
	return fmt.Sprintf("{%s %q @%d}", t.Type, t.Literal, t.Pos)
}

var keywords = map[string]TokenType{
	"from":                TokenFrom,
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
}

func lookupKeyword(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}

	return TokenIdent
}
