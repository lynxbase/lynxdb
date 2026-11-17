---
title: PPL grammar
sidebar_label: Grammar
description: The PPL grammar as LynxDB's parser implements it.
---

# PPL grammar

This is the grammar `pkg/ppl/parser` accepts, written in EBNF. Keywords are
case-insensitive identifiers matched by position. `[ x ]` is optional,
`{ x }` repeats zero or more times, `|` inside a production separates
alternatives, and `"|"` is the pipe token. Accepting a command or option here
does not mean LynxDB executes it: unsupported commands and options parse and
then fail with an `UNSUPPORTED_*` error (see the [feature matrix](./feature-matrix.md)).

## Queries

```ebnf
query           = [ "explain" [ "simple" | "standard" | "cost" | "extended" ] ]
                  pipeline [ ";" ] ;
pipeline        = [ "|" ] source_command { "|" command } ;
subsearch       = "[" pipeline "]" ;

source_command  = search | describe | makeresults | multisearch | union
                | "show" "datasources" | "rest" string ... | "graphlookup" ... ;
search          = [ "search" ] { search_item } ;          (* exactly one source clause *)
search_item     = ( "source" | "index" ) "=" source_clause
                | search_expr ;
source_clause   = ( subsearch | source_ref { "," source_ref } ) [ "as" name ] ;
source_ref      = string | quoted_ident | adjacent name characters (logs-2025.01.*, cluster:idx) ;
describe        = "describe" source_ref ;
makeresults     = "makeresults" { ( "count" "=" integer | "format" "=" ident | "data" "=" string ) } ;
multisearch     = "multisearch" subsearch subsearch { subsearch } ;
union           = "union" [ "maxout" "=" integer ]
                  ( subsearch | source_ref ) { [ "," ] ( subsearch | source_ref ) } ;
```

## Search expressions

```ebnf
search_expr     = search_or { [ "AND" ] search_or } ;        (* adjacency is AND *)
search_or       = search_not { "OR" search_not } ;
search_not      = "NOT" search_not | search_primary ;
search_primary  = "(" search_expr ")"
                | ( "earliest" | "latest" ) "=" time_modifier
                | field search_op search_value
                | field "IN" "(" search_value { "," search_value } ")"
                | search_value ;                           (* free text *)
search_op       = "=" | "==" | "!=" | "<" | "<=" | ">" | ">=" ;
search_value    = string | quoted_ident
                | adjacent term characters (letters, digits, - _ . : * ? @ / +) ;
time_modifier   = "now" [ "(" ")" ] | integer | decimal | string | snap
                | ( "+" | "-" ) offset_chain ;             (* -7d, +1d@d, -1mon@mon-10h *)
```

## Commands

```ebnf
command    = where | fields | rename | replace | eval | stats | dedup | sort
           | "reverse" | head | bin | top | parse | rex | regex | spath
           | patterns | lookup | fillnull | convert | trendline | field_command
           | append | join | totals | chart | transpose | xyseries | union
           | unsupported_command ;

where      = "where" expr ;
fields     = ( "fields" | "table" ) [ "+" | "-" ] wc_field { [ "," ] wc_field } ;
rename     = "rename" wc_field "as" wc_field { [ "," ] wc_field "as" wc_field } ;
replace    = "replace" string "with" string { "," string "with" string } "in" field_list ;
eval       = ( "eval" | "fieldformat" ) field "=" expr { "," field "=" expr } ;

stats      = ( "stats" | "eventstats" | "streamstats" ) { stats_arg }
             agg_term { "," agg_term } [ "by" by_list ] { stats_arg }
             [ "dedup_splitvalues" "=" value ] ;
stats_arg  = name "=" value ;   (* stats: partitions allnum delim bucket_nullable;
                                   eventstats: bucket_nullable;
                                   streamstats: current window global reset_before
                                   reset_after bucket_nullable time_window *)
agg_term   = ( "count" | "c" | function_call ) [ "as" wc_field ] ;
by_list    = by_item { [ "," ] by_item } ;
by_item    = span_expr | field ;
span_expr  = "span" "(" [ field "," ] span_literal ")" [ "as" field ] ;

dedup      = "dedup" [ integer ] field_list
             { ( "keepempty" | "consecutive" ) "=" bool } ;
sort       = "sort" [ integer ] sort_key { "," sort_key } ;
sort_key   = [ "+" | "-" ] ( ( "auto" | "str" | "ip" | "num" ) "(" field ")" | field )
             [ "asc" | "a" | "desc" | "d" ] ;
head       = "head" [ "limit" "=" integer | integer ] [ "from" integer ] ;
bin        = "bin" field { ( "span" | "bins" | "minspan" | "aligntime" | "start" | "end" ) "=" value }
             [ "as" field ] ;
top        = ( "top" | "rare" ) [ "limit" "=" integer | integer ] { top_arg }
             field_list [ "by" field_list ] { top_arg } ;
top_arg    = ( "countfield" | "showcount" | "percentfield" | "showperc" | "usenull" ) "=" value ;

parse      = ( "parse" | "grok" ) additive_expr string ;
rex        = "rex" [ "field" "=" field ] { rex_arg } string { rex_arg } [ "field" "=" field ] ;
rex_arg    = ( "max_match" | "mode" | "offset_field" ) "=" value ;
regex      = "regex" field ( "=" | "!=" ) string ;
spath      = "spath" { "input" "=" field | "output" "=" field | "path" "=" path | path } ;
patterns   = "patterns" { patterns_arg } additive_expr [ "by" field_list ] { patterns_arg } ;
patterns_arg = ( "method" | "mode" | "max_sample_count" | "buffer_limit" | "show_numbered_token"
             | "pattern" | "new_field" | "variable_count_threshold"
             | "frequency_threshold_percentage" ) "=" value ;
lookup     = "lookup" source_ref lookup_pair { "," lookup_pair }
             [ ( "replace" | "append" | "output" ) lookup_pair { "," lookup_pair } ] ;
lookup_pair = field [ "as" field ] ;
fillnull   = "fillnull" ( "with" additive_expr [ "in" field_list ]
                        | "using" field "=" additive_expr { "," field "=" additive_expr }
                        | "value" "=" additive_expr [ field_list ] ) ;
convert    = "convert" [ "timeformat" "=" string ]
             ident "(" field ")" [ "as" field ] { [ "," ] ident "(" field ")" [ "as" field ] } ;
trendline  = "trendline" [ "sort" sort_key ]
             ( "sma" | "wma" ) "(" integer "," field ")" [ "as" field ]
             { ( "sma" | "wma" ) "(" integer "," field ")" [ "as" field ] } ;
field_command = "expand" field [ "as" field ]
             | "flatten" field [ "as" ( "(" field_names ")" | field_names ) ]
             | "mvexpand" field [ "limit" "=" integer ]
             | "mvcombine" field [ "delim" "=" string ]
             | "nomv" field ;
append     = "append" "[" pipeline "]"                    (* a full pipeline with a source *)
           | ( "append" | "appendpipe" ) "[" [ "|" ] command { "|" command } "]"
           | "appendcol" [ "override" "=" bool ] "[" [ "|" ] command { "|" command } "]" ;
join       = [ "inner" | "cross" | "left" [ "outer" | "semi" | "anti" ] | "outer"
             | "right" [ "outer" ] | "full" [ "outer" ] | "semi" | "anti" ] "join"
             { ( "overwrite" | "type" | "max" ) "=" value }
             [ ( "left" | "right" ) "=" name { [ "," ] ( "left" | "right" ) "=" name } ]
             ( ( "on" | "where" ) expr join_right
             | field_list ( subsearch [ "as" name ] | source_ref [ "as" name ] ) ) ;
join_right = ( subsearch | source_ref ) [ "as" name ] ;
totals     = "addtotals" { ( "label" | "labelfield" | "fieldname" | "row" | "col" ) "=" value } [ field_list ]
           | "addcoltotals" { ( "label" | "labelfield" ) "=" value } [ field_list ] ;
chart      = "chart" { chart_arg } agg_term
             [ "over" field { bin_arg } [ "by" field { bin_arg } ]
             | "by" field { bin_arg } [ [ "," ] field { bin_arg } ] ] { chart_arg }
           | "timechart" { timechart_arg } agg_term [ "by" field ] { timechart_arg } ;
chart_arg  = ( "limit" | "useother" | "otherstr" | "usenull" | "nullstr" ) "=" value ;
timechart_arg = ( "limit" | "span" | "useother" | "usenull" | "nullstr" | "timefield"
             | "bins" | "minspan" ) "=" value ;
bin_arg    = ( "span" | "bins" | "minspan" | "start" | "end" ) "=" value ;
transpose  = "transpose" { integer | "column_name" "=" string } ;
xyseries   = "xyseries" { ( "sep" | "format" ) "=" value }
             field field "in" "(" string { "," string } ")" field_list ;
unsupported_command = ( "kmeans" | "ad" | "ml" | "graphlookup" | "foreach" | "timewrap" ) ... ;

field_list = field { [ "," ] field } ;
wc_field   = "*" | string | field ;                       (* names may contain * *)
```

## Expressions

```ebnf
expr          = or_expr ;
or_expr       = xor_expr { "OR" xor_expr } ;
xor_expr      = and_expr { "XOR" and_expr } ;
and_expr      = not_expr { "AND" not_expr } ;
not_expr      = "NOT" not_expr | predicate ;
predicate     = additive { compare_op additive
                         | [ "NOT" ] "IN" ( "(" [ expr { "," expr } ] ")" | subsearch )
                         | [ "NOT" ] "BETWEEN" additive "AND" additive
                         | "IS" [ "NOT" ] "NULL" } ;
compare_op    = "=" | "==" | "!=" | "<>" | "<" | "<=" | ">" | ">="
              | "LIKE" | "ILIKE" | "REGEXP" | "CONTAINS" ;
additive      = multiplicative { ( "+" | "-" ) multiplicative } ;
multiplicative = unary { ( "*" | "/" | "%" ) unary } ;
unary         = ( "-" | "+" ) unary | primary ;
primary       = literal | "*" | field | call | "(" expr ")"
              | "(" expr "," expr { "," expr } ")" [ "NOT" ] "IN" subsearch
              | "EXISTS" subsearch
              | subsearch                                 (* scalar subquery *)
              | "INTERVAL" additive interval_unit
              | ( "DATE" | "TIME" | "TIMESTAMP" ) string ;
call          = name "(" [ arg { "," arg } ] ")"
              | "cast" "(" expr "as" type ")"
              | "case" "(" expr "," expr { "," expr "," expr } [ [ "," ] "else" expr ] ")"
              | "position" "(" additive "in" additive ")"
              | "extract" "(" ident "from" expr ")"
              | ( "timestampadd" | "timestampdiff" ) "(" ident "," expr "," expr ")"
              | "get_format" "(" ident "," expr ")" ;
arg           = lambda | expr | name "=" expr ;           (* named arguments only for relevance functions *)
lambda        = ident "->" expr | "(" ident { "," ident } ")" "->" expr ;
field         = name_part { "." name_part } ;             (* no spaces around the dots *)
name_part     = ident | quoted_ident | integer ;
interval_unit = "MICROSECOND" | "MILLISECOND" | "SECOND" | "MINUTE" | "HOUR" | "DAY" | "WEEK"
              | "MONTH" | "QUARTER" | "YEAR" | "SECOND_MICROSECOND" | "MINUTE_MICROSECOND"
              | "MINUTE_SECOND" | "HOUR_MICROSECOND" | "HOUR_SECOND" | "HOUR_MINUTE"
              | "DAY_MICROSECOND" | "DAY_SECOND" | "DAY_MINUTE" | "DAY_HOUR" | "YEAR_MONTH" ;
type          = "int" | "integer" | "long" | "bigint" | "short" | "byte" | "double" | "float"
              | "string" | "boolean" | "timestamp" | "date" | "time" | "ip" | "json" ;
```

`count` and `c` written without parentheses in an aggregation are
`count()`. A percentile shortcut such as `p99` or `p99.5` is an aggregation
name (`pNN`, `percNN`). `count(eval(cond))` counts rows where `cond` is TRUE.

## Lexical grammar

```ebnf
ident         = ( letter | "_" | "@" | "*" ) { letter | digit | "_" | "-" | "*" } ;  (* no trailing "-" *)
quoted_ident  = "`" { char | "``" | "\`" } "`" ;
string        = "'" { char | "''" | escape } "'" | '"' { char | '""' | escape } '"' ;
integer       = digit { digit } ;
decimal       = digit { digit } "." digit { digit } | "." digit { digit } | digit { digit } ( "e" | "E" ) [ "+" | "-" ] digit { digit } ;
double        = decimal ( "d" | "D" ) ;
float         = decimal ( "f" | "F" ) ;
span_literal  = ( integer | decimal ) span_unit | integer | decimal | string ;
snap          = "@" snap_unit ;                            (* @d, @mon, @w0 .. @w7 *)
comment       = "//" { char } newline | "/*" { char } "*/" ;
```

The parser stops at the first syntax error and reports its line, column and
the tokens it expected (see [Error handling](./errors.md)).
