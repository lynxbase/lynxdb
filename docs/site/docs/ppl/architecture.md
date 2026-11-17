---
title: PPL architecture
sidebar_label: Architecture
description: How a PPL query becomes canonical IR, an optimized plan and a pipeline of operators.
---

# PPL architecture

LynxDB executes OpenSearch Piped Processing Language (PPL). The query
language and the database engine meet at one boundary: the canonical
intermediate representation (IR) in `pkg/ir`. The PPL frontend produces IR;
the optimizer and both executors consume IR and know nothing about PPL
syntax.

```
query text
    |
    v
pkg/ppl/lexer        tokens with line and column
    |
    v
pkg/ppl/parser       syntax tree (pkg/ppl/ast) with source spans
    |
    v
pkg/ppl              semantic analysis: field resolution against each
                     command's output schema, function resolution through
                     the PPL catalogs, static type checks, command lowering
    |
    v
pkg/ir               canonical IR plan, validated by ir.Validate
    |
    v
pkg/ir/opt           rule-based optimizer: constant folding, filter merging,
                     scan pruning predicates, time ranges, TopN, scan
                     limits, column pruning
    |
    v
pkg/database         runs the plan as a query or a job, narrowed to the
                     request's time range
    |
    v
pkg/engine/columnar  gives the columnar executor (pkg/colstore/exec, plans
                     translated by pkg/colstore/exec/irplan) the largest
                     part of the plan above each scan that it can run
    |
    v
pkg/engine/physical  row operators for the rest: IR operators to pipeline
pkg/engine/pipeline  iterators, IR expressions to VM bytecode (pkg/vm),
                     subquery evaluation
    |
    v
results
```

`pkg/query` ties the compiling stages together: `query.Compile` runs the
frontend and the optimizer, and `query.Explain` renders the plans. The CLI
and the server compile with the same `query.Compile` and run the plan with
the same `pkg/database`; they differ only in where the indexes are.

## Packages

| Package | Responsibility |
| --- | --- |
| `pkg/ppl/lexer` | Tokenizes PPL text. Keywords are case-insensitive identifiers; identifiers may contain `-` and `*`, as in the official grammar. |
| `pkg/ppl/ast` | Syntax tree of PPL queries: commands, search expressions, value expressions, with source spans. |
| `pkg/ppl/parser` | Recursive-descent parser over the official PPL grammar; reports the first syntax error with line, column and expected tokens. |
| `pkg/ppl/grok` | Grok pattern library; expands grok patterns to RE2 expressions at compile time. |
| `pkg/ppl` | Semantic analysis and lowering to IR; the PPL command catalog (`Commands`), scalar function catalog (`ScalarFunctions`) and aggregation catalog (`AggregateFunctions`); structured errors. |
| `pkg/ir` | IR types, schemas, expressions, operators, function registry, validation and rendering. |
| `pkg/ir/opt` | Optimizer rules over IR. |
| `pkg/vm` | Bytecode VM; `vm.CompileExpr` compiles IR expressions, with native implementations of every registry function. |
| `pkg/query` | Compile (with a cache of recent plans), optimize and explain. |
| `pkg/database` | Runs compiled queries over the indexes, as calls or as jobs. |
| `pkg/engine/columnar` | Splits a plan between the columnar executor and the row operators and collects the result. |
| `pkg/colstore/exec/irplan` | Translates the IR operators the columnar executor runs, and refuses the others. |
| `pkg/colstore/exec`, `pkg/colstore/exec/irfunc` | The columnar executor and its scalar functions. |
| `pkg/engine/physical` | Row operators: IR to pipeline iterators; the plan summary `explain` reports. |
| `pkg/engine/pipeline` | Pipeline iterators over batches of rows. |
| `internal/docgen` | Generates the command, function and aggregation tables from the catalogs. |

## Semantic analysis

Each PPL command is lowered with the schema its input produces. A field
reference is resolved against that schema:

- Schemas of events read from an index are open: the time column `_time`
  and the raw text `_raw` are typed, and any other column may exist with a
  type known only per row. References to unknown columns are allowed and
  read NULL on rows that lack the column.
- Commands such as `stats`, `fields` and `top` produce closed schemas. A
  reference to a column a closed schema does not have is a semantic error
  reported at the reference, listing the available columns.

The OpenSearch time field name `@timestamp` resolves to LynxDB's event time
column `_time`.

Functions resolve through the PPL scalar catalog, which maps each PPL name
either onto a function of the IR registry (often under the same name), onto
IR operators (for example `if` becomes `CASE`, `isnull` becomes `is_null`,
`match` becomes a field search), or onto an explicit unsupported error.
Aggregations resolve through the aggregation catalog. Static types are
checked against the registry's parameter types; values whose types are only
known per row are converted at run time by the rules in the
[IR specification](./ir.md). [PPL-to-IR compilation](./compilation.md)
shows how each command lowers.

## Errors

Compilation errors carry a kind, a position and an optional hint:

| Kind | Meaning |
| --- | --- |
| `syntax` | The query does not match the PPL grammar. |
| `semantic` | Unknown field, invalid option value, or a command used inconsistently. |
| `type` | Incompatible operand types. |
| `unsupported_command` | A PPL command LynxDB does not execute. |
| `unsupported_function` | A PPL function LynxDB does not execute, or an unknown function. |
| `unsupported_feature` | A command option or language feature LynxDB does not execute. |

The row operators report the few constructs they cannot run with
`physical.UnsupportedError`, whose message starts with `not supported by
the execution engine:`; nothing unsupported is silently approximated. A
construct the columnar executor does not take is not an error: the row
operators run it. [Error handling](./errors.md) maps the kinds to API
codes.

## Limits and plan reuse

The parser rejects, with a `syntax` error, a query longer than 256 KiB, a
nesting of parentheses, `NOT`, signs, function calls, search groups and
subsearches deeper than 256 levels, and a pipeline of more than 1000
commands. Within these bounds compilation takes time linear in the query
size, apart from field lookups in schemas of thousands of columns.

Compiling depends on the query text alone: relative times stay symbolic
until execution and source columns are discovered at read time. `query.Compile`
therefore keeps the plans of the 1024 most recently used queries of up to
2 KiB and returns a copy of the cached result for a repeated text.

## Extending

See [Adding commands](./adding-commands.md) and
[Adding functions](./adding-functions.md).
