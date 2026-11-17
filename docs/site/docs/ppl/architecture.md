---
title: PPL architecture
sidebar_label: Architecture
description: How a PPL query becomes canonical IR, an optimized plan and a pipeline of operators.
---

# PPL architecture

LynxDB executes OpenSearch Piped Processing Language (PPL). The query
language and the database engine meet at one boundary: the canonical
intermediate representation (IR) in `pkg/ir`. The PPL frontend produces IR;
the optimizer, the physical planner and the execution engine consume IR and
know nothing about PPL syntax.

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
                     the PPL catalog, static type checks, command lowering
    |
    v
pkg/ir               canonical IR plan, validated by ir.Validate
    |
    v
pkg/ir/opt           rule-based optimizer: constant folding, filter merging,
                     scan pruning hints, time ranges, TopN, limit and
                     column pushdown
    |
    v
pkg/engine/physical  physical planner: IR operators to pipeline iterators,
                     IR expressions to VM bytecode (pkg/vm), subquery
                     evaluation
    |
    v
pkg/engine/pipeline  batch iterators reading from the storage engine
    |
    v
results
```

`pkg/query` ties the stages together. `query.Compile` runs the frontend and
the optimizer; `query.Run` builds and drains the physical plan over an
in-memory event store, which is what the CLI's file and pipe modes use. The
server compiles with the same `query.Compile` and differs only in the scan
source it hands to the physical planner.

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
| `pkg/engine/physical` | Physical planner from IR to pipeline iterators; storage hints derived from scans. |
| `pkg/query` | Compile, optimize, explain and run. |

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
IR operators (for example `if` becomes `CASE`, `isnull` becomes `is_null`),
or onto an explicit unsupported error. Aggregations resolve through the
aggregation catalog. Static types are checked against the registry's
parameter types; values whose types are only known per row are converted at
run time by the rules in the [IR specification](./ir.md).

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

The physical planner reports constructs the engine cannot run with
`physical.UnsupportedError`; nothing unsupported is silently approximated.

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

## Adding a command

1. Add the syntax to `pkg/ppl/ast` and a parse function to
   `pkg/ppl/parser/commands.go`, dispatched from `parser.command`.
2. Lower it in `pkg/ppl/commands*.go` to existing IR operators. Only when no
   composition of operators expresses its semantics add an IR operator to
   `pkg/ir/plan.go` (with its schema, label, validation in
   `pkg/ir/validate.go` and expression listing in `pkg/ir/walk.go`) and its
   physical mapping to `pkg/engine/physical`.
3. Record its support level in the command catalog, `pkg/ppl/catalog.go`.
4. Add a compile golden under `pkg/ppl/testdata/compile` and an execution
   case in `pkg/query/query_test.go`.

## Adding a function

1. Register the function in the IR registry (`pkg/ir/funcs_catalog.go`) with
   its parameters, result rule, NULL behaviour and flags.
2. Implement it in `pkg/vm/ir_funcs*.go`; `TestIRRegistryHasImplementations`
   fails until every registered scalar function has an implementation.
3. Map the PPL name in `pkg/ppl/functions.go`, directly or with a lowering.
4. Add evaluation cases to `pkg/vm/ir_compiler_test.go`.
