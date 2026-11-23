---
title: IR specification
sidebar_label: IR specification
description: The canonical intermediate representation between query languages and the LynxDB engine.
---

# IR specification

The canonical IR (`pkg/ir`) is the contract between query-language
frontends and the LynxDB engine. A plan is a tree of logical operators
(`ir.Node`); scalar computations are typed expressions (`ir.Expr`). The IR
names semantic database operations, not the syntax of any language, and it
does not depend on storage formats.

## Types

| IR type | Values |
| --- | --- |
| `any` | A value whose type is decided per row (schema-on-read columns). |
| `null` | The type of the NULL literal; unifies with every type. |
| `bool` | TRUE, FALSE. |
| `int` | 64-bit signed integer. |
| `float` | 64-bit IEEE 754 float. |
| `string` | UTF-8 text. |
| `timestamp` | An instant with nanosecond precision, UTC. |
| `duration` | A signed span of time; also used for times of day. |
| `array<T>` | Ordered list. |
| `object` | String-keyed map. |

`Unify` gives the common type of CASE branches, COALESCE arguments and
UNION columns: equal types stay, NULL yields the other type, int and float
widen to float, everything else becomes `any`. A float parameter accepts an
int argument; `any` and `null` arguments are accepted everywhere and checked
at run time.

## Schemas

Every operator exposes its output schema: columns with name, type,
nullability and optional metadata. A closed schema lists every column; an
open schema lists the columns known at planning time and allows further
columns of type `any`.

## NULL, missing and three-valued logic

- A reference to a column absent from a row evaluates to NULL.
  `IsMissing(col)` is TRUE only for an absent column; `is_null(x)` is TRUE
  for NULL or absent.
- Comparisons and arithmetic with a NULL operand yield NULL.
- AND, OR, XOR and NOT use Kleene logic.
- `Filter` keeps a row only when its condition is TRUE.
- `InList` is TRUE on a match, NULL when there is no match and the tested
  value or an element is NULL, FALSE otherwise.
- Integer division and modulo by zero yield NULL.

## Run-time conversions

Values of type `any` meet each other at run time. The binary operators
apply these rules:

- int with int is int; int with float is float.
- Adding two strings concatenates them.
- A string meeting a number in arithmetic or a comparison is converted to a
  number; it yields NULL when it does not parse.
- A string compared with a timestamp is converted to a timestamp; it yields
  NULL when it does not parse.
- timestamp - timestamp is a duration; timestamp ± duration is a timestamp.
- Other incompatible combinations yield NULL.

`Cast` converts explicitly; a value that cannot be converted yields NULL.
Casting a float to int truncates toward zero; casting a string to bool
accepts true and false in any case; casting a number to a timestamp reads
epoch seconds.

## Expressions

| Expression | Meaning |
| --- | --- |
| `Literal` | A constant. |
| `FieldRef` | A column; a dotted name reads the column with exactly that name, else walks object members. |
| `Unary`, `Binary` | Negation, NOT; arithmetic, comparisons, AND, OR, XOR. |
| `Call` | A registered scalar function. |
| `Case` | First branch whose condition is TRUE, else the ELSE value or NULL. |
| `Cast` | Explicit conversion. |
| `ArrayExpr`, `ObjectExpr`, `Index` | Construction and element access. |
| `Lambda`, `LambdaParam` | Anonymous functions, only as arguments of higher-order functions. |
| `InList` | Membership test. |
| `IsMissing` | Column absence. |
| `RelativeTime` | An instant relative to the query's reference time (`now-1d@d`). |
| `Search` | Text predicate; see below. |
| `Subquery` | Uncorrelated EXISTS, IN or scalar subquery. |
| `AggCall` | Aggregate or window function application, only in `Aggregate` and `Window`. |

### Search predicates

`Search{Mode, Field, Pattern, CaseSensitive}` keeps the distinctions that
let a scan leave out data. `Field` empty means a free-text search of the
whole event.

| Mode | Matches | Columnar executor |
| --- | --- | --- |
| word | every token of the pattern as a whole token | token filters, then per row |
| prefix | a token starting with the pattern | per row |
| glob | a token matching `*`/`?` wildcards | a single trailing `*` as a prefix; other patterns run on the row operators |
| phrase | the pattern at token boundaries | token filters on the phrase's tokens, then per row |
| substring | the pattern anywhere | per row |
| regex | an RE2 expression | per row |

## Operators

| Operator | Semantics |
| --- | --- |
| `Scan` | Reads events of sources (names or `*` patterns) within an exact time range. Carries annotations: pruning predicates, required columns, a row limit, descending order. Annotations never change results. |
| `Values` | Literal rows. |
| `Empty` | No rows. |
| `SourceSchema` | One row per column of a source. |
| `Filter` | Keeps rows whose condition is TRUE. |
| `Project` | Keeps (in listed order) or drops columns; names may contain `*`. |
| `RenameColumns` | Renames columns, with `*` captures. |
| `Compute` | Adds or replaces columns; assignments see earlier ones. |
| `Aggregate` | Groups by expressions and computes aggregates; output is groups then aggregates; one row without groups. Rows without a group value form a group whose key is NULL; `DropNullGroups` drops them. |
| `Window` | Aggregates per row over the whole partition or a running frame (row bound, current-row exclusion). |
| `Sort`, `TopN`, `Limit`, `Reverse` | Ordering with explicit NULL placement; bounded heap top-N; offset and count; reversal. |
| `Dedup` | First N rows per key, optional NULL keys, optional consecutive mode. |
| `Join` | inner, left, right, full, semi, anti, cross; equality keys plus residual condition; per-row match cap; collision rule; optional qualified copies of each side's columns. |
| `Union` | Concatenation by column name, or newest-first merge. |
| `ZipColumns` | Positional pairing of two inputs. |
| `Extract` | Regex captures or JSON members from a string expression. |
| `Patterns` | Log pattern labeling or aggregation. |
| `Unnest` | One row per array element. |
| `FlattenObject` | Object members as columns. |
| `FillNull` | NULL replacement per column or for all columns. |
| `Transpose`, `Totals`, `CombineValues` | Reshaping operators. |

## Function registry

`ir.Funcs()` lists every function with its kind (scalar, aggregate,
window), category, parameters (type, constant-only, lambda arity), result
rule, NULL behaviour (strict, aware, skipped), volatility (volatile, query
constant), merge kind for partial aggregation (exact, approximate, none),
order sensitivity and documentation. `Foldable` functions with constant
arguments are folded at planning time. Every scalar function has one
implementation, in `pkg/colstore/exec/irfunc`, which the columnar executor
and the row operators both run; the functions that take a lambda are
computed by `pkg/colstore/exec/irplan`.

## Validation

`ir.Validate` checks operator shapes, column resolution against closed
input schemas, function kinds, arity and static argument types, constant
arguments, aggregate placement, lambda scoping and subquery shapes.

## Optimizer

`pkg/ir/opt` applies, to a fixed point:

- constant folding of deterministic calls, through an evaluator the caller
  supplies;
- merging adjacent filters and removing `TRUE` filters;
- copying indexable conjuncts (free-text searches, column equals string,
  disjunctions of words) into `Scan.Prune`, keeping the filter;
- narrowing `Scan.Time` with constant comparisons of the time column;
- `Sort` + `Limit` into `TopN`;
- pushing a row bound into `Scan.Limit` through `Compute`, `Project` and
  `RenameColumns`;
- recording the required columns on each scan.

See [IR optimizer](./ir-optimizer.md) for each rule.

## Execution

The IR plan is what both executors run. The columnar executor
(`pkg/colstore/exec/irplan` translates the plan) takes the chain of
operators from a scan upwards that it can run; the row operators
(`pkg/engine/physical`) run the rest. See
[Pipeline execution](../ppl/pipeline-execution.md).

`physical.PlanHints` (`pkg/engine/physical/hints.go`) summarizes the scans
of an optimized plan: sources, the resolved time range, search terms and
token globs from free-text `Search` pruning predicates, field predicates for
fields equal to strings, required columns, the row limit and the scan
order. A plan with one scan gets all of them; a plan with several scans
gets only the union of their sources and time ranges. `GET
/api/v1/query/explain` reports this summary.
