---
title: PPL overview
sidebar_label: Overview
description: LynxDB's query language is OpenSearch Piped Processing Language, compiled to a language-independent IR.
---

# PPL overview

LynxDB queries are written in OpenSearch Piped Processing Language (PPL). A
query is a pipeline: a source command reads events, and each command after a
`|` transforms the rows the previous command produced.

```
source=main status>=500 earliest=-1h
| stats count() as errors, p95(response_time) as p95 by host
| sort - errors
| head 5
```

The same text runs everywhere a query is accepted:

| Entry point | What runs the query |
| --- | --- |
| `lynxdb query '<query>'` | the server, over REST |
| `lynxdb query --file app.log '<query>'`, `cat app.log \| lynxdb query --file - '<query>'` | the CLI process, over the input loaded into the index `main` |
| `POST /api/v1/query`, `/api/v1/query/stream` | the server |
| `lynxdb explain '<query>'`, `GET /api/v1/query/explain` | the server's compiler, without running the query |

Every entry point compiles the text with the same compiler (`pkg/query`) into
the same plan, so a query that works on a file works on the server.

## How LynxDB relates to OpenSearch PPL

The PPL pages keep three things apart:

1. **OpenSearch PPL semantics.** Syntax, commands, functions and their
   documented behavior follow the OpenSearch PPL reference. Where LynxDB
   implements a feature fully, the OpenSearch documentation applies.
2. **LynxDB execution behavior.** LynxDB stores schema-on-read log events,
   not OpenSearch documents. Free-text search matches the tokens of every
   string field of an event, every index is local, and there are no
   relevance scores. Where this changes a result, the pages say so under a
   *LynxDB behavior* heading.
3. **OpenSearch features LynxDB does not support.** Unsupported commands,
   functions and options fail at compile time with an `UNSUPPORTED_COMMAND`,
   `UNSUPPORTED_FUNCTION` or `UNSUPPORTED_FEATURE` error. Nothing unsupported
   is silently approximated. The [feature matrix](./feature-matrix.md) lists
   them.

## Where to go next

| Topic | Page |
| --- | --- |
| First queries | [Getting started](./getting-started.md) |
| Pipelines, identifiers, literals, precedence, comments | [Syntax](./syntax.md) |
| The grammar as implemented | [Grammar](./grammar.md) |
| Types and conversions | [Data types](./data-types.md) |
| Operators, NULL handling, conditions | [Expressions](./expressions.md) |
| Free-text and field search | [Search semantics](./search-and-types.md) |
| The main commands | [Command guide](./command-guide.md), [command support table](./commands.md) |
| Scalar functions | [Function guide](./function-guide.md), [function table](./functions.md) |
| Aggregations | [Aggregation guide](./aggregation-guide.md), [aggregation table](./aggregations.md) |
| Time series | [Time series](./time-series.md) |
| Joins, lookups, subsearches | [Joins and subsearches](./joins-and-subsearches.md) |
| What is and is not supported | [Feature matrix](./feature-matrix.md) |
| Error codes and positions | [Error handling](./errors.md) |
| Worked queries | [Examples](./examples.md) |
| How a plan runs; making queries fast | [Pipeline execution](./pipeline-execution.md), [Performance](./performance.md) |
| Compiler internals | [Architecture](../contributing/ppl-architecture.md), [PPL-to-IR compilation](../contributing/ppl-compilation.md), [IR specification](../contributing/ir.md), [IR optimizer](../contributing/ir-optimizer.md) |
| Extending the language | [Adding commands](../contributing/adding-commands.md), [Adding functions](../contributing/adding-functions.md) |

The command, function and aggregation tables are generated from the
catalogs in `pkg/ppl` by `go run ./internal/docgen`. `GET /api/v1/catalog`
returns the same catalogs as JSON.
