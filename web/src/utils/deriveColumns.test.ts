import { describe, it, expect } from "vitest";
import {
  deriveColumns,
  deriveColumnsMemo,
  deriveColumnsFromRows,
  deriveColumnsFromEvents,
} from "./deriveColumns";
import type {
  EventsResult,
  AggregateResult,
} from "../api/client";

describe("deriveColumnsFromRows", () => {
  it("returns empty array for empty input", () => {
    expect(deriveColumnsFromRows([])).toEqual([]);
  });

  it("returns priority columns first then alphabetical rest", () => {
    const rows = [
      { z: 1, _time: "t", a: 2, _raw: "r", source: "s", _source: "x" },
    ];
    expect(deriveColumnsFromRows(rows)).toEqual([
      "_time",
      "_raw",
      "_source",
      "source",
      "a",
      "z",
    ]);
  });

  it("handles rows with different keys (union across rows)", () => {
    const rows = [
      { _time: "t1", host: "h1" },
      { _time: "t2", service: "s1" },
    ];
    expect(deriveColumnsFromRows(rows)).toEqual(["_time", "host", "service"]);
  });

  it("scans all rows when count <= 50 (PERF-03)", () => {
    // Row 51 would be outside the 50-row threshold but we have exactly 50 rows
    const rows: Record<string, unknown>[] = [];
    for (let i = 0; i < 50; i++) {
      rows.push({ common: i });
    }
    // Add a unique field at the very last row (index 49)
    rows[49] = { common: 49, rare_field: "x" };
    const cols = deriveColumnsFromRows(rows);
    expect(cols).toContain("rare_field");
  });

  it("caps sampling at 100 rows for large results (PERF-03)", () => {
    const rows: Record<string, unknown>[] = [];
    for (let i = 0; i < 200; i++) {
      rows.push({ common: i });
    }
    // Field only present beyond the 100-row sample window
    rows[150] = { common: 150, hidden_field: "y" };
    const cols = deriveColumnsFromRows(rows);
    // hidden_field should NOT be discovered (beyond sample window)
    expect(cols).not.toContain("hidden_field");
    expect(cols).toContain("common");
  });
});

describe("deriveColumnsFromEvents", () => {
  it("delegates to deriveColumnsFromRows", () => {
    const events = [{ _time: "t", level: "info" }];
    expect(deriveColumnsFromEvents(events)).toEqual(["_time", "level"]);
  });
});

describe("deriveColumns", () => {
  it("handles events result", () => {
    const result: EventsResult = {
      type: "events",
      events: [{ _time: "t", host: "h" }],
      total: 1,
      has_more: false,
    };
    expect(deriveColumns(result)).toEqual(["_time", "host"]);
  });

  it("returns columns as-is for aggregate result", () => {
    const result: AggregateResult = {
      type: "aggregate",
      columns: ["count", "avg_duration"],
      rows: [[5, 123.4]],
      total_rows: 1,
    };
    expect(deriveColumns(result)).toEqual(["count", "avg_duration"]);
  });

  it("returns columns as-is for timechart result", () => {
    const result: AggregateResult = {
      type: "timechart",
      columns: ["_time", "count"],
      rows: [["2026-01-01", 42]],
      total_rows: 1,
    };
    expect(deriveColumns(result)).toEqual(["_time", "count"]);
  });
});

describe("deriveColumnsMemo", () => {
  it("returns same reference for same result object", () => {
    const result: EventsResult = {
      type: "events",
      events: [{ _time: "t", a: 1 }],
      total: 1,
      has_more: false,
    };
    const first = deriveColumnsMemo(result);
    const second = deriveColumnsMemo(result);
    expect(first).toBe(second); // same reference
  });

  it("returns different arrays for different result objects with same data", () => {
    const r1: EventsResult = {
      type: "events",
      events: [{ _time: "t" }],
      total: 1,
      has_more: false,
    };
    const r2: EventsResult = {
      type: "events",
      events: [{ _time: "t" }],
      total: 1,
      has_more: false,
    };
    const c1 = deriveColumnsMemo(r1);
    const c2 = deriveColumnsMemo(r2);
    expect(c1).toEqual(c2);
    // They are structurally equal but not reference-equal since they come from different objects
    expect(c1).not.toBe(c2);
  });
});
