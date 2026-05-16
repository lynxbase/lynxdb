import { describe, it, expect } from "vitest";
import { filterNdjsonDataRows } from "./ndjsonFilter";

describe("filterNdjsonDataRows", () => {
  it("returns data rows and drops __meta control lines", () => {
    const input = [
      '{"_time":"2026-01-01","level":"error","msg":"fail"}',
      '{"__meta":{"total":1,"scanned":10,"took_ms":5}}',
      '{"_time":"2026-01-02","level":"info","msg":"ok"}',
    ].join("\n");
    const rows = filterNdjsonDataRows(input);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({
      _time: "2026-01-01",
      level: "error",
      msg: "fail",
    });
    expect(rows[1]).toEqual({
      _time: "2026-01-02",
      level: "info",
      msg: "ok",
    });
  });

  it("drops __error control lines", () => {
    const input = [
      '{"_time":"2026-01-01","level":"error"}',
      '{"__error":{"message":"timeout exceeded"}}',
    ].join("\n");
    const rows = filterNdjsonDataRows(input);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual({ _time: "2026-01-01", level: "error" });
  });

  it("drops lines with both __meta and __error", () => {
    const input = '{"__meta":{"total":0}}\n{"__error":{"message":"bad"}}';
    expect(filterNdjsonDataRows(input)).toHaveLength(0);
  });

  it("skips malformed JSON lines gracefully", () => {
    const input = [
      '{"_time":"2026-01-01","ok":true}',
      "not valid json {{{",
      '{"_time":"2026-01-02","ok":true}',
    ].join("\n");
    const rows = filterNdjsonDataRows(input);
    expect(rows).toHaveLength(2);
  });

  it("handles empty input", () => {
    expect(filterNdjsonDataRows("")).toHaveLength(0);
    expect(filterNdjsonDataRows("   ")).toHaveLength(0);
  });

  it("handles trailing newlines and blank lines", () => {
    const input = '\n{"a":1}\n\n{"b":2}\n\n';
    const rows = filterNdjsonDataRows(input);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({ a: 1 });
    expect(rows[1]).toEqual({ b: 2 });
  });

  it("keeps rows that happen to have other __ prefixed keys", () => {
    const input = '{"__custom":"value","data":1}';
    const rows = filterNdjsonDataRows(input);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual({ __custom: "value", data: 1 });
  });
});
