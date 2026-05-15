import { describe, it, expect } from "vitest";
import { extractDiagnostics } from "./diagnostics";
import type { ExplainResult } from "../api/client";

function explain(
  errors: { message: string; suggestion?: string }[],
): ExplainResult {
  return { is_valid: false, errors };
}

describe("extractDiagnostics", () => {
  it("returns nothing for a valid query", () => {
    expect(extractDiagnostics("foo", { is_valid: true })).toEqual([]);
    expect(extractDiagnostics("foo", explain([]))).toEqual([]);
  });

  it("maps an in-range position to a one-char marker", () => {
    const d = extractDiagnostics("level=error", explain([
      { message: "bad token at position 2" },
    ]));
    expect(d).toHaveLength(1);
    expect(d[0]).toMatchObject({ from: 2, to: 3, severity: "error" });
  });

  it("clamps a position at end of document into range", () => {
    const q = "level";
    const d = extractDiagnostics(q, explain([
      { message: "unexpected end at position 5" },
    ]));
    expect(d[0]!.from).toBe(q.length - 1);
    expect(d[0]!.to).toBe(q.length);
    expect(d[0]!.to).toBeLessThanOrEqual(q.length);
  });

  it("clamps a position past the end of document", () => {
    const q = "abc";
    const d = extractDiagnostics(q, explain([
      { message: "error at position 99" },
    ]));
    expect(d[0]!.from).toBeGreaterThanOrEqual(0);
    expect(d[0]!.to).toBeLessThanOrEqual(q.length);
    expect(d[0]!.from).toBeLessThanOrEqual(d[0]!.to);
  });

  it("never produces an out-of-range marker on an empty query", () => {
    const d = extractDiagnostics("", explain([{ message: "at position 0" }]));
    expect(d[0]!.from).toBe(0);
    expect(d[0]!.to).toBe(0);
  });

  it("prefers the suggestion text when present", () => {
    const d = extractDiagnostics("x", explain([
      { message: "raw", suggestion: "use Y" },
    ]));
    expect(d[0]!.message).toBe("use Y");
  });
});
