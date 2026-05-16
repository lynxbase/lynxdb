import { describe, it, expect } from "vitest";
import {
  parseRelativeExpression,
  parseNowExpression,
  toNowExpr,
  getTimeRangeLabel,
} from "./timeFormat";

describe("parseRelativeExpression", () => {
  it("parses an ago-to-ago range", () => {
    expect(parseRelativeExpression("2h ago to 30m ago")).toEqual({
      from: "-2h",
      to: "-30m",
    });
  });

  it("parses an ago-to-now range", () => {
    expect(parseRelativeExpression("1d ago to now")).toEqual({
      from: "-1d",
      to: "now",
    });
  });

  it("returns null for unrecognized input", () => {
    expect(parseRelativeExpression("yesterday")).toBeNull();
    expect(parseRelativeExpression("2h ago")).toBeNull();
  });
});

describe("parseNowExpression", () => {
  it("maps bare now to undefined", () => {
    expect(parseNowExpression("now")).toBeUndefined();
  });

  it("parses now-Nx with optional spaces", () => {
    expect(parseNowExpression("now-3h")).toBe("-3h");
    expect(parseNowExpression("now - 30m")).toBe("-30m");
  });

  it("accepts a bare relative value", () => {
    expect(parseNowExpression("-3h")).toBe("-3h");
  });

  it("returns null for invalid input", () => {
    expect(parseNowExpression("garbage")).toBeNull();
  });
});

describe("toNowExpr", () => {
  it("renders now for undefined or now", () => {
    expect(toNowExpr(undefined)).toBe("now");
    expect(toNowExpr("now")).toBe("now");
  });

  it("prefixes relative values with now", () => {
    expect(toNowExpr("-3h")).toBe("now-3h");
  });

  it("passes through absolute values", () => {
    expect(toNowExpr("2026-01-01T00:00:00Z")).toBe("2026-01-01T00:00:00Z");
  });
});

describe("getTimeRangeLabel", () => {
  it("returns the preset label for a known relative-from", () => {
    expect(getTimeRangeLabel("-1h", undefined)).toBe("Last 1 hour");
  });

  it("describes a relative range", () => {
    expect(getTimeRangeLabel("-2h", "-30m")).toBe("2h ago to 30m ago");
  });

  it("falls back to an absolute-style label", () => {
    expect(getTimeRangeLabel("notadate", undefined)).toBe("notadate -- now");
  });
});
