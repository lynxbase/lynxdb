import { describe, it, expect } from "vitest";
import { rowKey } from "./rowKey";

describe("rowKey", () => {
  it("prefers an explicit id", () => {
    expect(rowKey({ _id: "abc", _time: "t" })).toBe("id:abc");
    expect(rowKey({ id: 42 })).toBe("id:42");
  });

  it("is stable for identical content", () => {
    const a = rowKey({ _time: "2026-01-01", _raw: "hello" });
    const b = rowKey({ _time: "2026-01-01", _raw: "hello" });
    expect(a).toBe(b);
    expect(a.startsWith("c:")).toBe(true);
  });

  it("differs when content differs", () => {
    expect(rowKey({ _time: "t1", _raw: "x" })).not.toBe(
      rowKey({ _time: "t2", _raw: "x" }),
    );
  });

  it("does not depend on row position", () => {
    const row = { _time: "t", _raw: "same" };
    expect(rowKey(row)).toBe(rowKey({ ...row }));
  });

  it("falls back to a full-content hash with no id/time/body", () => {
    const k = rowKey({ status: 500, path: "/a" });
    expect(k.startsWith("h:")).toBe(true);
  });
});
