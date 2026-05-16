import { describe, it, expect } from "vitest";
import {
  formatCount,
  formatBytes,
  formatUptime,
  formatMs,
  formatElapsed,
} from "./format";

describe("formatCount", () => {
  it("returns plain numbers below 1000", () => {
    expect(formatCount(0)).toBe("0");
    expect(formatCount(999)).toBe("999");
  });

  it("uses K for thousands without trailing .0", () => {
    expect(formatCount(1000)).toBe("1K");
    expect(formatCount(1500)).toBe("1.5K");
    expect(formatCount(12_300)).toBe("12.3K");
  });

  it("uses M for millions", () => {
    expect(formatCount(1_000_000)).toBe("1M");
    expect(formatCount(2_500_000)).toBe("2.5M");
  });
});

describe("formatBytes", () => {
  it("handles zero", () => {
    expect(formatBytes(0)).toBe("0 B");
  });

  it("keeps raw byte count at tier 0", () => {
    expect(formatBytes(500)).toBe("500 B");
  });

  it("scales to KB/MB with one decimal", () => {
    expect(formatBytes(1024)).toBe("1.0 KB");
    expect(formatBytes(1536)).toBe("1.5 KB");
    expect(formatBytes(1_048_576)).toBe("1.0 MB");
  });
});

describe("formatUptime", () => {
  it("clamps negatives to 0s", () => {
    expect(formatUptime(-5)).toBe("0s");
  });

  it("formats seconds and minutes", () => {
    expect(formatUptime(45)).toBe("45s");
    expect(formatUptime(90)).toBe("1m 30s");
  });

  it("formats hours and days", () => {
    expect(formatUptime(3661)).toBe("1h 1m");
    expect(formatUptime(90000)).toBe("1d 1h");
  });
});

describe("formatMs", () => {
  it("uses ms below 1000", () => {
    expect(formatMs(500)).toBe("500.0ms");
  });

  it("uses seconds at or above 1000", () => {
    expect(formatMs(1000)).toBe("1.0s");
    expect(formatMs(1500)).toBe("1.5s");
  });
});

describe("formatElapsed", () => {
  it("always renders seconds with one decimal", () => {
    expect(formatElapsed(1234)).toBe("1.2s");
    expect(formatElapsed(300)).toBe("0.3s");
  });
});
