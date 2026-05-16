import { describe, it, expect, vi } from "vitest";
import { processLine } from "./streaming";
import type { StreamCallbacks } from "./streaming";

function makeCallbacks() {
  const onRow = vi.fn<StreamCallbacks["onRow"]>();
  const onMeta = vi.fn<StreamCallbacks["onMeta"]>();
  const onError = vi.fn<StreamCallbacks["onError"]>();
  const callbacks: StreamCallbacks = { onRow, onMeta, onError };
  return { callbacks, onRow, onMeta, onError };
}

describe("processLine", () => {
  it("dispatches a data row and reports it as non-control", () => {
    const cb = makeCallbacks();
    const isControl = processLine('{"a":1}', cb.callbacks);
    expect(isControl).toBe(false);
    expect(cb.onRow).toHaveBeenCalledWith({ a: 1 });
    expect(cb.onMeta).not.toHaveBeenCalled();
    expect(cb.onError).not.toHaveBeenCalled();
  });

  it("routes __meta control lines to onMeta", () => {
    const cb = makeCallbacks();
    const isControl = processLine('{"__meta":{"total":42}}', cb.callbacks);
    expect(isControl).toBe(true);
    expect(cb.onMeta).toHaveBeenCalledWith({ total: 42 });
    expect(cb.onRow).not.toHaveBeenCalled();
  });

  it("routes __error control lines to onError", () => {
    const cb = makeCallbacks();
    const isControl = processLine('{"__error":{"message":"boom"}}', cb.callbacks);
    expect(isControl).toBe(true);
    expect(cb.onError).toHaveBeenCalledWith("boom");
  });

  it("falls back to a default error message", () => {
    const cb = makeCallbacks();
    processLine('{"__error":{}}', cb.callbacks);
    expect(cb.onError).toHaveBeenCalledWith("Stream error");
  });

  it("skips malformed JSON without invoking callbacks", () => {
    const cb = makeCallbacks();
    const isControl = processLine("{not json", cb.callbacks);
    expect(isControl).toBe(true);
    expect(cb.onRow).not.toHaveBeenCalled();
    expect(cb.onMeta).not.toHaveBeenCalled();
    expect(cb.onError).not.toHaveBeenCalled();
  });
});
