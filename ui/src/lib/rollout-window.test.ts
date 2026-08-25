import { describe, expect, it } from "vitest";
import { rolloutWindow } from "./rollout-window";

describe("rolloutWindow", () => {
  it("renders every item in a short rollout", () => {
    expect(rolloutWindow(4, 2)).toEqual([0, 1, 2, 3]);
  });

  it("keeps long rollout windows bounded at the beginning", () => {
    const window = rolloutWindow(10_000, 0);
    expect(window).toHaveLength(201);
    expect(window[0]).toBe(0);
    expect(window.at(-1)).toBe(200);
  });

  it("centers a long rollout window on the current frame", () => {
    const window = rolloutWindow(10_000, 5_000);
    expect(window).toHaveLength(201);
    expect(window[0]).toBe(4_900);
    expect(window.at(-1)).toBe(5_100);
  });

  it("keeps long rollout windows bounded at the end", () => {
    const window = rolloutWindow(10_000, 9_999);
    expect(window).toHaveLength(201);
    expect(window[0]).toBe(9_799);
    expect(window.at(-1)).toBe(9_999);
  });

  it("rejects values outside the server contract", () => {
    expect(() => rolloutWindow(3, 3)).toThrow(RangeError);
    expect(() => rolloutWindow(-1, 0)).toThrow(RangeError);
    expect(rolloutWindow(0, 0)).toEqual([]);
  });
});
