import { describe, it, expect } from "vitest";
import {
  buildChartData,
  chartEmptyReason,
  highlightSetSeriesCalls,
  resolveCssColor,
  formatStep,
  formatMetricValue,
  type ChartSeries,
} from "./chart-utils";
import type { EvalSeriesPoint } from "./types";

function pt(step: number | null, value: number | null): EvalSeriesPoint {
  return {
    step,
    value: value as number, // tests deliberately pass null sometimes
    eval_run_id: null,
    state: "succeeded",
    checkpoint_artifact_id: "artifact_x",
  };
}

function series(id: string, points: EvalSeriesPoint[]): ChartSeries {
  return { id, points };
}

describe("buildChartData", () => {
  it("returns empty arrays when given no series", () => {
    const out = buildChartData([]);
    expect(out.xs).toEqual([]);
    expect(out.ys).toEqual([]);
    expect(out.data).toEqual([[]]);
  });

  it("returns empty xs when every series has no plottable points", () => {
    const out = buildChartData([series("a", [pt(null, 1), pt(2, null as unknown as number)])]);
    expect(out.xs).toEqual([]);
    expect(out.data[0]).toEqual([]);
  });

  it("collects xs from a single series and aligns ys", () => {
    const out = buildChartData([
      series("a", [pt(10, 0.1), pt(20, 0.2), pt(30, 0.3)]),
    ]);
    expect(out.xs).toEqual([10, 20, 30]);
    expect(out.ys).toEqual([[0.1, 0.2, 0.3]]);
    expect(out.data).toEqual([
      [10, 20, 30],
      [0.1, 0.2, 0.3],
    ]);
  });

  it("unions xs across overlapping series, padding missing values with null", () => {
    const out = buildChartData([
      series("a", [pt(10, 0.1), pt(20, 0.2)]),
      series("b", [pt(20, 0.5), pt(30, 0.6)]),
    ]);
    expect(out.xs).toEqual([10, 20, 30]);
    expect(out.ys).toEqual([
      [0.1, 0.2, null],
      [null, 0.5, 0.6],
    ]);
  });

  it("sorts xs ascending regardless of input order", () => {
    const out = buildChartData([
      series("a", [pt(30, 0.3), pt(10, 0.1), pt(20, 0.2)]),
    ]);
    expect(out.xs).toEqual([10, 20, 30]);
    expect(out.ys[0]).toEqual([0.1, 0.2, 0.3]);
  });

  it("drops points that lack step or value", () => {
    const out = buildChartData([
      series("a", [
        pt(10, 0.1),
        pt(null, 0.5),
        pt(20, null as unknown as number),
        pt(30, 0.3),
      ]),
    ]);
    expect(out.xs).toEqual([10, 30]);
    expect(out.ys[0]).toEqual([0.1, 0.3]);
  });

  it("preserves series order in output", () => {
    const out = buildChartData([
      series("a", [pt(10, 1)]),
      series("b", [pt(10, 2)]),
      series("c", [pt(10, 3)]),
    ]);
    expect(out.ys).toEqual([[1], [2], [3]]);
  });
});

describe("chartEmptyReason", () => {
  it("returns no-series for an empty list", () => {
    expect(chartEmptyReason([])).toEqual({ kind: "no-series" });
  });

  it("returns no-points when all series are empty", () => {
    expect(chartEmptyReason([series("a", [])])).toEqual({ kind: "no-points" });
  });

  it("returns no-steps when points exist but all lack step or value", () => {
    expect(
      chartEmptyReason([
        series("a", [pt(null, 0.1), pt(20, null as unknown as number)]),
      ]),
    ).toEqual({ kind: "no-steps" });
  });

  it("returns ok when at least one point has both step and value", () => {
    expect(chartEmptyReason([series("a", [pt(10, 0.1)])])).toEqual({ kind: "ok" });
  });

  it("returns ok if any series has plottable points, even if others don't", () => {
    expect(
      chartEmptyReason([
        series("a", [pt(null, 0.1)]),
        series("b", [pt(10, 0.2)]),
      ]),
    ).toEqual({ kind: "ok" });
  });
});

describe("resolveCssColor", () => {
  function fakeProps(map: Record<string, string>) {
    return (name: string): string => map[name] ?? "";
  }

  it("returns the fallback when input is undefined", () => {
    expect(resolveCssColor(undefined, "#ff0000", fakeProps({}))).toBe("#ff0000");
  });

  it("passes through a non-var color", () => {
    expect(resolveCssColor("#abcdef", "#000", fakeProps({}))).toBe("#abcdef");
  });

  it("resolves a var() reference via the props lookup", () => {
    expect(
      resolveCssColor(
        "var(--accent)",
        "#000",
        fakeProps({ "--accent": "#bdf26d" }),
      ),
    ).toBe("#bdf26d");
  });

  it("trims whitespace from resolved values", () => {
    expect(
      resolveCssColor(
        "var(--x)",
        "#000",
        fakeProps({ "--x": "  #abc123  " }),
      ),
    ).toBe("#abc123");
  });

  it("falls back when the var resolves to an empty string", () => {
    expect(
      resolveCssColor(
        "var(--missing)",
        "#fallback",
        fakeProps({ "--missing": "" }),
      ),
    ).toBe("#fallback");
  });

  it("falls back when the input is malformed var()", () => {
    expect(resolveCssColor("var(broken", "#000", fakeProps({}))).toBe("#000");
  });
});

describe("formatStep", () => {
  it("formats round numbers under 1k", () => {
    expect(formatStep(0)).toBe("0");
    expect(formatStep(500)).toBe("500");
    expect(formatStep(999)).toBe("999");
  });

  it("formats thousands with k", () => {
    expect(formatStep(1000)).toBe("1k");
    expect(formatStep(1500)).toBe("1.5k");
    expect(formatStep(50000)).toBe("50k");
  });

  it("formats millions with M", () => {
    expect(formatStep(1_000_000)).toBe("1M");
    expect(formatStep(2_500_000)).toBe("2.5M");
  });
});

describe("highlightSetSeriesCalls", () => {
  const a = series("run_a", [pt(10, 0.1)]);
  const b = series("run_b", [pt(10, 0.2)]);
  const c = series("run_c", [pt(10, 0.3)]);

  it("emits one width call per series + a clear-focus call when nothing is highlighted", () => {
    const calls = highlightSetSeriesCalls([a, b, c], null);
    // 3 width calls + 1 focus-clear call
    expect(calls).toHaveLength(4);
    expect(calls.slice(0, 3).every((c) => c.opts.width === 1.6)).toBe(true);
    // The focus-clear call MUST have `opts = {focus: true}`, not `null` —
    // uPlot's setSeries does `opts.focus != null` unconditionally and
    // throws if opts is null.
    expect(calls[3]).toEqual({ idx: null, opts: { focus: true } });
  });

  it("widens the highlighted series and focuses it", () => {
    const calls = highlightSetSeriesCalls([a, b, c], "run_b");
    expect(calls).toHaveLength(4);
    expect(calls[0]).toEqual({ idx: 1, opts: { width: 1.6 } });
    expect(calls[1]).toEqual({ idx: 2, opts: { width: 2.4 } });
    expect(calls[2]).toEqual({ idx: 3, opts: { width: 1.6 } });
    expect(calls[3]).toEqual({ idx: 2, opts: { focus: true } });
  });

  it("uses 1-based uPlot indices (idx 0 is reserved for the x-axis)", () => {
    const calls = highlightSetSeriesCalls([a, b, c], "run_a");
    // First series is uPlot idx 1, not 0.
    expect(calls[0]?.idx).toBe(1);
    // Focus call also uses 1-based.
    expect(calls.find((c) => c.opts.focus === true)?.idx).toBe(1);
  });

  it("falls back to a clear-focus call if highlightedId doesn't match any series", () => {
    const calls = highlightSetSeriesCalls([a, b], "ghost_run");
    // 2 width calls (both at normal width) + no focus call (since no
    // match was found). This is the silent no-op case — the chart stays
    // in its current focus state rather than throwing.
    expect(calls).toHaveLength(2);
    expect(calls.every((c) => c.opts.width === 1.6)).toBe(true);
  });

  it("never returns calls with opts=null (regression for the uPlot null-throw bug)", () => {
    for (const hi of [null, "run_a", "run_b", "ghost"]) {
      const calls = highlightSetSeriesCalls([a, b], hi);
      for (const c of calls) {
        expect(c.opts).not.toBeNull();
        expect(typeof c.opts).toBe("object");
      }
    }
  });
});

describe("formatMetricValue", () => {
  it("uses 4 decimals under 1.0", () => {
    expect(formatMetricValue(0.6543)).toBe("0.6543");
    expect(formatMetricValue(0.1)).toBe("0.1000");
  });

  it("uses 3 decimals between 1 and 100", () => {
    expect(formatMetricValue(1.5)).toBe("1.500");
    expect(formatMetricValue(42.123456)).toBe("42.123");
  });

  it("uses 1 decimal at 100+", () => {
    expect(formatMetricValue(100)).toBe("100.0");
    expect(formatMetricValue(1234.567)).toBe("1234.6");
  });
});
