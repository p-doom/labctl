// Pure functions extracted from MetricChart so they can be unit-tested
// without spinning up jsdom + Svelte runes. Anything in here must NOT
// depend on the DOM, getComputedStyle, or component state.

import type { EvalSeriesPoint } from "./types";

export interface ChartSeries {
  id: string;
  color?: string;
  points: EvalSeriesPoint[];
}

export type ChartEmptyReason =
  | { kind: "ok" }
  | { kind: "no-series" }
  | { kind: "no-points" }
  | { kind: "no-steps" };

/**
 * Walk all series and decide why (if at all) we shouldn't render a chart.
 * Distinct reasons let the UI surface what's actually wrong instead of a
 * silent blank rectangle.
 *
 *   no-series : caller passed []
 *   no-points : every series has 0 points
 *   no-steps  : at least one point exists but none have BOTH step and
 *               value populated — i.e. metric values were extracted but
 *               we never recorded which checkpoint step they came from
 *   ok        : at least one point has both step and value
 */
export function chartEmptyReason(series: ChartSeries[]): ChartEmptyReason {
  if (series.length === 0) return { kind: "no-series" };
  let totalPoints = 0;
  let plottable = 0;
  for (const s of series) {
    totalPoints += s.points.length;
    for (const p of s.points) {
      if (p.step != null && p.value != null) plottable++;
    }
  }
  if (totalPoints === 0) return { kind: "no-points" };
  if (plottable === 0) return { kind: "no-steps" };
  return { kind: "ok" };
}

/**
 * Build the [xs, ys1, ys2, …] aligned data shape uPlot wants. Steps are
 * unioned across series so a run that's only at step=10k still shares
 * the x-axis with one that's at 10k–50k. Missing values become null,
 * which uPlot renders as a gap in the line.
 */
export function buildChartData(series: ChartSeries[]): {
  xs: number[];
  ys: (number | null)[][];
  data: (number | null)[][];
} {
  const allX = new Set<number>();
  for (const s of series) {
    for (const p of s.points) {
      if (p.step != null && p.value != null) allX.add(p.step);
    }
  }
  const xs = [...allX].sort((a, b) => a - b);
  const ys: (number | null)[][] = series.map((s) => {
    const lookup = new Map<number, number>();
    for (const p of s.points) {
      if (p.step != null && p.value != null) lookup.set(p.step, p.value);
    }
    return xs.map((x) => lookup.get(x) ?? null);
  });
  return { xs, ys, data: [xs, ...ys] };
}

/**
 * Resolve a CSS color string. If `c` is `var(--name)`, look up the
 * computed property; otherwise pass through. Falls back to `fallback`
 * if the var isn't set or is empty.
 *
 * The `getProp` indirection is so this can be called from tests with a
 * fake style lookup — the real component passes
 * `getComputedStyle(document.documentElement).getPropertyValue`.
 */
export function resolveCssColor(
  c: string | undefined,
  fallback: string,
  getProp: (name: string) => string,
): string {
  if (!c) return fallback;
  if (!c.startsWith("var(")) return c;
  const match = c.match(/var\((--[^,)]+)/);
  if (!match) return fallback;
  const value = getProp(match[1]!).trim();
  return value || fallback;
}

/** Compact step formatting: 50000 → "50k", 1.5M → "1.5M". */
export function formatStep(n: number): string {
  if (n >= 1_000_000) {
    return `${(n / 1_000_000).toFixed(n % 1_000_000 === 0 ? 0 : 1)}M`;
  }
  if (n >= 1000) {
    return `${(n / 1000).toFixed(n % 1000 === 0 ? 0 : 1)}k`;
  }
  return String(n);
}

/** Tabular metric value formatting. */
export function formatMetricValue(v: number): string {
  if (Math.abs(v) >= 100) return v.toFixed(1);
  if (Math.abs(v) >= 1) return v.toFixed(3);
  return v.toFixed(4);
}

/**
 * Compute the sequence of `setSeries` calls needed to reflect the given
 * highlight state on the chart. Pure function — pulled out so it can be
 * tested without spinning up uPlot.
 *
 * Returns calls in the form `[seriesIdx, opts]`. `seriesIdx` is uPlot's
 * 1-based series index (0 is reserved for the x-axis). The caller is
 * responsible for adding `_fire=false` and invoking `chart.setSeries`.
 *
 * Important shape note: when nothing is highlighted, we emit a
 * `setSeries(null, {focus: true})` to clear focus state. uPlot's API
 * does `opts.focus != null` unconditionally — passing `null` for opts
 * throws. The internal `FOCUS_TRUE = {focus: true}` constant in uPlot
 * itself confirms `{focus: true}` is the right shape regardless of idx.
 */
export interface SetSeriesCall {
  idx: number | null;
  opts: { width?: number; focus?: boolean };
}

export function highlightSetSeriesCalls(
  series: ChartSeries[],
  highlightedId: string | null,
  widths = { highlighted: 2.4, normal: 1.6 },
): SetSeriesCall[] {
  const calls: SetSeriesCall[] = [];
  series.forEach((s, i) => {
    const isHi = highlightedId === s.id;
    calls.push({
      idx: i + 1,
      opts: { width: isHi ? widths.highlighted : widths.normal },
    });
  });
  if (highlightedId == null) {
    calls.push({ idx: null, opts: { focus: true } });
  } else {
    const idx = series.findIndex((s) => s.id === highlightedId);
    if (idx >= 0) {
      calls.push({ idx: idx + 1, opts: { focus: true } });
    }
  }
  return calls;
}
