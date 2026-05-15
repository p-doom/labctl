<script lang="ts">
  // uPlot-backed chart. Pure data shaping + theming logic lives in
  // lib/chart-utils.ts and is unit-tested; this component only owns the
  // DOM lifecycle (mount, resize, theme flip, destroy).
  //
  // Why uPlot vs Observable Plot: Plot is gorgeous out of the box but
  // primarily a static rendering library — toggle / brush / clean click
  // handlers are bolt-on. uPlot has those interactions as first-class
  // concerns and is 2.5× smaller in the bundle. We own the design
  // system, so styling cost is one-time.
  //
  // Why bidirectional highlight is safe: uPlot fires the `setSeries`
  // hook for every setSeries call — both cursor-driven and programmatic.
  // The recursion-free trick is `_fire=false` (third arg) on every
  // programmatic call we make, which suppresses the hook for that call.
  // Cursor focus still fires the hook (we want that — chart→legend),
  // and our highlightedId effect responds to legend hovers without
  // echoing back into the hook (legend→chart, no loop).

  import { onMount, onDestroy, tick } from "svelte";
  import uPlot, { type Options as UPlotOptions } from "uplot";
  import "uplot/dist/uPlot.min.css";
  import type { EvalSeriesPoint } from "../lib/types";
  import { theme } from "../lib/theme.svelte";
  import {
    buildChartData,
    chartEmptyReason,
    highlightSetSeriesCalls,
    resolveCssColor,
    formatStep,
    formatMetricValue,
    type ChartSeries,
  } from "../lib/chart-utils";

  export type { ChartSeries };

  interface Props {
    series: ChartSeries[];
    height?: number;
    onPointClick?: (seriesId: string, p: EvalSeriesPoint) => void;
    visible?: Record<string, boolean>;
    highlightedId?: string | null;
    onSeriesEnter?: (id: string | null) => void;
  }
  let {
    series,
    height = 280,
    onPointClick,
    visible,
    highlightedId = null,
    onSeriesEnter,
  }: Props = $props();

  let container: HTMLDivElement | null = $state(null);
  let chart: uPlot | null = null;
  let resizeObserver: ResizeObserver | null = null;
  let renderError = $state<string | null>(null);

  // Reading raw CSS vars at chart-build time so canvas strokes get
  // concrete color strings. Re-reads when the theme flips so the chart's
  // axis/grid colors match the active palette.
  function readTokens() {
    const cs = getComputedStyle(document.documentElement);
    const get = (name: string) => cs.getPropertyValue(name).trim();
    return {
      fg0: get("--fg-0"),
      fg2: get("--fg-2"),
      fg3: get("--fg-3"),
      line0: get("--line-0"),
      line1: get("--line-1"),
      bg0: get("--bg-0"),
      bg1: get("--bg-1"),
      bg2: get("--bg-2"),
      accent: get("--accent"),
    };
  }

  function getProp(name: string): string {
    return getComputedStyle(document.documentElement).getPropertyValue(name);
  }

  function buildOpts(width: number, h: number): UPlotOptions {
    const t = readTokens();
    const accent = t.accent;
    return {
      width,
      height: h,
      legend: { show: false },
      cursor: {
        drag: { x: false, y: false },
        // prox: pixel radius within which a series is "focused" by the
        // cursor. Triggers the setSeries hook so the chart → legend
        // direction works without our own mousemove listener.
        focus: { prox: 30 },
        points: { size: 8, stroke: t.bg0 },
      },
      // Non-focused series fade to this alpha when any series is
      // focused. Gives the chart a glance-anywhere "which line is which"
      // affordance without us drawing anything ourselves.
      focus: { alpha: 0.25 },
      scales: {
        x: { time: false },
        y: {
          range: (_u, min, max) => {
            const pad = (max - min) * 0.12 || 0.01;
            return [Math.max(0, min - pad), max + pad];
          },
        },
      },
      axes: [
        {
          stroke: t.fg2,
          grid: { stroke: t.line0, width: 1 },
          ticks: { stroke: t.line0, size: 4 },
          font: '11px "JetBrains Mono Variable", "JetBrains Mono", monospace',
          values: (_u, vals) => vals.map((v) => formatStep(v)),
          size: 32,
        },
        {
          stroke: t.fg2,
          grid: { stroke: t.line0, width: 1 },
          ticks: { stroke: t.line0, size: 4 },
          font: '11px "JetBrains Mono Variable", "JetBrains Mono", monospace',
          values: (_u, vals) => vals.map((v) => formatMetricValue(v)),
          size: 56,
        },
      ],
      series: [
        {},
        ...series.map((s) => {
          const color = resolveCssColor(s.color, accent, getProp);
          const isHighlighted = highlightedId === s.id;
          return {
            label: s.id,
            stroke: color,
            width: isHighlighted ? 2.4 : 1.6,
            show: visible ? visible[s.id] !== false : true,
            points: {
              show: true,
              size: isHighlighted ? 7 : 5,
              stroke: color,
              fill: t.bg1,
              width: 1.5,
            },
          };
        }),
      ],
      hooks: {
        // Bubble cursor-driven focus changes to the parent so it can
        // highlight the matching legend row. This is the only place
        // where the hook fires — programmatic setSeries calls below
        // pass `_fire=false`, so they never echo back here.
        setSeries: [
          (_u, idx) => {
            if (idx == null || idx === 0) {
              onSeriesEnter?.(null);
            } else {
              onSeriesEnter?.(series[idx - 1]?.id ?? null);
            }
          },
        ],
      },
      plugins: [
        {
          hooks: {
            ready: [
              (u: uPlot) => {
                const over = u.over;
                over.addEventListener("click", () => {
                  const idx = u.cursor.idxs?.[1];
                  if (idx == null || idx < 0) return;
                  const cy = u.cursor.top;
                  if (cy == null || cy < 0) return;
                  let bestI = -1;
                  let bestDist = Infinity;
                  for (let i = 1; i < u.series.length; i++) {
                    const ser = u.series[i]!;
                    if (ser.show === false) continue;
                    const v = u.data[i]?.[idx];
                    if (v == null) continue;
                    const py = u.valToPos(v as number, "y", false);
                    const d = Math.abs(py - cy);
                    if (d < bestDist) {
                      bestDist = d;
                      bestI = i;
                    }
                  }
                  if (bestI < 0) return;
                  const sourceSeries = series[bestI - 1];
                  if (!sourceSeries) return;
                  const x = u.data[0]?.[idx] as number | undefined;
                  if (x == null) return;
                  const point = sourceSeries.points.find((p) => p.step === x);
                  if (point) onPointClick?.(sourceSeries.id, point);
                });
              },
            ],
          },
        },
      ],
    };
  }

  function rebuild() {
    if (!container) return;
    chart?.destroy();
    chart = null;
    renderError = null;
    const reason = chartEmptyReason(series);
    if (reason.kind !== "ok") return;
    const w = container.clientWidth;
    if (w === 0) {
      // Layout hasn't settled yet — the ResizeObserver will pick this
      // up as soon as the container has a real width and call us again.
      return;
    }
    try {
      const { data } = buildChartData(series);
      const opts = buildOpts(w, height);
      chart = new uPlot(opts, data, container);
    } catch (err) {
      // uPlot construction failure used to be silent — surface it so
      // we can see what's wrong instead of staring at a blank rectangle.
      renderError = err instanceof Error ? err.message : String(err);
      console.error("MetricChart: uPlot construction failed:", err);
      chart = null;
    }
  }

  onMount(async () => {
    // Wait for Svelte's tick + a microtask so the DOM is fully laid out
    // before we ask the browser for `clientWidth`. This is the load-bearing
    // fix: previously, `rebuild()` ran on the same frame as mount and saw
    // a 0-width container, so uPlot was created at 0 pixels (silently
    // invisible). We wait for layout, then build.
    await tick();
    requestAnimationFrame(() => {
      rebuild();
    });

    if (container) {
      resizeObserver = new ResizeObserver(() => {
        if (!container) return;
        const w = container.clientWidth;
        if (w === 0) return;
        if (chart) {
          chart.setSize({ width: w, height });
        } else {
          rebuild();
        }
      });
      resizeObserver.observe(container);
    }
  });

  onDestroy(() => {
    chart?.destroy();
    chart = null;
    resizeObserver?.disconnect();
    resizeObserver = null;
  });

  // Rebuild on series-shape change or theme flip. Defer to rAF so the
  // browser has a chance to settle layout if the parent grew/shrank.
  $effect(() => {
    void series;
    void theme.effective;
    if (!container) return;
    requestAnimationFrame(() => {
      if (container) rebuild();
    });
  });

  // Visibility toggling without a full rebuild — uPlot updates in place.
  // `false` as the third arg suppresses the setSeries hook so even if a
  // hook were re-introduced our updates wouldn't echo back.
  $effect(() => {
    if (!chart || !visible) return;
    series.forEach((s, i) => {
      const wantShow = visible[s.id] !== false;
      const cur = chart!.series[i + 1]?.show;
      if (cur !== wantShow) {
        chart!.setSeries(i + 1, { show: wantShow }, false);
      }
    });
  });

  // Drive both width emphasis and uPlot's focus-alpha dimming in
  // response to legend hovers. `_fire=false` is the load-bearing flag —
  // without it, each programmatic setSeries would re-fire our hook,
  // recurse via onSeriesEnter, and lock up the page. setSeries already
  // commits internally on focus changes, so no explicit redraw needed.
  $effect(() => {
    if (!chart) return;
    for (const call of highlightSetSeriesCalls(series, highlightedId ?? null)) {
      chart.setSeries(call.idx, call.opts, false);
    }
  });

  let emptyReason = $derived(chartEmptyReason(series));
  let isEmpty = $derived(emptyReason.kind !== "ok");

  // Diagnostic copy. Each branch tells the caller exactly why the chart
  // isn't rendering, instead of leaving an unexplained blank rectangle.
  function emptyMessage(reason: typeof emptyReason): { title: string; sub: string } {
    switch (reason.kind) {
      case "no-series":
        return {
          title: "No runs to plot",
          sub: "This view received an empty series list.",
        };
      case "no-points":
        return {
          title: "No measured checkpoints yet",
          sub: "These runs haven't produced eval results for this metric.",
        };
      case "no-steps":
        return {
          title: "Eval values exist but no checkpoint steps",
          sub: "labctl couldn't read `metadata.step` from any checkpoint artifact. The chart needs (step, value) pairs.",
        };
      case "ok":
        return { title: "", sub: "" };
    }
  }
</script>

<div class="wrap" style="height: {height}px;">
  {#if isEmpty}
    {@const msg = emptyMessage(emptyReason)}
    <div class="empty">
      <div class="title">{msg.title}</div>
      <div class="sub">{msg.sub}</div>
    </div>
  {:else if renderError}
    <div class="error">
      <div class="title">Chart failed to render</div>
      <pre class="sub">{renderError}</pre>
    </div>
  {/if}
  <div bind:this={container} class="canvas" class:hidden={isEmpty || renderError}></div>
</div>

<style>
  .wrap {
    position: relative;
    width: 100%;
  }
  .canvas {
    width: 100%;
    height: 100%;
  }
  .canvas.hidden {
    display: none;
  }
  .empty,
  .error {
    height: 100%;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 4px;
    text-align: center;
    background: var(--bg-0);
    border: 1px solid var(--line-0);
    border-radius: 4px;
    padding: 16px;
  }
  .empty .title,
  .error .title {
    font-size: 13px;
    color: var(--fg-0);
  }
  .empty .sub,
  .error .sub {
    font-size: 11px;
    color: var(--fg-2);
    max-width: 480px;
    margin: 0;
    white-space: pre-wrap;
    font-family: theme("fontFamily.mono");
  }
  .error {
    border-color: var(--status-failed);
  }
  .error .title { color: var(--status-failed-fg); }

  /* uPlot palette overrides — the library ships a default CSS that
   * doesn't match our tokens. We restyle just the bits that touch the
   * design surface. */
  :global(.uplot) {
    font-family: "JetBrains Mono Variable", "JetBrains Mono", monospace;
    font-size: 11px;
    color: var(--fg-1);
  }
  :global(.uplot .u-cursor-x),
  :global(.uplot .u-cursor-y) {
    background: var(--line-2) !important;
  }
  :global(.uplot .u-tooltip),
  :global(.uplot .u-legend) {
    background: var(--bg-2) !important;
    color: var(--fg-0) !important;
    border: 1px solid var(--line-1) !important;
  }
  :global(.uplot .u-axis) {
    color: var(--fg-2);
  }
  :global(.uplot .u-over) {
    cursor: crosshair;
  }
</style>
