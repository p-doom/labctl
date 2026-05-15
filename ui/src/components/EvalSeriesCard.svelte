<script lang="ts">
  // Per-policy card. Shows headline metric + delta vs previous + chart.
  // Click a chart point or expand the row list to navigate to the
  // underlying eval run.

  import type { EvalSeries, EvalSeriesPoint } from "../lib/types";
  import { router } from "../lib/router.svelte";
  import { statusGroup, shortStatus } from "../lib/format";
  import MetricChart from "./MetricChart.svelte";
  import Pill from "./Pill.svelte";

  interface Props {
    series: EvalSeries;
  }
  let { series }: Props = $props();
  let expanded = $state(false);

  let delta = $derived.by(() => {
    if (series.latest_value == null || series.previous_value == null) return null;
    return series.latest_value - series.previous_value;
  });

  // Status of the *most recent* point — drives the small status pill in
  // the header. "running" / "submitted" → user knows there's an eval in
  // flight; "succeeded" → all done.
  let lastPoint = $derived(series.points[series.points.length - 1] ?? null);

  function fmtValue(v: number): string {
    if (Math.abs(v) >= 100) return v.toFixed(1);
    if (Math.abs(v) >= 1) return v.toFixed(3);
    return v.toFixed(4);
  }
  function fmtDelta(d: number): string {
    const sign = d >= 0 ? "+" : "−";
    const abs = Math.abs(d);
    if (abs >= 1) return `${sign}${abs.toFixed(2)}`;
    if (abs >= 0.001) return `${sign}${abs.toFixed(3)}`;
    return `${sign}${abs.toFixed(4)}`;
  }
  function fmtStep(s: number | null): string {
    if (s == null) return "—";
    if (s >= 1000) return `${(s / 1000).toFixed(s % 1000 === 0 ? 0 : 1)}k`;
    return String(s);
  }

  function onPointClick(_seriesId: string, p: EvalSeriesPoint) {
    if (p.eval_run_id) router.go("runs", p.eval_run_id);
  }
  let chartSeries = $derived([
    { id: series.policy_id, points: series.points },
  ]);
</script>

<section class="card">
  <header>
    <div class="title">
      <button
        type="button"
        class="policy mono"
        title={`Open policy "${series.policy_id}" — compare across runs`}
        onclick={() => router.go("policies", series.policy_id)}
      >{series.policy_id}</button>
      {#if series.metric_name}
        <span class="metric mono">{series.metric_name}</span>
      {/if}
      {#if lastPoint}
        <Pill status={lastPoint.state} />
      {/if}
    </div>
    {#if series.latest_value != null}
      <div class="headline">
        <span class="value mono">{fmtValue(series.latest_value)}</span>
        {#if delta != null}
          <span class="delta mono" data-sign={delta >= 0 ? "pos" : "neg"}>
            {fmtDelta(delta)}
          </span>
        {/if}
        <span class="at mono">step {fmtStep(series.latest_step)}</span>
      </div>
    {/if}
  </header>

  <div class="chart-wrap">
    <MetricChart series={chartSeries} {onPointClick} />
  </div>

  <button
    type="button"
    class="expand"
    onclick={() => (expanded = !expanded)}
    aria-expanded={expanded}
  >
    {series.count} {series.count === 1 ? "checkpoint" : "checkpoints"} ·
    <span class="expand-cta">{expanded ? "hide" : "show"}</span>
  </button>

  {#if expanded}
    <div class="rows">
      {#each series.points as p (`${p.step}-${p.checkpoint_artifact_id}`)}
        {#if p.eval_run_id}
          <button
            type="button"
            class="row clickable"
            onclick={() => router.go("runs", p.eval_run_id!)}
          >
            <Pill status={p.state} showLabel={false} />
            <span class="step mono">step {fmtStep(p.step)}</span>
            <span class="row-value mono">
              {p.value != null ? fmtValue(p.value) : "—"}
            </span>
            <span class="row-state mono">{shortStatus(p.state)}</span>
          </button>
        {:else}
          <div class="row">
            <Pill status={p.state} showLabel={false} />
            <span class="step mono">step {fmtStep(p.step)}</span>
            <span class="row-value mono">—</span>
            <span class="row-state mono">{shortStatus(p.state)}</span>
          </div>
        {/if}
      {/each}
    </div>
  {/if}
</section>

<style>
  .card {
    background: theme("colors.bg.2");
    border: 1px solid theme("colors.line.0");
    border-radius: 6px;
    padding: 10px 12px 6px 12px;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  header {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    align-items: baseline;
    gap: 12px;
  }
  .title {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    overflow: hidden;
    min-width: 0;
  }
  .policy {
    font-size: 12px;
    color: theme("colors.fg.0");
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
    background: transparent;
    border: none;
    padding: 0;
    cursor: pointer;
    text-align: left;
  }
  .policy:hover { color: theme("colors.accent.DEFAULT"); }
  .metric {
    font-size: 11px;
    color: theme("colors.fg.2");
    flex-shrink: 0;
  }
  .headline {
    display: inline-flex;
    align-items: baseline;
    gap: 8px;
    flex-shrink: 0;
  }
  .value {
    font-size: 14px;
    color: theme("colors.accent.DEFAULT");
    font-variant-numeric: tabular-nums;
    font-weight: 500;
  }
  .delta {
    font-size: 11px;
    font-variant-numeric: tabular-nums;
  }
  .delta[data-sign="pos"] { color: theme("colors.status.succeeded.fg"); }
  .delta[data-sign="neg"] { color: theme("colors.status.failed.fg"); }
  .at {
    font-size: 10px;
    color: theme("colors.fg.3");
  }

  .chart-wrap {
    padding: 0 4px;
  }

  .expand {
    background: transparent;
    border: none;
    padding: 4px 0 2px 0;
    color: theme("colors.fg.2");
    font-size: 11px;
    font-family: theme("fontFamily.mono");
    cursor: pointer;
    text-align: left;
  }
  .expand:hover { color: theme("colors.fg.0"); }
  .expand-cta { color: theme("colors.fg.1"); }

  .rows {
    display: flex;
    flex-direction: column;
    gap: 1px;
    padding-top: 4px;
    border-top: 1px solid theme("colors.line.0");
  }
  .row {
    display: grid;
    grid-template-columns: 22px 1fr auto auto;
    column-gap: 12px;
    align-items: center;
    padding: 5px 4px;
    background: transparent;
    border: none;
    color: inherit;
    font: inherit;
    text-align: left;
    width: 100%;
  }
  .row.clickable { cursor: pointer; }
  .row.clickable:hover { background: theme("colors.bg.3"); }
  .step { font-size: 11px; color: theme("colors.fg.0"); }
  .row-value {
    font-size: 12px;
    color: theme("colors.fg.0");
    font-variant-numeric: tabular-nums;
  }
  .row-state {
    font-size: 10px;
    color: theme("colors.fg.2");
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }
</style>
