<script lang="ts">
  // Container for the chart + legend stack used by PolicyDetail, Compare
  // and Recipe. Owns the bordered surface, the metric header row, and
  // exposes a slot for the chart and a slot for the legend rows.
  import type { Snippet } from "svelte";

  interface Props {
    metric: string | null;
    /** Right-aligned subtitle next to the metric name, e.g. "5 of 12 runs". */
    subtitle?: string;
    chart: Snippet;
    /** Optional legend section under the chart. When omitted, no divider. */
    legend?: Snippet;
    /** Optional legend column headers (rendered above the legend rows). */
    legendHead?: Snippet;
  }
  let { metric, subtitle, chart, legend, legendHead }: Props = $props();
</script>

<div class="chart-card">
  <header class="chart-h">
    <span class="metric mono">{metric}</span>
    {#if subtitle}
      <span class="count">{subtitle}</span>
    {/if}
  </header>
  <div class="chart-wrap">{@render chart()}</div>
  {#if legend}
    <div class="legend">
      {#if legendHead}
        <div class="leg-head mono">{@render legendHead()}</div>
      {/if}
      {@render legend()}
    </div>
  {/if}
</div>

<style>
  .chart-card {
    background: var(--bg-1);
    border: 1px solid var(--line-0);
    border-radius: 6px;
    padding: 14px 16px 12px 16px;
  }
  .chart-h {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 8px;
  }
  .chart-h .metric { font-size: 13px; color: var(--fg-0); }
  .chart-h .count {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: var(--fg-2);
  }
  .chart-wrap { padding: 0; }
  .legend {
    display: flex;
    flex-direction: column;
    margin-top: 12px;
    padding-top: 8px;
    border-top: 1px solid var(--line-0);
  }
  .leg-head {
    display: grid;
    grid-template-columns: var(--legend-cols, 22px minmax(0, 1.5fr) 96px 72px 56px 56px);
    column-gap: 12px;
    align-items: center;
    padding: 4px 4px 6px 4px;
    font-size: 11px;
    font-weight: 500;
    color: var(--fg-1);
  }
  .leg-head :global(.r) { text-align: right; }
</style>
