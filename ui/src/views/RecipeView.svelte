<script lang="ts">
  // Recipe view: every run of a recipe overlaid on a single chart per
  // metric (chip selector for switching). Same shape as Compare; only
  // the source of runs differs (recipe membership vs user selection).

  import { store, loadRecipeView } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { shortId } from "../lib/format";
  import type { MetricSeries, MetricSeriesPoint } from "../lib/types";

  import MetricChart, { type ChartSeries } from "../components/MetricChart.svelte";
  import Pill from "../components/Pill.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Hash from "../components/Hash.svelte";
  import Icon from "../components/Icon.svelte";

  interface Props {
    recipeName: string;
  }
  let { recipeName }: Props = $props();

  let recipe = $derived(store.recipeView(recipeName));
  let error = $state<string | null>(null);
  let highlighted = $state<string | null>(null);
  let selectedMetric = $state<string | null>(null);
  let visible = $state<Record<string, boolean>>({});

  $effect(() => {
    if (!recipeName) return;
    loadRecipeView(recipeName)
      .then(() => { error = null; })
      .catch((e) => { error = e instanceof Error ? e.message : String(e); });
  });

  $effect(() => {
    void recipeName;
    selectedMetric = null;
    visible = {};
  });

  let metrics = $derived(recipe?.metrics ?? []);
  let activeMetric = $derived(selectedMetric ?? metrics[0] ?? null);
  let activeSeries = $derived<MetricSeries | null>(
    recipe?.series_by_metric.find((s) => s.metric_name === activeMetric) ?? null,
  );

  const COLORS = [
    "var(--accent)",
    "#7dc3e8",
    "#f0a872",
    "#d895c4",
    "#7dd3a8",
    "#c8b9f0",
    "#e8c170",
    "#9ed4d4",
  ];
  function colorFor(idx: number): string {
    return COLORS[idx % COLORS.length]!;
  }

  let chartSeries = $derived.by<ChartSeries[]>(() => {
    if (!activeSeries) return [];
    return activeSeries.runs.map((r, i) => ({
      id: r.run_id,
      color: colorFor(i),
      points: r.points.map((p) => ({
        step: p.step,
        value: p.value,
        eval_run_id: p.eval_run_id,
        state: p.state,
        checkpoint_artifact_id: p.checkpoint_artifact_id,
        metric_name: activeMetric,
      })),
    }));
  });

  function toggleVisible(runId: string) {
    visible = { ...visible, [runId]: visible[runId] === false };
  }
  function isVisible(runId: string): boolean {
    return visible[runId] !== false;
  }

  function fmtValue(v: number | null): string {
    if (v == null) return "—";
    if (Math.abs(v) >= 100) return v.toFixed(1);
    if (Math.abs(v) >= 1) return v.toFixed(3);
    return v.toFixed(4);
  }
  function fmtStep(s: number | null): string {
    if (s == null) return "—";
    if (s >= 1000) return `${(s / 1000).toFixed(s % 1000 === 0 ? 0 : 1)}k`;
    return String(s);
  }
  function fmtDelta(d: number | null): string | null {
    if (d == null) return null;
    const sign = d >= 0 ? "+" : "−";
    const abs = Math.abs(d);
    if (abs >= 1) return `${sign}${abs.toFixed(2)}`;
    if (abs >= 0.001) return `${sign}${abs.toFixed(3)}`;
    return `${sign}${abs.toFixed(4)}`;
  }

  function close() { router.go("runs"); }
  function onPointClick(_seriesId: string, p: MetricSeriesPoint) {
    if (p.eval_run_id) router.go("runs", p.eval_run_id);
  }
  function metricRunCount(name: string): number {
    return recipe?.series_by_metric.find((s) => s.metric_name === name)?.run_count ?? 0;
  }
</script>

<div class="page">
  <header class="header">
    <button type="button" class="back" onclick={close} aria-label="Back to runs">
      <Icon name="back" size={14} />
      <span>Runs</span>
    </button>
    <div class="title">
      <span class="t-label mono">recipe</span>
      <span class="t-name mono">{recipeName}</span>
      {#if recipe}
        <span class="t-count">
          {recipe.runs.length} {recipe.runs.length === 1 ? "run" : "runs"}
        </span>
      {/if}
    </div>
  </header>

  {#if error}
    <div class="error">{error}</div>
  {:else if !recipe}
    <div class="loading">
      <div class="skel" style="height: 240px; width: 100%"></div>
    </div>
  {:else if metrics.length === 0}
    <div class="empty">
      <p class="title">No eval data yet</p>
      <p class="sub">
        Once these runs produce eval_result artifacts, their trajectories
        show up here overlaid by run.
      </p>
    </div>
  {:else}
    <div class="body">
      {#if metrics.length > 1}
        <div class="metric-chips">
          <span class="m-label mono">metric</span>
          {#each metrics as m (m)}
            {@const count = metricRunCount(m)}
            <button
              type="button"
              class="m-chip"
              class:active={activeMetric === m}
              onclick={() => (selectedMetric = m)}
            >
              <span class="text mono">{m}</span>
              <span class="count mono">{count}/{recipe.runs.length}</span>
            </button>
          {/each}
        </div>
      {/if}

      <div class="chart-card">
        <header class="chart-h">
          <span class="metric mono">{activeMetric}</span>
          {#if activeSeries}
            <span class="count">
              {activeSeries.run_count} of {recipe.runs.length} runs
            </span>
          {/if}
        </header>
        <div class="chart-wrap">
          <MetricChart
            series={chartSeries}
            height={360}
            visible={visible}
            highlightedId={highlighted}
            onPointClick={onPointClick}
            onSeriesEnter={(id) => (highlighted = id)}
          />
        </div>

        {#if activeSeries}
          <div class="legend">
            {#each activeSeries.runs as r, i (r.run_id)}
              {@const color = colorFor(i)}
              {@const delta =
                r.previous_value != null && r.latest_value != null
                  ? r.latest_value - r.previous_value
                  : null}
              <div
                class="leg-row"
                class:dim={highlighted != null && highlighted !== r.run_id}
                class:hidden={!isVisible(r.run_id)}
                onmouseenter={() => (highlighted = r.run_id)}
                onmouseleave={() => (highlighted = null)}
              >
                <button
                  type="button"
                  class="leg-toggle"
                  onclick={() => toggleVisible(r.run_id)}
                  aria-label={isVisible(r.run_id) ? "Hide this run" : "Show this run"}
                  aria-pressed={isVisible(r.run_id)}
                  title={isVisible(r.run_id) ? "Hide" : "Show"}
                >
                  <span class="swatch" style="background: {color};"></span>
                </button>
                <button
                  type="button"
                  class="leg-id mono"
                  onclick={() => router.go("runs", r.run_id)}
                >
                  {shortId(r.run_id, 14)}
                </button>
                <span class="leg-val mono">{fmtValue(r.latest_value)}</span>
                {#if delta != null}
                  <span class="leg-delta mono" data-sign={delta >= 0 ? "pos" : "neg"}>
                    {fmtDelta(delta)}
                  </span>
                {:else}
                  <span class="leg-delta mono dim">—</span>
                {/if}
                <span class="leg-step mono">step {fmtStep(r.latest_step)}</span>
              </div>
            {/each}
          </div>
        {/if}
      </div>

      <section class="runs">
        <header class="runs-h">
          <h3>Runs</h3>
          <span class="count">{recipe.runs.length}</span>
        </header>
        <div class="run-list">
          {#each recipe.runs as r (r.id)}
            <button
              type="button"
              class="run-row"
              class:highlighted={highlighted === r.id}
              onmouseenter={() => (highlighted = r.id)}
              onmouseleave={() => (highlighted = null)}
              onclick={() => router.go("runs", r.id)}
            >
              <Pill status={r.status} showLabel={false} />
              <span class="run-id mono">
                <Hash value={r.id} n={12} />
              </span>
              {#if r.stage_name}
                <span class="stage mono">{r.stage_name}</span>
              {:else}
                <span class="stage muted">—</span>
              {/if}
              <RelativeTime ts={r.created_at} />
            </button>
          {/each}
        </div>
      </section>
    </div>
  {/if}
</div>

<style>
  .page { display: flex; flex-direction: column; height: 100%; background: var(--bg-0); overflow: hidden; }
  .header {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--line-0);
    background: var(--bg-0);
    flex-shrink: 0;
  }
  .back {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px 4px 8px;
    background: transparent;
    border: 1px solid var(--line-1);
    border-radius: 4px;
    color: var(--fg-1);
    font-size: 12px;
    cursor: pointer;
  }
  .back:hover { background: var(--bg-2); color: var(--fg-0); border-color: var(--line-2); }
  .title { display: flex; align-items: baseline; gap: 10px; overflow: hidden; }
  .t-label { font-size: 10px; color: var(--fg-3); letter-spacing: 0.06em; text-transform: uppercase; }
  .t-name { font-size: 14px; color: var(--fg-0); }
  .t-count { font-family: theme("fontFamily.mono"); font-size: 11px; color: var(--fg-2); }

  .body {
    flex: 1;
    overflow-y: auto;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 20px;
  }
  .loading, .error { padding: 24px 18px; color: var(--fg-2); font-size: 13px; }

  .metric-chips {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
  }
  .m-label {
    font-size: 10px;
    color: var(--fg-3);
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin-right: 4px;
  }
  .m-chip {
    display: inline-flex;
    align-items: baseline;
    gap: 6px;
    padding: 3px 10px;
    background: transparent;
    border: 1px solid var(--line-1);
    border-radius: 999px;
    color: var(--fg-1);
    cursor: pointer;
  }
  .m-chip:hover { background: var(--bg-2); color: var(--fg-0); }
  .m-chip.active {
    background: var(--accent-soft);
    border-color: var(--accent-dim);
    color: var(--accent-dim);
  }
  .m-chip .text { font-size: 12px; }
  .m-chip .count { font-size: 10px; color: var(--fg-3); }
  .m-chip.active .count { color: var(--accent-dim); opacity: 0.8; }

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

  .legend {
    display: flex;
    flex-direction: column;
    gap: 0;
    margin-top: 12px;
    padding-top: 8px;
    border-top: 1px solid var(--line-0);
  }
  .leg-row {
    display: grid;
    grid-template-columns: 22px minmax(0, 1fr) auto auto auto;
    column-gap: 12px;
    align-items: center;
    padding: 5px 4px;
    border-bottom: 1px dashed var(--line-0);
  }
  .leg-row:last-child { border-bottom: none; }
  .leg-row.dim { opacity: 0.45; }
  .leg-row.hidden .swatch { background: transparent !important; border-color: var(--line-2); }
  .leg-row.hidden .leg-id,
  .leg-row.hidden .leg-val,
  .leg-row.hidden .leg-step,
  .leg-row.hidden .leg-delta {
    color: var(--fg-3) !important;
    text-decoration: line-through;
  }
  .leg-toggle {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    border: none;
    background: transparent;
    cursor: pointer;
    padding: 0;
  }
  .swatch {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 999px;
    border: 2px solid transparent;
  }
  .leg-id {
    background: transparent;
    border: none;
    padding: 0;
    cursor: pointer;
    font-size: 12px;
    color: var(--fg-0);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    text-align: left;
  }
  .leg-id:hover { color: var(--accent-dim); }
  .leg-val {
    font-size: 13px;
    color: var(--fg-0);
    font-variant-numeric: tabular-nums;
    text-align: right;
    min-width: 56px;
  }
  .leg-delta {
    font-size: 11px;
    font-variant-numeric: tabular-nums;
    min-width: 48px;
    text-align: right;
  }
  .leg-delta[data-sign="pos"] { color: var(--status-succeeded-fg); }
  .leg-delta[data-sign="neg"] { color: var(--status-failed-fg); }
  .leg-delta.dim { color: var(--fg-3); }
  .leg-step { font-size: 11px; color: var(--fg-3); min-width: 64px; text-align: right; }

  .runs { display: flex; flex-direction: column; gap: 6px; }
  .runs-h { display: flex; align-items: baseline; justify-content: space-between; }
  .runs-h h3 {
    font-size: 11px;
    font-family: theme("fontFamily.mono");
    color: var(--fg-3);
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin: 0;
  }
  .runs-h .count {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: var(--fg-2);
  }
  .run-list {
    display: flex;
    flex-direction: column;
    border: 1px solid var(--line-0);
    border-radius: 6px;
    overflow: hidden;
  }
  .run-row {
    display: grid;
    grid-template-columns: 22px 140px 1fr auto;
    column-gap: 14px;
    align-items: center;
    padding: 8px 14px;
    border: none;
    border-bottom: 1px solid var(--line-0);
    background: transparent;
    color: inherit;
    font: inherit;
    text-align: left;
    cursor: pointer;
    width: 100%;
  }
  .run-row:last-child { border-bottom: none; }
  .run-row:hover, .run-row.highlighted { background: var(--bg-2); }
  .run-id { font-family: theme("fontFamily.mono"); font-size: 12px; }
  .stage { font-size: 11px; color: var(--fg-1); }
  .muted { color: var(--fg-3); }

  .empty { padding: 60px 24px; text-align: center; }
  .empty .title { font-size: 14px; color: var(--fg-0); margin: 0 0 6px 0; }
  .empty .sub { font-size: 13px; color: var(--fg-2); margin: 0 auto; max-width: 480px; }
</style>
