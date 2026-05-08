<script lang="ts">
  // Cross-run comparison. URL-driven (#/compare?runs=a,b,c). Pivot is
  // by metric, not by policy: pick a metric, see every selected run on
  // a single chart on a shared y-axis. Switch metrics with one click.

  import { store, loadCompareView } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { compareSelection } from "../lib/compare.svelte";
  import { shortId } from "../lib/format";
  import type { MetricSeries, MetricSeriesPoint, MetricSeriesRun } from "../lib/types";

  import MetricChart, { type ChartSeries } from "../components/MetricChart.svelte";
  import Pill from "../components/Pill.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Hash from "../components/Hash.svelte";
  import Icon from "../components/Icon.svelte";

  let ids = $derived.by(() => {
    const raw = router.query.get("runs") ?? "";
    return raw.split(",").map((s) => s.trim()).filter(Boolean);
  });

  let view = $derived(store.compareView(ids));
  let error = $state<string | null>(null);
  let highlighted = $state<string | null>(null);
  let selectedMetric = $state<string | null>(null);
  let visible = $state<Record<string, boolean>>({});

  $effect(() => {
    if (ids.length === 0) return;
    loadCompareView(ids)
      .then(() => { error = null; })
      .catch((e) => { error = e instanceof Error ? e.message : String(e); });
  });

  // Reset metric/visibility when the underlying selection changes.
  $effect(() => {
    void ids.join(",");
    selectedMetric = null;
    visible = {};
  });

  let metrics = $derived(view?.metrics ?? []);
  let activeMetric = $derived(selectedMetric ?? metrics[0] ?? null);
  let activeSeries = $derived<MetricSeries | null>(
    view?.series_by_metric.find((s) => s.metric_name === activeMetric) ?? null,
  );

  // Color cycle — same palette across compare and recipe views so the
  // same run gets the same color in both surfaces.
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

  function close() {
    router.go("runs");
  }
  function onPointClick(_seriesId: string, p: MetricSeriesPoint) {
    if (p.eval_run_id) router.go("runs", p.eval_run_id);
  }
  function removeFromCompare(id: string) {
    const remaining = ids.filter((x) => x !== id);
    if (remaining.length === 0) {
      compareSelection.clear();
      router.go("runs");
      return;
    }
    const q = new URLSearchParams({ runs: remaining.join(",") });
    router.go("compare", null, q);
  }

  let runsByRecipe = $derived.by(() => {
    if (!view) return new Map<string, number>();
    const m = new Map<string, number>();
    for (const r of view.runs) m.set(r.recipe_name, (m.get(r.recipe_name) ?? 0) + 1);
    return m;
  });

  function metricRunCount(name: string): number {
    return view?.series_by_metric.find((s) => s.metric_name === name)?.run_count ?? 0;
  }
</script>

<div class="page">
  <header class="header">
    <button type="button" class="back" onclick={close} aria-label="Back to runs">
      <Icon name="back" size={14} />
      <span>Runs</span>
    </button>
    <div class="title">
      <span class="t-label mono">compare</span>
      <span class="t-name">
        {ids.length} {ids.length === 1 ? "run" : "runs"}
      </span>
      {#if runsByRecipe.size > 0}
        <span class="t-recipes mono">
          across {runsByRecipe.size} {runsByRecipe.size === 1 ? "recipe" : "recipes"}
        </span>
      {/if}
    </div>
  </header>

  {#if ids.length === 0}
    <div class="empty">
      <p class="title">No runs selected</p>
      <p class="sub">
        Select runs in the runs list (click the checkbox on the left of
        each row), then click "Compare" in the floating bar.
      </p>
    </div>
  {:else if error}
    <div class="error">{error}</div>
  {:else if !view}
    <div class="loading">
      <div class="skel" style="height: 240px; width: 100%"></div>
    </div>
  {:else if metrics.length === 0}
    <div class="empty">
      <p class="title">No eval metrics across these runs</p>
      <p class="sub">
        These runs don't share any metrics that labctl can recognize.
        Either evals haven't completed yet, or their result.json doesn't
        contain a recognizable metric dict.
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
              <span class="count mono">{count}/{view.runs.length}</span>
            </button>
          {/each}
        </div>
      {/if}

      <div class="chart-card">
        <header class="chart-h">
          <span class="metric mono">{activeMetric}</span>
          {#if activeSeries}
            <span class="count">
              {activeSeries.run_count} of {view.runs.length} runs
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
                  class="leg-recipe mono"
                  onclick={() => router.go("recipes", r.run_recipe_name)}
                  title={`All runs of ${r.run_recipe_name}`}
                >{r.run_recipe_name}</button>
                <button
                  type="button"
                  class="leg-id mono"
                  onclick={() => router.go("runs", r.run_id)}
                >
                  {shortId(r.run_id, 12)}
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
          <h3>Selected runs</h3>
          <span class="count">{view.runs.length}</span>
        </header>
        <div class="run-list">
          {#each view.runs as r (r.id)}
            <div
              class="run-row"
              class:highlighted={highlighted === r.id}
              onmouseenter={() => (highlighted = r.id)}
              onmouseleave={() => (highlighted = null)}
            >
              <Pill status={r.status} showLabel={false} />
              <button type="button" class="recipe-link mono" onclick={() => router.go("recipes", r.recipe_name)}>
                {r.recipe_name}
              </button>
              <button type="button" class="run-id-link mono" onclick={() => router.go("runs", r.id)}>
                <Hash value={r.id} n={12} />
              </button>
              <RelativeTime ts={r.created_at} />
              <button
                type="button"
                class="remove"
                onclick={() => removeFromCompare(r.id)}
                aria-label="Remove from comparison"
                title="Remove from comparison"
              >
                <Icon name="close" size={12} />
              </button>
            </div>
          {/each}
        </div>
      </section>
    </div>
  {/if}
</div>

<style>
  .page {
    display: flex;
    flex-direction: column;
    height: 100%;
    background: var(--bg-0);
    overflow: hidden;
  }
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
  .t-label {
    font-size: 10px;
    color: var(--fg-3);
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }
  .t-name { font-size: 14px; color: var(--fg-0); }
  .t-recipes { font-size: 11px; color: var(--fg-2); }

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
  .m-chip .count {
    font-size: 10px;
    color: var(--fg-3);
  }
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
  .chart-wrap {
    padding: 0;
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
    grid-template-columns: 22px minmax(0, 1.5fr) 110px auto auto auto;
    column-gap: 12px;
    align-items: center;
    padding: 5px 4px;
    border-bottom: 1px dashed var(--line-0);
  }
  .leg-row:last-child { border-bottom: none; }
  .leg-row.dim { opacity: 0.45; }
  .leg-row.hidden .swatch { background: transparent !important; border-color: var(--line-2); }
  .leg-row.hidden .leg-recipe,
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
  .leg-recipe,
  .leg-id {
    background: transparent;
    border: none;
    padding: 0;
    cursor: pointer;
    font-size: 12px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    text-align: left;
  }
  .leg-recipe { color: var(--fg-0); }
  .leg-recipe:hover { color: var(--accent-dim); }
  .leg-id { color: var(--fg-2); font-size: 11px; }
  .leg-id:hover { color: var(--fg-0); }
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
  .runs-h {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
  }
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
    grid-template-columns: 22px minmax(0, 1.5fr) 140px auto 26px;
    column-gap: 14px;
    align-items: center;
    padding: 8px 14px;
    border-bottom: 1px solid var(--line-0);
  }
  .run-row:last-child { border-bottom: none; }
  .run-row:hover, .run-row.highlighted { background: var(--bg-2); }
  .recipe-link, .run-id-link {
    background: transparent;
    border: none;
    padding: 0;
    text-align: left;
    cursor: pointer;
    font-size: 12px;
    color: var(--fg-0);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .recipe-link:hover { color: var(--accent-dim); }
  .run-id-link { color: var(--fg-1); }
  .run-id-link:hover { color: var(--fg-0); }
  .remove {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    border-radius: 4px;
    background: transparent;
    border: none;
    color: var(--fg-3);
    cursor: pointer;
  }
  .remove:hover { background: var(--bg-3); color: var(--status-failed-fg); }

  .empty {
    padding: 80px 24px;
    text-align: center;
  }
  .empty .title { font-size: 14px; color: var(--fg-0); margin: 0 0 6px 0; }
  .empty .sub { font-size: 13px; color: var(--fg-2); margin: 0 auto; max-width: 480px; }
</style>
