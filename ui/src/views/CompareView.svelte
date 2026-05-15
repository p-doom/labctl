<script lang="ts">
  // Cross-run comparison. URL-driven (#/compare?runs=a,b,c). Pivot is
  // by metric, not by policy: pick a metric, see every selected run on
  // a single chart on a shared y-axis. Switch metrics with one click.

  import { store, loadCompareView } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { compareSelection } from "../lib/compare.svelte";
  import type { MetricSeries, MetricSeriesPoint } from "../lib/types";

  import MetricChart, { type ChartSeries } from "../components/MetricChart.svelte";
  import Pill from "../components/Pill.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Hash from "../components/Hash.svelte";
  import Icon from "../components/Icon.svelte";
  import DetailHeader from "../components/DetailHeader.svelte";
  import MetricChips from "../components/MetricChips.svelte";
  import ChartCard from "../components/ChartCard.svelte";
  import LegendRow from "../components/LegendRow.svelte";
  import EmptyState from "../components/EmptyState.svelte";
  import { seriesColor as colorFor } from "../lib/chart-colors";

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

  function close() { router.go("runs"); }
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
  <DetailHeader
    label="compare"
    name={`${ids.length} ${ids.length === 1 ? "run" : "runs"}`}
    meta={runsByRecipe.size > 0 ? `across ${runsByRecipe.size} ${runsByRecipe.size === 1 ? "recipe" : "recipes"}` : undefined}
    backLabel="Runs"
    onBack={close}
  />

  {#if ids.length === 0}
    <EmptyState title="No runs selected">
      {#snippet sub()}
        Select runs in the runs list (click the checkbox on the left of
        each row), then click "Compare" in the floating bar.
      {/snippet}
    </EmptyState>
  {:else if error}
    <div class="error">{error}</div>
  {:else if !view}
    <div class="loading">
      <div class="skel" style="height: 240px; width: 100%"></div>
    </div>
  {:else if metrics.length === 0}
    <EmptyState title="No eval metrics across these runs">
      {#snippet sub()}
        These runs don't share any metrics that labctl can recognize.
        Either evals haven't completed yet, or their result.json doesn't
        contain a recognizable metric dict.
      {/snippet}
    </EmptyState>
  {:else}
    <div class="body">
      {#if metrics.length > 1}
        <MetricChips
          metrics={metrics}
          active={activeMetric}
          runCount={metricRunCount}
          totalRuns={view.runs.length}
          onSelect={(m) => (selectedMetric = m)}
        />
      {/if}

      <ChartCard
        metric={activeMetric}
        subtitle={activeSeries ? `${activeSeries.run_count} of ${view.runs.length} runs` : undefined}
      >
        {#snippet chart()}
          <MetricChart
            series={chartSeries}
            height={360}
            visible={visible}
            highlightedId={highlighted}
            onPointClick={onPointClick}
            onSeriesEnter={(id) => (highlighted = id)}
          />
        {/snippet}
        {#snippet legend()}
          {#if activeSeries}
            {#each activeSeries.runs as r, i (r.run_id)}
              {@const color = colorFor(i)}
              {@const delta =
                r.previous_value != null && r.latest_value != null
                  ? r.latest_value - r.previous_value
                  : null}
              <LegendRow
                runId={r.run_id}
                recipe={r.run_recipe_name}
                color={color}
                latestValue={r.latest_value}
                delta={delta}
                latestStep={r.latest_step}
                visible={isVisible(r.run_id)}
                dimmed={highlighted != null && highlighted !== r.run_id}
                onToggleVisible={() => toggleVisible(r.run_id)}
                onEnter={() => (highlighted = r.run_id)}
                onLeave={() => (highlighted = null)}
              />
            {/each}
          {/if}
        {/snippet}
      </ChartCard>

      <section class="runs">
        <header class="runs-h">
          <h3>Selected runs</h3>
          <span class="count">{view.runs.length}</span>
        </header>
        <div class="run-list">
          {#each view.runs as r (r.id)}
            <div
              class="list-row run-row"
              data-state={highlighted === r.id ? "active" : undefined}
              onmouseenter={() => (highlighted = r.id)}
              onmouseleave={() => (highlighted = null)}
              role="presentation"
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
                class="iconbtn remove"
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
    /* Legend grid: swatch | recipe | id | latest | Δ | step */
    --legend-cols: 22px minmax(0, 1.5fr) 96px 72px 56px 56px;
  }
  .body {
    flex: 1;
    overflow-y: auto;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 20px;
  }
  .loading, .error { padding: 24px 16px; color: var(--fg-2); font-size: 13px; }

  .runs { display: flex; flex-direction: column; gap: 8px; }
  .runs-h {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
  }
  .runs-h h3 {
    font-size: 12px;
    font-weight: 500;
    color: var(--fg-1);
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
    grid-template-columns: 22px minmax(0, 1.5fr) 140px auto 26px;
    cursor: default;
  }
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
    width: 22px;
    height: 22px;
  }
  .remove:hover { color: var(--status-failed-fg); }
</style>
