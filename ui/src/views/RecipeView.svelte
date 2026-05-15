<script lang="ts">
  // Recipe view: every run of a recipe overlaid on a single chart per
  // metric (chip selector for switching). Same shape as Compare; only
  // the source of runs differs (recipe membership vs user selection).

  import { store, loadRecipeView } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import type { MetricSeries, MetricSeriesPoint } from "../lib/types";

  import MetricChart, { type ChartSeries } from "../components/MetricChart.svelte";
  import Pill from "../components/Pill.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Hash from "../components/Hash.svelte";
  import DetailHeader from "../components/DetailHeader.svelte";
  import MetricChips from "../components/MetricChips.svelte";
  import ChartCard from "../components/ChartCard.svelte";
  import LegendRow from "../components/LegendRow.svelte";
  import EmptyState from "../components/EmptyState.svelte";
  import { seriesColor as colorFor } from "../lib/chart-colors";

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
  function metricRunCount(name: string): number {
    return recipe?.series_by_metric.find((s) => s.metric_name === name)?.run_count ?? 0;
  }
</script>

<div class="page">
  <DetailHeader
    label="recipe"
    name={recipeName}
    meta={recipe ? `${recipe.runs.length} ${recipe.runs.length === 1 ? "run" : "runs"}` : undefined}
    backLabel="Runs"
    onBack={close}
  />

  {#if error}
    <div class="error">{error}</div>
  {:else if !recipe}
    <div class="loading">
      <div class="skel" style="height: 240px; width: 100%"></div>
    </div>
  {:else if metrics.length === 0}
    <EmptyState title="No eval data yet">
      {#snippet sub()}
        Once these runs produce eval_result artifacts, their trajectories
        show up here overlaid by run.
      {/snippet}
    </EmptyState>
  {:else}
    <div class="body">
      {#if metrics.length > 1}
        <MetricChips
          metrics={metrics}
          active={activeMetric}
          runCount={metricRunCount}
          totalRuns={recipe.runs.length}
          onSelect={(m) => (selectedMetric = m)}
        />
      {/if}

      <ChartCard
        metric={activeMetric}
        subtitle={activeSeries ? `${activeSeries.run_count} of ${recipe.runs.length} runs` : undefined}
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
          <h3>Runs</h3>
          <span class="count">{recipe.runs.length}</span>
        </header>
        <div class="run-list">
          {#each recipe.runs as r (r.id)}
            <button
              type="button"
              class="list-row run-row"
              data-state={highlighted === r.id ? "active" : undefined}
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
                <span class="stage dim">—</span>
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
  .page {
    display: flex;
    flex-direction: column;
    height: 100%;
    background: var(--bg-0);
    overflow: hidden;
    /* Recipe view legend omits the recipe column (all runs share one). */
    --legend-cols: 22px minmax(0, 1fr) 72px 56px 56px;
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
  .runs-h { display: flex; align-items: baseline; justify-content: space-between; }
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
    grid-template-columns: 22px 140px 1fr auto;
  }
  .run-id { font-family: theme("fontFamily.mono"); font-size: 12px; }
  .stage { font-size: 11px; color: var(--fg-1); }
</style>
