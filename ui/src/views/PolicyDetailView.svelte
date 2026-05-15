<script lang="ts">
  // Policy detail. Pre-pivoted CompareView: every training run that's
  // produced eval data under this policy, overlaid on a single chart per
  // metric. Below: leaderboard sorted by latest value, with deltas.
  // Bottom drawer: raw eval_requests for the policy (the activity log).

  import { store, loadPolicyDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { statusGroup } from "../lib/format";
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

  interface Props {
    policyName: string;
  }
  let { policyName }: Props = $props();

  let detail = $derived(store.policyDetail(policyName));
  let error = $state<string | null>(null);
  let highlighted = $state<string | null>(null);
  let selectedMetric = $state<string | null>(null);
  let visible = $state<Record<string, boolean>>({});
  let activityOpen = $state(false);

  $effect(() => {
    if (!policyName) return;
    loadPolicyDetail(policyName)
      .then(() => { error = null; })
      .catch((e) => { error = e instanceof Error ? e.message : String(e); });
  });

  // Reset metric/visibility when policy switches under us.
  $effect(() => {
    void policyName;
    selectedMetric = null;
    visible = {};
    activityOpen = false;
  });

  let metrics = $derived(detail?.metrics ?? []);
  let activeMetric = $derived(selectedMetric ?? metrics[0] ?? null);
  let activeSeries = $derived<MetricSeries | null>(
    detail?.series_by_metric.find((s) => s.metric_name === activeMetric) ?? null,
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

  // Leaderboard sort: runs with a value first, then by value desc. Ties
  // resolve by run created_at desc (newer wins).
  let leaderboard = $derived.by(() => {
    if (!activeSeries) return [];
    return activeSeries.runs.slice().sort((a, b) => {
      const va = a.latest_value;
      const vb = b.latest_value;
      if (va == null && vb == null) return b.run_created_at - a.run_created_at;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (va !== vb) return vb - va;
      return b.run_created_at - a.run_created_at;
    });
  });

  function toggleVisible(runId: string) {
    visible = { ...visible, [runId]: visible[runId] === false };
  }
  function isVisible(runId: string): boolean {
    return visible[runId] !== false;
  }

  function close() { router.go("policies"); }
  function onPointClick(_seriesId: string, p: MetricSeriesPoint) {
    if (p.eval_run_id) router.go("runs", p.eval_run_id);
  }
  function metricRunCount(name: string): number {
    return detail?.series_by_metric.find((s) => s.metric_name === name)?.run_count ?? 0;
  }
</script>

<div class="page">
  <DetailHeader
    label="policy"
    name={policyName}
    meta={detail ? `${detail.requests.length} eval${detail.requests.length === 1 ? "" : "s"} · across ${detail.runs.length} training run${detail.runs.length === 1 ? "" : "s"}` : undefined}
    backLabel="Policies"
    onBack={close}
  />

  {#if error}
    <div class="error">{error}</div>
  {:else if !detail}
    <div class="loading">
      <div class="skel" style="height: 240px; width: 100%"></div>
    </div>
  {:else if metrics.length === 0}
    <EmptyState title="No metric values yet">
      {#snippet sub()}
        {detail.requests.length === 0
          ? "This policy has no eval requests on record."
          : "Eval requests exist but their result artifacts don't yet contain a recognizable metric dict. Trajectories will appear once they do."}
      {/snippet}
    </EmptyState>
  {:else}
    <div class="body">
      {#if metrics.length > 1}
        <MetricChips
          metrics={metrics}
          active={activeMetric}
          runCount={metricRunCount}
          totalRuns={detail.runs.length}
          onSelect={(m) => (selectedMetric = m)}
        />
      {/if}

      <ChartCard
        metric={activeMetric}
        subtitle={activeSeries ? `${activeSeries.run_count} of ${detail.runs.length} runs` : undefined}
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
        {#snippet legendHead()}
          <span></span>
          <span>run</span>
          <span>id</span>
          <span class="r">latest</span>
          <span class="r">Δ prev</span>
          <span class="r">step</span>
        {/snippet}
        {#snippet legend()}
          {#if activeSeries}
            {#each leaderboard as r, i (r.run_id)}
              {@const sortIdx = activeSeries.runs.findIndex((x) => x.run_id === r.run_id)}
              {@const color = colorFor(sortIdx >= 0 ? sortIdx : i)}
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

      <section class="activity">
        <button
          type="button"
          class="btn-secondary activity-toggle"
          onclick={() => (activityOpen = !activityOpen)}
          aria-expanded={activityOpen}
        >
          <Icon name={activityOpen ? "chevron-down" : "chevron-right"} size={12} />
          <span>Activity</span>
          <span class="count mono">{detail.requests.length}</span>
        </button>
        {#if activityOpen}
          <div class="activity-list">
            <div class="a-head mono">
              <span></span>
              <span>checkpoint</span>
              <span>eval run</span>
              <span>updated</span>
            </div>
            {#each detail.requests as req (req.eval_key)}
              {@const eg = statusGroup(req.state)}
              <div class="a-row">
                <Pill status={eg} showLabel={false} />
                <button
                  type="button"
                  class="link mono"
                  onclick={() => router.go("artifacts", req.checkpoint_artifact_id)}
                >
                  <Hash value={req.checkpoint_artifact_id} n={10} />
                </button>
                {#if req.eval_run_id}
                  <button
                    type="button"
                    class="link mono"
                    onclick={() => router.go("runs", req.eval_run_id!)}
                  >
                    <Hash value={req.eval_run_id} n={10} />
                  </button>
                {:else}
                  <span class="dim">—</span>
                {/if}
                {#if req.updated_at}
                  <RelativeTime ts={req.updated_at} />
                {:else}
                  <span class="dim">—</span>
                {/if}
              </div>
            {/each}
          </div>
        {/if}
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

  .activity {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .activity-toggle {
    align-self: flex-start;
  }
  .activity-toggle .count {
    font-size: 11px;
    color: var(--fg-3);
  }
  .activity-list {
    display: flex;
    flex-direction: column;
    border: 1px solid var(--line-0);
    border-radius: 6px;
    overflow: hidden;
  }
  .a-head,
  .a-row {
    display: grid;
    grid-template-columns: 22px 96px 96px 1fr;
    column-gap: 12px;
    align-items: center;
    padding: 6px 12px;
    font-size: 12px;
  }
  .a-head {
    font-size: 11px;
    font-weight: 500;
    color: var(--fg-1);
    border-bottom: 1px solid var(--line-0);
    background: var(--bg-0);
  }
  .a-row {
    border-bottom: 1px solid var(--line-0);
  }
  .a-row:last-child { border-bottom: none; }
  .link {
    background: transparent;
    border: none;
    cursor: pointer;
    color: var(--fg-1);
    font-size: 11px;
    padding: 0;
    text-align: left;
  }
  .link:hover { color: var(--fg-0); }
</style>
