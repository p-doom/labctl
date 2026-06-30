<script lang="ts">
  // Full-page rollout browser. Steps through every checkpoint-eval of one
  // training run + policy, in checkpoint-step order, without bouncing back
  // to the lineage graph between evals.
  //
  // Data: GET /runs/:id/rollouts returns the run's evals aggregated one
  // export hop down (training run → per-checkpoint export → osworld eval),
  // grouped by policy and step-sorted, each point carrying
  // `eval_result_artifact_id` + `has_rollout` (see get_run_rollouts in
  // src/server.rs). We pick a policy series, list its checkpoints in the left
  // rail, and render the selected eval's rollout via <Result> (which handles
  // single- vs multi-instruction shapes + the frame viewer).
  //
  // Keyboard: ↑/↓ step between checkpoints (rail is vertical). ←/→ are left to
  // the RolloutViewer for frame scrubbing, so the two never collide.

  import { api } from "../lib/api";
  import { store, loadArtifactDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import DetailHeader from "../components/DetailHeader.svelte";
  import Result from "../components/Result.svelte";
  import Pill from "../components/Pill.svelte";
  import EmptyState from "../components/EmptyState.svelte";
  import type { EvalSeries, EvalSeriesPoint, RolloutSeriesResponse } from "../lib/types";

  interface Props {
    runId: string;
  }
  let { runId }: Props = $props();

  let data = $state<RolloutSeriesResponse | null>(null);
  let error = $state<string | null>(null);

  $effect(() => {
    if (!runId) return;
    data = null;
    error = null;
    api.runRollouts(runId).then((d) => (data = d)).catch((e) => {
      error = e instanceof Error ? e.message : String(e);
    });
  });

  // --- pick the policy series ---
  let policyParam = $derived(router.query.get("policy"));
  let series = $derived.by<EvalSeries | null>(() => {
    const all = data?.series ?? [];
    if (all.length === 0) return null;
    if (policyParam) {
      const m = all.find((s) => s.policy_id === policyParam);
      if (m) return m;
    }
    // Default: first series that actually has a browsable rollout.
    return all.find((s) => s.points.some((p) => p.has_rollout)) ?? all[0];
  });

  let points = $derived(series?.points ?? []);
  // Indices (into `points`) that have a browsable rollout. Prev/Next hop
  // between these, skipping pending/metric-only checkpoints.
  let rolloutIdxs = $derived(
    points.flatMap((p, i) => (p.has_rollout && p.eval_result_artifact_id ? [i] : [])),
  );

  // --- current selection (URL-driven, replace-state so Back returns to run) ---
  let current = $derived.by(() => {
    const raw = parseInt(router.query.get("i") ?? "", 10);
    if (Number.isFinite(raw) && raw >= 0 && raw < points.length) return raw;
    return rolloutIdxs[0] ?? 0;
  });
  let currentPoint = $derived<EvalSeriesPoint | null>(points[current] ?? null);
  let currentArtifactId = $derived(currentPoint?.eval_result_artifact_id ?? null);

  // Position among rollout-bearing checkpoints, for the "(n/m)" indicator.
  let rolloutPos = $derived(rolloutIdxs.indexOf(current));

  function goToIndex(i: number) {
    if (i < 0 || i >= points.length) return;
    router.setQueryReplace({ i: String(i) });
  }
  function prevEval() {
    const before = rolloutIdxs.filter((i) => i < current);
    if (before.length) goToIndex(before[before.length - 1]);
  }
  function nextEval() {
    const after = rolloutIdxs.find((i) => i > current);
    if (after !== undefined) goToIndex(after);
  }
  let canPrev = $derived(rolloutIdxs.some((i) => i < current));
  let canNext = $derived(rolloutIdxs.some((i) => i > current));

  // --- fetch the selected eval_result's metadata.result for <Result> ---
  $effect(() => {
    if (currentArtifactId) loadArtifactDetail(currentArtifactId).catch(() => {});
  });
  let artifactDetail = $derived(
    currentArtifactId ? store.artifactDetail(currentArtifactId) : null,
  );
  let result = $derived(
    (artifactDetail?.artifact.metadata as { result?: unknown } | undefined)?.result ?? null,
  );

  // Keep the selected checkpoint visible in the rail as you step through
  // (↑/↓, Prev/Next, or chart entry). `block: "nearest"` only scrolls the
  // rail when the active row has gone out of bounds — no manual scrolling.
  let railEl = $state<HTMLElement | null>(null);
  $effect(() => {
    void current;
    const row = railEl?.querySelector<HTMLElement>(".rail-row.active");
    row?.scrollIntoView({ block: "nearest" });
  });

  // Selected instruction (task index) for multi-instruction evals. Owned here
  // so it survives checkpoint switches — the viewer remounts while the next
  // eval's detail loads, which would otherwise reset the selection to task 0.
  let taskIdx = $state<number | undefined>(undefined);

  function back() {
    router.go("runs", runId);
  }

  function fmtStep(s: number | null): string {
    if (s == null) return "—";
    if (s >= 1000) return `${(s / 1000).toFixed(s % 1000 === 0 ? 0 : 1)}k`;
    return String(s);
  }
  function fmtValue(v: number | null): string {
    if (v == null) return "—";
    if (Math.abs(v) >= 100) return v.toFixed(1);
    if (Math.abs(v) >= 1) return v.toFixed(3);
    return v.toFixed(4);
  }

  function onKey(e: KeyboardEvent) {
    if (router.view !== "rollouts") return;
    const t = e.target as HTMLElement | null;
    if (t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.isContentEditable)) return;
    if (e.key === "ArrowUp" || e.key === "k") {
      e.preventDefault();
      prevEval();
    } else if (e.key === "ArrowDown" || e.key === "j") {
      e.preventDefault();
      nextEval();
    }
  }
  $effect(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  let runName = $derived(data?.recipe_name ?? runId);

  // Series that actually have browsable rollouts — drives the switcher.
  let rolloutSeries = $derived(
    (data?.series ?? []).filter((s) => s.points.some((p) => p.has_rollout)),
  );
  function selectPolicy(p: string) {
    // New series → clear the checkpoint index so it lands on that series'
    // first rollout.
    router.setQueryReplace({ policy: p, i: null });
  }
</script>

<div class="browser">
  <DetailHeader
    label="rollouts"
    name={runName}
    meta={rolloutSeries.length > 1 ? undefined : series?.policy_id}
    backLabel="run"
    onBack={back}
  >
    {#snippet actions()}
      {#if rolloutSeries.length > 1}
        <select
          class="policy-select"
          value={series?.policy_id}
          onchange={(e) => selectPolicy(e.currentTarget.value)}
          title="Eval series"
        >
          {#each rolloutSeries as s (s.policy_id)}
            <option value={s.policy_id}>{s.policy_id}</option>
          {/each}
        </select>
      {/if}
      {#if rolloutIdxs.length}
        <div class="stepper">
          <button class="step-btn" onclick={prevEval} disabled={!canPrev} title="Previous checkpoint (↑)">◀ Prev</button>
          <span class="step-pos mono">
            {#if currentPoint}step {fmtStep(currentPoint.step)}{/if}
            {#if rolloutPos >= 0}<span class="muted"> · {rolloutPos + 1}/{rolloutIdxs.length}</span>{/if}
          </span>
          <button class="step-btn" onclick={nextEval} disabled={!canNext} title="Next checkpoint (↓)">Next ▶</button>
        </div>
      {/if}
    {/snippet}
  </DetailHeader>

  {#if error}
    <div class="pad"><p class="err">Failed to load: {error}</p></div>
  {:else if !data}
    <div class="pad"><div class="skel" style="height: 320px; border-radius: 8px;"></div></div>
  {:else if !series || rolloutIdxs.length === 0}
    <EmptyState title="No rollouts for this run.">
      {#snippet sub()}
        None of this run's checkpoint evals recorded a GUI rollout yet. They may
        still be pending, or this policy only reports metrics.
      {/snippet}
    </EmptyState>
  {:else}
    <div class="body">
      <!-- left rail: every checkpoint, step-ordered -->
      <nav class="rail" aria-label="checkpoints" bind:this={railEl}>
        {#each points as p, i (`${p.step}-${p.checkpoint_artifact_id}`)}
          {@const browsable = p.has_rollout && !!p.eval_result_artifact_id}
          <button
            type="button"
            class="rail-row"
            class:active={i === current}
            class:disabled={!browsable}
            onclick={() => browsable && goToIndex(i)}
            disabled={!browsable}
            title={browsable ? `step ${fmtStep(p.step)}` : `step ${fmtStep(p.step)} — no rollout`}
          >
            <Pill status={p.state} showLabel={false} />
            <span class="r-step mono">{fmtStep(p.step)}</span>
            <span class="r-val mono">{fmtValue(p.value)}</span>
          </button>
        {/each}
      </nav>

      <!-- main: the selected eval's rollout -->
      <section class="stage">
        {#if !currentArtifactId}
          <p class="err">This checkpoint has no rollout. Pick another from the rail.</p>
        {:else if !artifactDetail}
          <div class="skel" style="height: 360px; border-radius: 8px;"></div>
        {:else if result}
          <!-- task is owned here (controlled) so the chosen instruction
               survives checkpoint switches even though the viewer remounts
               while the next eval's detail loads. -->
          <Result
            {result}
            artifactId={currentArtifactId}
            task={taskIdx}
            onTaskChange={(i) => (taskIdx = i)}
          />
        {:else}
          <p class="err">No result payload for this eval.</p>
        {/if}
      </section>
    </div>
  {/if}
</div>

<style>
  .browser {
    display: flex;
    flex-direction: column;
    height: 100%;
    min-height: 0;
    background: var(--bg-0);
  }
  .pad { padding: 24px; }
  .err { color: var(--status-failed); font-size: 13px; }

  .policy-select {
    max-width: 320px;
    padding: 4px 8px;
    border: 1px solid var(--line-1);
    border-radius: 5px;
    background: var(--bg-1);
    color: var(--fg-1);
    font-size: 12px;
    font-family: theme("fontFamily.mono");
    cursor: pointer;
  }

  .stepper {
    display: inline-flex;
    align-items: center;
    gap: 10px;
  }
  .step-btn {
    border: 1px solid var(--line-1);
    background: var(--bg-1);
    color: var(--fg-1);
    border-radius: 5px;
    padding: 4px 10px;
    font-size: 12px;
    cursor: pointer;
  }
  .step-btn:hover:not(:disabled) { background: var(--bg-2); color: var(--fg-0); }
  .step-btn:disabled { opacity: 0.4; cursor: default; }
  .step-pos { font-size: 12px; color: var(--fg-0); font-variant-numeric: tabular-nums; }
  .muted { color: var(--fg-3); }

  .body {
    flex: 1;
    min-height: 0;
    display: flex;
  }

  .rail {
    flex: 0 0 200px;
    overflow-y: auto;
    border-right: 1px solid var(--line-1);
    padding: 8px;
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .rail-row {
    display: grid;
    grid-template-columns: 18px 1fr auto;
    align-items: center;
    gap: 8px;
    padding: 6px 8px;
    border-radius: 5px;
    border: 1px solid transparent;
    background: transparent;
    color: inherit;
    font: inherit;
    text-align: left;
    cursor: pointer;
    width: 100%;
  }
  .rail-row:hover:not(.disabled) { background: var(--bg-2); }
  .rail-row.active {
    background: var(--accent-soft);
    border-color: var(--accent);
  }
  .rail-row.disabled { opacity: 0.4; cursor: default; }
  .r-step { font-size: 12px; color: var(--fg-0); }
  .r-val {
    font-size: 11px;
    color: var(--fg-2);
    font-variant-numeric: tabular-nums;
  }

  .stage {
    flex: 1;
    min-width: 0;
    overflow-y: auto;
    padding: 20px 24px;
  }
</style>
