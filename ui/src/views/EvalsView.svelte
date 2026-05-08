<script lang="ts">
  import { onMount } from "svelte";
  import { store, loadEvals } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import Pill from "../components/Pill.svelte";
  import Hash from "../components/Hash.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterChips from "../components/FilterChips.svelte";
  import type { ChipDef } from "../lib/filters";

  onMount(() => loadEvals());

  let stateFilter = $derived(router.query.get("state"));
  let policyFilter = $derived(router.query.get("policy"));

  let allEvals = $derived(store.evals.data ?? []);
  let isLoading = $derived(store.evals.loading && allEvals.length === 0);
  let filtered = $derived.by(() => {
    return allEvals.filter((e) => {
      if (stateFilter && stateToGroup(e.state) !== stateFilter) return false;
      if (policyFilter && e.policy_id !== policyFilter) return false;
      return true;
    });
  });

  let counts = $derived.by(() => {
    const c = { running: 0, succeeded: 0, failed: 0, pending: 0 };
    for (const e of allEvals) {
      const g = stateToGroup(e.state);
      if (g === "running" || g === "succeeded" || g === "failed" || g === "pending") c[g]++;
    }
    return c;
  });

  let policies = $derived.by(() => {
    const s = new Set<string>();
    for (const e of allEvals) s.add(e.policy_id);
    return [...s].sort();
  });

  let stateChips = $derived<ChipDef[]>([
    { key: null, label: "All", count: allEvals.length, always: true },
    { key: "running", label: "Running", count: counts.running, dot: "running" },
    { key: "failed", label: "Failed", count: counts.failed, dot: "failed" },
    { key: "succeeded", label: "Succeeded", count: counts.succeeded, dot: "succeeded" },
    { key: "pending", label: "Pending", count: counts.pending, dot: "pending" },
  ]);
  let policyChips = $derived<ChipDef[]>(
    policies.map((p) => ({
      key: p,
      label: p,
      count: allEvals.filter((e) => e.policy_id === p).length,
    })),
  );
</script>

<div class="page">
  <FilterBar>
    <FilterChips chips={stateChips} active={stateFilter} onSelect={(k) => router.setQuery({ state: k })} />
    {#if policies.length > 1}
      <FilterChips label="policy" chips={policyChips} active={policyFilter} onSelect={(k) => router.setQuery({ policy: k })} />
    {/if}
  </FilterBar>
  <div class="list">
    <div class="header">
      <div></div>
      <div>policy</div>
      <div>checkpoint</div>
      <div>eval run</div>
      <div>updated</div>
    </div>
    {#if isLoading}
      {#each Array(4) as _, i (i)}
        <div class="erow">
          <div class="skel" style="width: 6px; height: 6px"></div>
          <div class="skel" style="height: 14px; width: 50%"></div>
          <div class="skel" style="height: 12px; width: 80px"></div>
          <div class="skel" style="height: 12px; width: 80px"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
        </div>
      {/each}
    {:else if filtered.length === 0}
      <div class="empty">
        <p class="title">{allEvals.length === 0 ? "No evals queued" : "No evals match"}</p>
        <p class="sub">
          {allEvals.length === 0
            ? "Eval requests appear when a policy fires against a checkpoint."
            : "Clear the filter chips above."}
        </p>
      </div>
    {:else}
      {#each filtered as e}
        {@const eg = stateToGroup(e.state)}
        <div class="erow">
          <Pill status={eg} showLabel={false} />
          <span class="policy mono">{e.policy_id}</span>
          <button
            type="button"
            class="link"
            onclick={() => router.go("artifacts", e.checkpoint_artifact_id)}
          >
            <Hash value={e.checkpoint_artifact_id} n={10} />
          </button>
          {#if e.eval_run_id}
            <button
              type="button"
              class="link"
              onclick={() => router.go("runs", e.eval_run_id!)}
            >
              <Hash value={e.eval_run_id} n={10} />
            </button>
          {:else}
            <span class="muted">—</span>
          {/if}
          {#if e.updated_at}
            <RelativeTime ts={e.updated_at} />
          {:else}
            <span class="muted">—</span>
          {/if}
        </div>
      {/each}
    {/if}
  </div>
</div>

<script lang="ts" module>
  function stateToGroup(s: string): string {
    if (s === "succeeded") return "succeeded";
    if (s === "failed") return "failed";
    if (s === "running" || s === "submitted") return s;
    return "pending";
  }
</script>

<style>
  .page { display: flex; flex-direction: column; height: 100%; overflow: hidden; }
  .list { flex: 1; overflow-y: auto; }
  .header {
    display: grid;
    grid-template-columns: 22px 1fr 110px 110px 90px;
    column-gap: 14px;
    padding: 6px 16px;
    font-family: theme("fontFamily.mono");
    font-size: 10px;
    color: theme("colors.fg.3");
    letter-spacing: 0.06em;
    text-transform: uppercase;
    border-bottom: 1px solid theme("colors.line.0");
    background: theme("colors.bg.0");
    position: sticky;
    top: 0;
    z-index: 1;
  }
  .erow {
    display: grid;
    grid-template-columns: 22px 1fr 110px 110px 90px;
    column-gap: 14px;
    align-items: center;
    padding: 8px 16px;
    border-bottom: 1px solid theme("colors.line.0");
    font-size: 13px;
  }
  .erow:hover { background: theme("colors.bg.2"); }
  .policy { font-size: 13px; color: theme("colors.fg.0"); }
  .link {
    background: transparent;
    border: none;
    cursor: pointer;
    color: theme("colors.fg.1");
    font-size: 11px;
    padding: 0;
  }
  .link:hover { color: theme("colors.fg.0"); }
  .muted { color: theme("colors.fg.3"); font-size: 11px; }
  .empty { padding: 80px 24px; text-align: center; }
  .empty .title { font-size: 14px; color: theme("colors.fg.0"); margin: 0 0 6px 0; }
  .empty .sub { font-size: 13px; color: theme("colors.fg.2"); margin: 0; }
</style>
