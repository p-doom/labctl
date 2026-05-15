<script lang="ts">
  // Policies list. Top-level: one row per policy with the primary-metric
  // sparkline overlaying the most recent training runs, plus aggregate
  // counts and the last-fired timestamp.

  import { onMount } from "svelte";
  import { store, loadPolicies } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import PolicyTrend from "../components/PolicyTrend.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterInput from "../components/FilterInput.svelte";
  import EmptyState from "../components/EmptyState.svelte";
  import { seriesColor as colorFor } from "../lib/chart-colors";

  onMount(() => loadPolicies());

  let policies = $derived(store.policies.data ?? []);
  let filterText = $state("");
  let textQuery = $derived(filterText.trim().toLowerCase());
  let cursor = $state(0);
  let listEl = $state<HTMLDivElement | null>(null);
  let filterInputEl = $state<HTMLInputElement | null>(null);
  $effect(() => { void textQuery; cursor = 0; });
  $effect(() => { if (cursor >= sorted.length) cursor = Math.max(0, sorted.length - 1); });
  function openCursorRow() {
    const p = sorted[cursor];
    if (p) router.go("policies", p.name);
  }
  function onKey(e: KeyboardEvent) {
    if (router.view !== "policies" || router.selected) return;
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      filterInputEl?.focus();
      filterInputEl?.select();
      return;
    }
    const t = e.target as HTMLElement | null;
    const inField = t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA");
    if (e.key === "/" && !inField) {
      e.preventDefault();
      filterInputEl?.focus();
      return;
    }
    if (inField) return;
    if (e.key === "j" || e.key === "ArrowDown") { e.preventDefault(); cursor = Math.min(cursor + 1, sorted.length - 1); }
    else if (e.key === "k" || e.key === "ArrowUp") { e.preventDefault(); cursor = Math.max(cursor - 1, 0); }
    else if (e.key === "Enter") { openCursorRow(); }
  }
  $effect(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });
  $effect(() => {
    const i = cursor;
    if (!listEl) return;
    requestAnimationFrame(() => {
      (listEl!.querySelectorAll<HTMLElement>('.list-row'))[i]?.scrollIntoView({ block: "nearest" });
    });
  });
  let isLoading = $derived(store.policies.loading && policies.length === 0);

  let haystacks = $derived.by(() => {
    const m = new Map<string, string>();
    for (const p of policies) m.set(p.name, `${p.name}\n${p.primary_metric ?? ""}`.toLowerCase());
    return m;
  });

  // Sort: failing policies first (drag regressions to the top), then by
  // last_fired_at desc. Matches the Linear "things that need your
  // attention" instinct.
  let sorted = $derived.by(() => {
    const q = textQuery;
    const filtered = q
      ? policies.filter((p) => haystacks.get(p.name)!.includes(q))
      : policies;
    return filtered.slice().sort((a, b) => {
      if (a.failed_count !== b.failed_count) return b.failed_count - a.failed_count;
      return b.last_fired_at - a.last_fired_at;
    });
  });

  function fmtValue(v: number | null | undefined): string {
    if (v == null) return "—";
    if (Math.abs(v) >= 100) return v.toFixed(1);
    if (Math.abs(v) >= 1) return v.toFixed(3);
    return v.toFixed(4);
  }

  function bestLatest(runs: { latest_value: number | null }[]): number | null {
    let best: number | null = null;
    for (const r of runs) {
      if (r.latest_value == null) continue;
      if (best == null || r.latest_value > best) best = r.latest_value;
    }
    return best;
  }
</script>

<div class="page">
  <FilterBar>
    <FilterInput
      bind:inputRef={filterInputEl}
      value={filterText}
      placeholder="Filter policies…"
      onInput={(v) => (filterText = v)}
      onEnter={openCursorRow}
    />
  </FilterBar>
  <div class="list" bind:this={listEl}>
    <div class="list-head policy-head">
      <div>policy</div>
      <div>metric</div>
      <div>trend</div>
      <div>best</div>
      <div>activity</div>
      <div>last fired</div>
    </div>
    {#if isLoading}
      {#each Array(4) as _, i (i)}
        <div class="list-row policy-row">
          <div class="skel" style="height: 14px; width: 50%"></div>
          <div class="skel" style="height: 12px; width: 80px"></div>
          <div class="skel" style="height: 18px; width: 120px"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
          <div class="skel" style="height: 12px; width: 80px"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
        </div>
      {/each}
    {:else if policies.length === 0}
      <EmptyState title="No policies yet">
        {#snippet sub()}
          Drop an <code>EvalPolicy</code> TOML file in your dispatcher's
          <code>policies_dir</code> and the agent will fire eval runs as
          new checkpoints land. Policies appear here once they have at
          least one eval request.
        {/snippet}
      </EmptyState>
    {:else}
      {#each sorted as p, i (p.name)}
        <button
          type="button"
          class="list-row policy-row"
          data-state={cursor === i ? "cursor" : undefined}
          onclick={() => { cursor = i; router.go("policies", p.name); }}
        >
          <div class="policy">
            <span class="name mono">{p.name}</span>
            {#if p.failed_count > 0}
              <span class="badge fail" title="{p.failed_count} failed eval request{p.failed_count === 1 ? '' : 's'}">
                {p.failed_count} failed
              </span>
            {/if}
            {#if p.running_count > 0}
              <span class="badge run" title="{p.running_count} in flight">
                {p.running_count} running
              </span>
            {/if}
          </div>
          <span class="metric mono">{p.primary_metric ?? "—"}</span>
          <div class="trend">
            {#if p.series && p.series.runs.length > 0}
              <PolicyTrend runs={p.series.runs} {colorFor} />
            {:else}
              <span class="dim">—</span>
            {/if}
          </div>
          <div class="latest mono">
            {p.series ? fmtValue(bestLatest(p.series.runs)) : "—"}
          </div>
          <div class="activity mono">
            {p.total_count} eval{p.total_count === 1 ? "" : "s"}
          </div>
          <div class="when">
            {#if p.last_fired_at}
              <RelativeTime ts={p.last_fired_at} />
            {:else}
              <span class="dim">—</span>
            {/if}
          </div>
        </button>
      {/each}
    {/if}
  </div>
</div>

<style>
  .page { display: flex; flex-direction: column; height: 100%; overflow: hidden; }
  .list { flex: 1; overflow-y: auto; }
  .policy-head,
  .policy-row {
    grid-template-columns: minmax(0, 1.5fr) 140px 180px 80px 110px 96px;
  }
  .policy-row { padding: 10px var(--row-pad-x); }
  .policy {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    overflow: hidden;
    min-width: 0;
  }
  .name {
    font-size: 13px;
    color: var(--fg-0);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }
  .badge {
    display: inline-block;
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    padding: 1px 6px;
    border-radius: 999px;
  }
  .badge.fail {
    background: var(--status-failed-soft);
    color: var(--status-failed-fg);
  }
  .badge.run {
    background: var(--status-running-soft);
    color: var(--status-running-fg);
  }
  .metric { font-size: 11px; color: var(--fg-2); }
  .trend { min-width: 0; }
  .latest { font-size: 12px; color: var(--fg-0); font-variant-numeric: tabular-nums; }
  .activity { font-size: 11px; color: var(--fg-2); }
  .when { font-size: 11px; color: var(--fg-2); }
</style>
