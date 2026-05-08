<script lang="ts">
  import { onMount } from "svelte";
  import { store, loadRuns, loadRecipeHistory, loadRunDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { compareSelection } from "../lib/compare.svelte";
  import Pill from "../components/Pill.svelte";
  import Sparkline from "../components/Sparkline.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Duration from "../components/Duration.svelte";
  import Hash from "../components/Hash.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterChips from "../components/FilterChips.svelte";
  import Icon from "../components/Icon.svelte";
  import type { ChipDef } from "../lib/filters";
  import { statusGroup } from "../lib/format";
  import type { RunSummary } from "../lib/types";

  onMount(() => {
    loadRuns();
  });

  let statusFilter = $derived(router.query.get("status"));
  let pipelineFilter = $derived(router.query.get("pipeline"));
  let repoFilter = $derived(router.query.get("repo"));
  let userFilter = $derived(router.query.get("user"));

  let allRuns = $derived(store.runs.data ?? []);
  let isLoading = $derived(store.runs.loading && allRuns.length === 0);
  let filtered = $derived.by(() => {
    return allRuns.filter((r) => {
      if (statusFilter && statusGroup(r.status) !== statusFilter) return false;
      if (pipelineFilter && r.pipeline_id !== pipelineFilter) return false;
      if (repoFilter && r.repo !== repoFilter) return false;
      if (userFilter && (r.submitted_by ?? "") !== userFilter) return false;
      return true;
    });
  });

  let counts = $derived.by(() => {
    const c = { running: 0, succeeded: 0, failed: 0, pending: 0, neutral: 0 };
    for (const r of allRuns) c[statusGroup(r.status)]++;
    return c;
  });

  let repos = $derived.by(() => {
    const set = new Set<string>();
    for (const r of allRuns) set.add(r.repo);
    return [...set].sort();
  });

  let users = $derived.by(() => {
    const set = new Set<string>();
    for (const r of allRuns) {
      if (r.submitted_by) set.add(r.submitted_by);
    }
    return [...set].sort();
  });

  let statusChips = $derived<ChipDef[]>([
    { key: null, label: "All", count: allRuns.length, always: true },
    { key: "running", label: "Running", count: counts.running, dot: "running" },
    { key: "failed", label: "Failed", count: counts.failed, dot: "failed" },
    { key: "succeeded", label: "Succeeded", count: counts.succeeded, dot: "succeeded" },
    { key: "pending", label: "Pending", count: counts.pending, dot: "pending" },
  ]);
  let repoChips = $derived<ChipDef[]>(
    repos.map((r) => ({
      key: r,
      label: r,
      count: allRuns.filter((run) => run.repo === r).length,
    })),
  );
  let userChips = $derived<ChipDef[]>(
    users.map((u) => ({
      key: u,
      label: u,
      count: allRuns.filter((run) => run.submitted_by === u).length,
    })),
  );

  function open(r: RunSummary) {
    router.select("runs", r.id);
  }

  // Kick off recipe-history loads as new recipes appear in view.
  $effect(() => {
    const seen = new Set<string>();
    for (const r of filtered) {
      if (seen.has(r.recipe_name)) continue;
      seen.add(r.recipe_name);
      if (!store.recipeHistory(r.recipe_name)) {
        loadRecipeHistory(r.recipe_name).catch(() => {});
      }
    }
  });

  // Hover prefetch — by the time the click lands the side panel renders
  // from cache. 100ms threshold keeps drive-by scrolls from hammering the
  // server.
  let prefetchTimer: number | null = null;
  let prefetchTarget: string | null = null;
  function onRowEnter(r: RunSummary) {
    prefetchTarget = r.id;
    if (prefetchTimer) clearTimeout(prefetchTimer);
    prefetchTimer = window.setTimeout(() => {
      if (prefetchTarget === r.id) loadRunDetail(r.id).catch(() => {});
    }, 100);
  }
  function onRowLeave() {
    if (prefetchTimer) clearTimeout(prefetchTimer);
    prefetchTimer = null;
    prefetchTarget = null;
  }

  function setFilter(key: string, value: string | null) {
    router.setQuery({ [key]: value });
  }

  // Keyboard nav (j/k/Enter when no panel is open and no input is focused).
  let cursor = $state(0);
  // Reset cursor when the filter set changes (status/repo/pipeline) so j/k
  // don't land mid-list on the new view.
  $effect(() => {
    void statusFilter;
    void repoFilter;
    void pipelineFilter;
    void userFilter;
    cursor = 0;
  });
  $effect(() => {
    if (cursor >= filtered.length) cursor = Math.max(0, filtered.length - 1);
  });

  function onKey(e: KeyboardEvent) {
    if (router.selected) return;
    const target = e.target as HTMLElement | null;
    if (target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA")) return;
    if (e.key === "j") {
      e.preventDefault();
      cursor = Math.min(cursor + 1, filtered.length - 1);
    } else if (e.key === "k") {
      e.preventDefault();
      cursor = Math.max(cursor - 1, 0);
    } else if (e.key === "Enter") {
      const r = filtered[cursor];
      if (r) open(r);
    }
  }

  $effect(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });
</script>

<div class="page">
  <FilterBar>
    <FilterChips chips={statusChips} active={statusFilter} onSelect={(k) => setFilter("status", k)} />
    {#if repos.length > 1}
      <FilterChips label="repo" chips={repoChips} active={repoFilter} onSelect={(k) => setFilter("repo", k)} />
    {/if}
    {#if users.length > 1}
      <FilterChips label="user" chips={userChips} active={userFilter} onSelect={(k) => setFilter("user", k)} />
    {/if}
  </FilterBar>

  <div class="list">
    <div class="header">
      <div></div>
      <div></div>
      <div>recipe</div>
      <div>id</div>
      <div>history</div>
      <div>duration</div>
      <div>age</div>
    </div>

    {#if isLoading}
      {#each Array(8) as _, i (i)}
        <div class="row">
          <div></div>
          <div class="skel" style="width: 6px; height: 6px; border-radius: 3px"></div>
          <div class="skel" style="height: 14px; width: 60%"></div>
          <div class="skel" style="height: 12px; width: 100px"></div>
          <div class="skel" style="height: 14px; width: 80px"></div>
          <div class="skel" style="height: 12px; width: 40px"></div>
          <div class="skel" style="height: 12px; width: 50px"></div>
        </div>
      {/each}
    {:else if filtered.length === 0}
      <div class="empty">
        <p class="title">{allRuns.length === 0 ? "No runs yet" : "No runs match these filters"}</p>
        <p class="sub">
          {allRuns.length === 0
            ? "Submit a recipe with labctl run, and it shows up here."
            : "Clear the filter chips above to see the rest."}
        </p>
      </div>
    {:else}
      {#each filtered as r, i (r.id)}
        {@const hist = store.recipeHistory(r.recipe_name)}
        {@const inCompare = compareSelection.has(r.id)}
        <div
          class="row"
          class:is-active={router.selected === r.id}
          class:is-cursor={cursor === i && router.selected !== r.id}
          class:in-compare={inCompare}
          class:any-selected={compareSelection.size > 0}
          onclick={(e) => {
            // Shift-click is the power-user shortcut. The visible
            // checkbox in the leftmost column is the discoverable path.
            if (e.shiftKey) {
              e.preventDefault();
              compareSelection.toggle(r.id);
              return;
            }
            cursor = i;
            open(r);
          }}
          onkeydown={(e) => e.key === "Enter" && open(r)}
          onmouseenter={() => onRowEnter(r)}
          onmouseleave={onRowLeave}
          role="button"
          tabindex="0"
        >
          <button
            type="button"
            class="check"
            class:checked={inCompare}
            onclick={(e) => {
              e.stopPropagation();
              compareSelection.toggle(r.id);
            }}
            aria-label={inCompare ? "Remove from comparison" : "Add to comparison"}
            aria-pressed={inCompare}
            title={inCompare ? "Remove from comparison" : "Add to comparison"}
          >
            {#if inCompare}
              <Icon name="check" size={11} />
            {/if}
          </button>
          <Pill status={r.status} showLabel={false} />
          <div class="recipe">
            <button
              type="button"
              class="name"
              title={`Open all runs of ${r.recipe_name}`}
              onclick={(e) => {
                e.stopPropagation();
                router.go("recipes", r.recipe_name);
              }}
            >{r.recipe_name}</button>
            {#if r.stage_name}
              <span class="stage" title="pipeline stage">/ {r.stage_name}</span>
            {/if}
            {#if r.submitted_by && users.length > 1 && !userFilter}
              <span class="user" title={`Submitted by ${r.submitted_by}`}>{r.submitted_by}</span>
            {/if}
            {#if r.repo && repos.length > 1 && !repoFilter}
              <span class="repo">{r.repo}</span>
            {/if}
          </div>
          <div class="id">
            <Hash value={r.id} n={12} />
          </div>
          <div class="hist">
            {#if hist}
              <Sparkline history={hist.history} />
            {:else}
              <div class="skel" style="height: 14px; width: 80px; opacity: 0.5"></div>
            {/if}
          </div>
          <Duration run={r} />
          <RelativeTime ts={r.created_at} />
        </div>
      {/each}
    {/if}
  </div>
</div>

<style>
  .page {
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: hidden;
  }
  .list {
    flex: 1;
    overflow-y: auto;
  }
  .header {
    display: grid;
    grid-template-columns: 18px 22px 1fr 140px 110px 80px 80px;
    column-gap: 12px;
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
  .row {
    grid-template-columns: 18px 22px 1fr 140px 110px 80px 80px;
    cursor: pointer;
    position: relative;
  }
  .check {
    width: 16px;
    height: 16px;
    border-radius: 4px;
    border: 1px solid theme("colors.line.2");
    background: transparent;
    padding: 0;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    color: theme("colors.bg.0");
    /* Hidden by default; appears on row hover OR when *any* row is
     * selected (so the user knows which rows are in the comparison without
     * having to hover each one), AND stays visible on the row that's
     * actually checked. */
    opacity: 0;
  }
  .row:hover .check,
  .row.any-selected .check,
  .check.checked,
  .check:focus-visible {
    opacity: 1;
  }
  .check:hover {
    border-color: theme("colors.fg.2");
    background: theme("colors.bg.3");
  }
  .check.checked {
    background: theme("colors.accent.DEFAULT");
    border-color: theme("colors.accent.DEFAULT");
    color: theme("colors.bg.0");
  }
  .check.checked:hover {
    background: theme("colors.accent.dim");
    border-color: theme("colors.accent.dim");
  }
  .row.is-cursor::before {
    content: "";
    position: absolute;
    left: 0;
    top: 4px;
    bottom: 4px;
    width: 2px;
    background: theme("colors.fg.3");
    border-radius: 0 1px 1px 0;
  }
  .row.is-active::before {
    content: "";
    position: absolute;
    left: 0;
    top: 4px;
    bottom: 4px;
    width: 2px;
    background: theme("colors.accent.DEFAULT");
    border-radius: 0 1px 1px 0;
  }
  .row.is-active {
    background: theme("colors.bg.2");
  }
  .row.in-compare {
    background: theme("colors.accent.soft");
    box-shadow: inset 2px 0 0 theme("colors.accent.DEFAULT");
  }
  /* Subtle bump on hover that still works in either theme without a
   * hardcoded accent rgba — relies on the existing soft token + a
   * slightly darker overlay from the underlying surface. */
  .row.in-compare:hover {
    background: theme("colors.bg.2");
    box-shadow: inset 2px 0 0 theme("colors.accent.DEFAULT");
  }
  .recipe {
    display: flex;
    align-items: baseline;
    gap: 8px;
    overflow: hidden;
  }
  .recipe .name {
    font-family: theme("fontFamily.mono");
    font-size: 13px;
    color: theme("colors.fg.0");
    letter-spacing: -0.005em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    background: transparent;
    border: none;
    padding: 0;
    cursor: pointer;
    text-align: left;
  }
  .recipe .name:hover {
    color: theme("colors.accent.dim");
  }
  .recipe .stage {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.fg.2");
    flex-shrink: 0;
  }
  .recipe .repo {
    font-size: 11px;
    color: theme("colors.fg.3");
    margin-left: auto;
    flex-shrink: 0;
    padding: 1px 5px;
    border: 1px solid theme("colors.line.1");
    border-radius: 3px;
  }
  /* The user badge sits to the right of the recipe name. When the repo
   * badge is also present (multi-repo deployments), only the first
   * margin-left:auto sibling pushes to the far right; the user badge
   * sits adjacent to it without competing for the auto-margin. */
  .recipe .user {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.fg.2");
    margin-left: auto;
    flex-shrink: 0;
  }
  /* When both badges are present, the repo gets the auto-margin and the
   * user just sits next to it. */
  .recipe .user + .repo {
    margin-left: 6px;
  }
  .id { font-family: theme("fontFamily.mono"); }
  .hist { display: flex; align-items: center; }

  .empty {
    padding: 80px 24px;
    text-align: center;
    animation: fadeIn 250ms cubic-bezier(0.2, 0, 0, 1);
  }
  .empty .title {
    font-size: 14px;
    color: theme("colors.fg.0");
    margin: 0 0 6px 0;
  }
  .empty .sub {
    font-size: 13px;
    color: theme("colors.fg.2");
    margin: 0;
  }
  @keyframes fadeIn {
    from { opacity: 0; }
    to { opacity: 1; }
  }
</style>
