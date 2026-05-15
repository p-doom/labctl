<script lang="ts">
  import { onMount } from "svelte";
  import { store, loadRuns, loadRecipeHistory, loadRunDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { compareSelection } from "../lib/compare.svelte";
  import Sparkline from "../components/Sparkline.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterChips from "../components/FilterChips.svelte";
  import FilterInput from "../components/FilterInput.svelte";
  import Icon from "../components/Icon.svelte";
  import EmptyState from "../components/EmptyState.svelte";
  import { nowSecs } from "../lib/time.svelte";
  import type { ChipDef } from "../lib/filters";
  import {
    statusGroup,
    shortHash,
    shortId,
    formatDuration,
    formatRelative,
    formatAbsolute,
    liveDuration,
    copy,
  } from "../lib/format";
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
  let filterText = $state("");
  let textQuery = $derived(filterText.trim().toLowerCase());

  // Precomputed lowercase haystack per run. Built once when the list
  // changes (cheap — ~1ms for 1000 runs); reused on every keystroke so
  // the per-keystroke filter is one `.includes()` per row, not five
  // `.toLowerCase().includes()` calls.
  let haystacks = $derived.by(() => {
    const m = new Map<string, string>();
    for (const r of allRuns) {
      m.set(
        r.id,
        `${r.recipe_name}\n${r.id}\n${r.stage_name ?? ""}\n${r.repo ?? ""}\n${r.submitted_by ?? ""}`.toLowerCase(),
      );
    }
    return m;
  });

  let filtered = $derived.by(() => {
    const q = textQuery;
    const useText = q.length > 0;
    const useStatus = statusFilter != null;
    const usePipe = pipelineFilter != null;
    const useRepo = repoFilter != null;
    const useUser = userFilter != null;
    if (!useText && !useStatus && !usePipe && !useRepo && !useUser) return allRuns;
    return allRuns.filter((r) => {
      if (useStatus && statusGroup(r.status) !== statusFilter) return false;
      if (usePipe && r.pipeline_id !== pipelineFilter) return false;
      if (useRepo && r.repo !== repoFilter) return false;
      if (useUser && (r.submitted_by ?? "") !== userFilter) return false;
      if (useText && !haystacks.get(r.id)!.includes(q)) return false;
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

  // Kick off recipe-history loads as new recipes appear in view. We
  // iterate only the *visible* slice — virtualization already caps that
  // to ~30 rows, so this stays O(viewport), not O(filtered). Important:
  // this is the only $effect on the keystroke critical path, and it
  // used to walk thousands of rows per keystroke.
  $effect(() => {
    const seen = new Set<string>();
    for (const r of visibleRows) {
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

  let filterInputEl = $state<HTMLInputElement | null>(null);
  let listEl = $state<HTMLDivElement | null>(null);

  // -------- virtualization --------
  // The list can hold thousands of rows. Mounting every row's Pill /
  // Sparkline / Hash / Duration / RelativeTime on every filter change was
  // the actual source of keystroke lag. We render only ~viewport-height
  // worth of rows + a small overscan; spacer divs above and below keep
  // the scroll height truthful so the scrollbar still represents the full
  // dataset and scroll-position math stays linear.
  const ROW_HEIGHT = 37;
  const HEADER_HEIGHT = 30;
  const OVERSCAN = 8;

  let scrollTop = $state(0);
  let viewportHeight = $state(800);

  function onListScroll(e: UIEvent) {
    scrollTop = (e.currentTarget as HTMLDivElement).scrollTop;
  }

  let firstIdx = $derived(
    Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN),
  );
  let lastIdx = $derived(
    Math.min(
      filtered.length,
      Math.ceil((scrollTop + viewportHeight - HEADER_HEIGHT) / ROW_HEIGHT) + OVERSCAN,
    ),
  );
  let topPad = $derived(firstIdx * ROW_HEIGHT);
  let bottomPad = $derived(Math.max(0, (filtered.length - lastIdx) * ROW_HEIGHT));
  let visibleRows = $derived(filtered.slice(firstIdx, lastIdx));

  // Keyboard nav (j/k/Enter when no panel is open and no input is focused).
  let cursor = $state(0);
  // Reset cursor and scroll position when the filter set changes — keeps
  // the user grounded at the top of the new result set.
  $effect(() => {
    void statusFilter;
    void repoFilter;
    void pipelineFilter;
    void userFilter;
    void textQuery;
    cursor = 0;
    scrollTop = 0;
    if (listEl) listEl.scrollTop = 0;
  });
  $effect(() => {
    if (cursor >= filtered.length) cursor = Math.max(0, filtered.length - 1);
  });

  function openCursorRow() {
    const r = filtered[cursor];
    if (r) open(r);
  }

  function onKey(e: KeyboardEvent) {
    if (router.view !== "runs") return;
    // Cmd/Ctrl-K and "/" focus the inline filter — same shortcut, one
    // consistent behavior. Works even from inside other inputs, since the
    // intent is "I want to filter."
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      filterInputEl?.focus();
      filterInputEl?.select();
      return;
    }
    const target = e.target as HTMLElement | null;
    const inField =
      target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA");
    if (e.key === "/" && !inField) {
      e.preventDefault();
      filterInputEl?.focus();
      return;
    }
    if (inField) return;
    if (e.key === "j" || e.key === "ArrowDown") {
      e.preventDefault();
      cursor = Math.min(cursor + 1, filtered.length - 1);
      if (router.selected) { const r = filtered[cursor]; if (r) open(r); }
    } else if (e.key === "k" || e.key === "ArrowUp") {
      e.preventDefault();
      cursor = Math.max(cursor - 1, 0);
      if (router.selected) { const r = filtered[cursor]; if (r) open(r); }
    } else if (e.key === "Enter") {
      openCursorRow();
    }
  }

  $effect(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  // Cursor scrollIntoView — compute target scrollTop directly because the
  // cursor row may not be in the DOM when virtualized.
  $effect(() => {
    const i = cursor;
    if (!listEl || filtered.length === 0) return;
    const rowY = i * ROW_HEIGHT; // row position in row-space (header excluded)
    const visTop = scrollTop;
    const visBottom = scrollTop + viewportHeight - HEADER_HEIGHT;
    if (rowY < visTop) {
      listEl.scrollTop = rowY;
    } else if (rowY + ROW_HEIGHT > visBottom) {
      listEl.scrollTop = rowY + ROW_HEIGHT - viewportHeight + HEADER_HEIGHT;
    }
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
    <FilterInput
      bind:inputRef={filterInputEl}
      value={filterText}
      placeholder="Filter runs…"
      onInput={(v) => (filterText = v)}
      onEnter={openCursorRow}
    />
  </FilterBar>

  <div
    class="list"
    bind:this={listEl}
    bind:clientHeight={viewportHeight}
    onscroll={onListScroll}
  >
    <div class="list-head run-head">
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
        <div class="list-row run-row">
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
      <EmptyState title={allRuns.length === 0 ? "No runs yet" : "No runs match these filters"}>
        {#snippet sub()}
          {allRuns.length === 0
            ? "Submit a recipe with labctl run, and it shows up here."
            : "Clear the filter chips above to see the rest."}
        {/snippet}
      </EmptyState>
    {:else}
      {#if topPad > 0}<div class="spacer" style={`height: ${topPad}px`} aria-hidden="true"></div>{/if}
      {#each visibleRows as r, j (r.id)}
        {@const i = firstIdx + j}
        {@const hist = store.recipeHistory(r.recipe_name)}
        {@const inCompare = compareSelection.has(r.id)}
        {@const group = statusGroup(r.status)}
        {@const pulse = group === "running" || r.status === "submitted"}
        {@const dur = liveDuration(r, nowSecs.value)}
        <div
          class="list-row run-row"
          class:any-selected={compareSelection.size > 0}
          data-state={
            inCompare ? "compared" :
            router.selected === r.id ? "active" :
            (cursor === i && router.selected !== r.id) ? "cursor" :
            undefined
          }
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
          <!-- Pill (dot only): inlined to skip per-row component setup. -->
          <span class="dot" data-group={group} class:animate-pulse-dot={pulse} aria-label={r.status}></span>
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
          <!-- Hash (id): inlined. Drops the copy-badge animation in
               exchange for ~30× fewer Svelte component setups. -->
          <button
            type="button"
            class="hash"
            onclick={(e) => { e.stopPropagation(); copy(r.id); }}
            title={r.id}
            aria-label={`Copy run id ${r.id}`}
          >{shortId(r.id, 12)}</button>
          <div class="hist">
            {#if hist}
              <Sparkline history={hist.history} />
            {:else}
              <div class="skel" style="height: 14px; width: 80px; opacity: 0.5"></div>
            {/if}
          </div>
          <!-- Duration: inlined. -->
          <span class="dur mono" class:live={!r.is_terminal}>{formatDuration(dur)}</span>
          <!-- RelativeTime: inlined. -->
          <span class="rel mono" title={formatAbsolute(r.created_at)}>{formatRelative(r.created_at, nowSecs.value)}</span>
        </div>
      {/each}
      {#if bottomPad > 0}<div class="spacer" style={`height: ${bottomPad}px`} aria-hidden="true"></div>{/if}
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
  .spacer {
    /* No content, no border — purely a layout placeholder so the scroll
     * track size reflects the full filtered list while only a viewport's
     * worth of rows is actually in the DOM. */
    width: 100%;
  }
  /* Runs view geometry: check | dot | recipe | id | sparkline | duration | age */
  .run-head,
  .run-row {
    grid-template-columns: 18px 22px 1fr 140px 110px 80px 80px;
  }
  .check {
    width: 16px;
    height: 16px;
    border-radius: 4px;
    border: 1px solid var(--line-2);
    background: transparent;
    padding: 0;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    color: var(--bg-0);
    /* Hidden by default; appears on row hover OR when *any* row is
     * selected (so the user knows which rows are in the comparison
     * without having to hover each one), AND stays visible on the row
     * that's actually checked. */
    opacity: 0;
    transition: opacity var(--dur-micro) var(--ease);
  }
  .list-row:hover .check,
  .run-row.any-selected .check,
  .check.checked,
  .check:focus-visible {
    opacity: 1;
  }
  .check:hover {
    border-color: var(--fg-2);
    background: var(--bg-3);
  }
  .check.checked {
    background: var(--accent);
    border-color: var(--accent);
    color: var(--bg-0);
  }
  .check.checked:hover {
    background: var(--accent-dim);
    border-color: var(--accent-dim);
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
    color: var(--fg-0);
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
  .recipe .name:hover { color: var(--accent-dim); }
  .recipe .stage {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: var(--fg-1);
    flex-shrink: 0;
  }
  .recipe .repo {
    font-size: 11px;
    color: var(--fg-2);
    margin-left: auto;
    flex-shrink: 0;
    padding: 1px 5px;
    border: 1px solid var(--line-1);
    border-radius: 3px;
  }
  .recipe .user {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: var(--fg-1);
    margin-left: auto;
    flex-shrink: 0;
  }
  .recipe .user + .repo { margin-left: 6px; }
  .hist { display: flex; align-items: center; }

  /* Inlined Pill dot. */
  .dot {
    width: 6px;
    height: 6px;
    border-radius: 999px;
    flex-shrink: 0;
    justify-self: center;
  }
  .dot[data-group="running"]   { --dot: var(--status-running);   background: var(--dot); }
  .dot[data-group="succeeded"] { --dot: var(--status-succeeded); background: var(--dot); }
  .dot[data-group="failed"]    { --dot: var(--status-failed);    background: var(--dot); }
  .dot[data-group="pending"]   { --dot: var(--status-pending);   background: var(--dot); }
  .dot[data-group="neutral"]   { --dot: var(--status-neutral);   background: var(--dot); }

  /* Inlined Hash. No copied-badge — title attr does the job. */
  .hash {
    background: transparent;
    border: none;
    padding: 0;
    cursor: pointer;
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: var(--fg-1);
    text-align: left;
    letter-spacing: 0.01em;
    transition: color var(--dur-micro) var(--ease);
  }
  .hash:hover { color: var(--fg-0); }

  /* Inlined Duration. */
  .dur {
    font-size: 12px;
    color: var(--fg-1);
    font-variant-numeric: tabular-nums;
  }
  .dur.live { color: var(--fg-0); }

  /* Inlined RelativeTime. */
  .rel {
    font-size: 12px;
    color: var(--fg-1);
    font-variant-numeric: tabular-nums;
  }
</style>
