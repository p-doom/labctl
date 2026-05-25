<script lang="ts">
  import { onMount, untrack } from "svelte";
  import { store, loadRuns, loadRunDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { compareSelection } from "../lib/compare.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterChips from "../components/FilterChips.svelte";
  import FilterInput from "../components/FilterInput.svelte";
  import Icon from "../components/Icon.svelte";
  import EmptyState from "../components/EmptyState.svelte";
  import { nowSecs } from "../lib/time.svelte";
  import type { ChipDef } from "../lib/filters";
  import {
    statusGroup,
    shortStatus,
    editionNumber,
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
  // changes; reused on every keystroke.
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

  // Surface a short status label next to the headline for terminal non-
  // success statuses where the dot color alone is ambiguous (timeout,
  // oom, cancelled, etc.). For running/succeeded/pending the dot speaks
  // for itself.
  function showStatusLabel(s: string): boolean {
    return (
      s === "failed" ||
      s === "cancelled" ||
      s === "timeout" ||
      s === "oom" ||
      s === "unknown_terminal"
    );
  }

  // Hover prefetch — by the time the click lands the side panel renders
  // from cache.
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
  // Stanza rows are 56px (two-line: italic-Lora headline + mono meta).
  // Fewer rows visible than the old 37px Linear-school density, but the
  // typographic hierarchy buys back the scan speed in a different
  // dimension: the eye reads a column of italic recipe names cleanly.
  const ROW_HEIGHT = 56;
  const HEADER_HEIGHT = 34;
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

  let cursor = $state(0);
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

  $effect(() => {
    const i = cursor;
    untrack(() => {
      if (!listEl || filtered.length === 0) return;
      const rowY = i * ROW_HEIGHT;
      const visTop = scrollTop;
      const visBottom = scrollTop + viewportHeight - HEADER_HEIGHT;
      if (rowY < visTop) {
        listEl.scrollTop = rowY;
      } else if (rowY + ROW_HEIGHT > visBottom) {
        listEl.scrollTop = rowY + ROW_HEIGHT - viewportHeight + HEADER_HEIGHT;
      }
    });
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
      placeholder="Filter editions…"
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
      <div>Edition</div>
      <div>Recipe</div>
      <div></div>
      <div>Duration</div>
      <div>Logged</div>
      <div></div>
    </div>

    {#if isLoading}
      {#each Array(8) as _, i (i)}
        <div class="list-row run-row">
          <div></div>
          <div class="skel" style="height: 11px; width: 50px"></div>
          <div class="rec-cell">
            <div class="skel" style="height: 14px; width: 60%; margin-bottom: 4px"></div>
            <div class="skel" style="height: 11px; width: 40%"></div>
          </div>
          <div class="skel" style="width: 8px; height: 8px; border-radius: 4px"></div>
          <div class="skel" style="height: 11px; width: 50px"></div>
          <div class="skel" style="height: 11px; width: 60px"></div>
          <div></div>
        </div>
      {/each}
    {:else if filtered.length === 0}
      <EmptyState title={allRuns.length === 0 ? "Nothing to record." : "No editions match these filters."}>
        {#snippet sub()}
          {#if allRuns.length === 0}
            Submit a recipe with <code>labctl run</code>. Editions will appear here.
          {:else}
            Clear the filter chips above to widen the catalogue.
          {/if}
        {/snippet}
      </EmptyState>
    {:else}
      {#if topPad > 0}<div class="spacer" style={`height: ${topPad}px`} aria-hidden="true"></div>{/if}
      {#each visibleRows as r, j (r.id)}
        {@const i = firstIdx + j}
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

          <!-- Edition number — masthead micro-caps, click to copy id. -->
          <button
            type="button"
            class="edno"
            onclick={(e) => { e.stopPropagation(); copy(r.id); }}
            title={r.id}
            aria-label={`Copy run id ${r.id}`}
          >No. {editionNumber(r.id)}</button>

          <!-- Headline + meta stack. Recipe name is the row's hero, in
               italic Lora; meta below in 11px mono. -->
          <div class="rec-cell">
            <div class="rec-top">
              <button
                type="button"
                class="rec-name"
                title={`Open all runs of ${r.recipe_name}`}
                onclick={(e) => {
                  e.stopPropagation();
                  router.go("recipes", r.recipe_name);
                }}
              >{r.recipe_name}</button>
              {#if r.stage_name}
                <span class="rec-stage">/ {r.stage_name}</span>
              {/if}
              {#if showStatusLabel(r.status)}
                <span class="rec-status">{shortStatus(r.status)}</span>
              {/if}
            </div>
            <div class="rec-meta">
              <span class="hash" title={r.recipe_hash}>{r.recipe_hash.slice(0, 7)}</span>
              {#if r.submitted_by && users.length > 1 && !userFilter}
                <span class="sep">·</span>
                <span>{r.submitted_by}</span>
              {/if}
              {#if r.repo && repos.length > 1 && !repoFilter}
                <span class="sep">·</span>
                <span>{r.repo}</span>
              {/if}
            </div>
          </div>

          <!-- Status: a single colored dot. Color carries hue; pulse
               carries "running." Glyph is reserved for high-contrast
               contexts (the run detail masthead). -->
          <div class="status" aria-label={r.status}>
            <span class="dot" data-group={group} class:animate-pulse-dot={pulse}></span>
          </div>

          <span class="dur mono" class:live={!r.is_terminal}>{formatDuration(dur)}</span>
          <span class="rel mono" title={formatAbsolute(r.created_at)}>{formatRelative(r.created_at, nowSecs.value)}</span>

          <span class="chev" aria-hidden="true">›</span>
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
  .spacer { width: 100%; }
  /* Stanza geometry:
     check | edition-no | headline-stack | status-dot | duration | logged | chevron */
  .run-head,
  .run-row {
    grid-template-columns: 18px 80px 1fr 16px 70px 70px 12px;
    min-height: 56px;
  }
  .run-head { min-height: auto; }

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
    color: #fff;
  }
  .check.checked:hover {
    background: var(--accent-dim);
    border-color: var(--accent-dim);
  }

  /* Edition number — masthead micro-caps, top-aligned to the headline. */
  .edno {
    background: transparent;
    border: none;
    padding: 0;
    cursor: pointer;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.06em;
    color: var(--fg-2);
    text-align: left;
    align-self: start;
    margin-top: 4px;
    transition: color var(--dur-micro) var(--ease);
    font-variant-numeric: tabular-nums;
  }
  .edno:hover { color: var(--fg-0); }

  /* Headline + meta stack. */
  .rec-cell {
    display: flex;
    flex-direction: column;
    gap: 3px;
    overflow: hidden;
    min-width: 0;
  }
  .rec-top {
    display: flex;
    align-items: baseline;
    gap: 8px;
    overflow: hidden;
    min-width: 0;
  }
  .rec-name {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    font-size: 15px;
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
    min-width: 0;
    flex-shrink: 1;
    transition: color var(--dur-micro) var(--ease);
    font-feature-settings: normal;
  }
  .rec-name:hover { color: var(--accent-dim); }
  .rec-stage {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: var(--fg-2);
    flex-shrink: 0;
  }
  .rec-status {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--status-failed);
    flex-shrink: 0;
  }
  .rec-meta {
    display: flex;
    align-items: baseline;
    gap: 6px;
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: var(--fg-2);
    overflow: hidden;
    white-space: nowrap;
  }
  .rec-meta .sep { color: var(--fg-3); }
  .rec-meta .hash {
    color: var(--fg-1);
  }

  /* Status: just the colored dot. */
  .status {
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .dot {
    width: 6px;
    height: 6px;
    border-radius: 999px;
    flex-shrink: 0;
  }
  .dot[data-group="running"]   { --dot: var(--status-running);   background: var(--dot); }
  .dot[data-group="succeeded"] { --dot: var(--status-succeeded); background: var(--dot); }
  .dot[data-group="failed"]    { --dot: var(--status-failed);    background: var(--dot); }
  .dot[data-group="pending"]   { --dot: var(--status-pending);   background: var(--dot); }
  .dot[data-group="neutral"]   { --dot: var(--status-neutral);   background: var(--dot); }

  /* Duration. Right-aligned tabular numerals so values stack cleanly. */
  .dur {
    font-size: 12px;
    color: var(--fg-1);
    font-variant-numeric: tabular-nums;
    text-align: right;
  }
  .dur.live { color: var(--fg-0); }

  /* Logged-at relative time. */
  .rel {
    font-size: 12px;
    color: var(--fg-2);
    font-variant-numeric: tabular-nums;
    text-align: right;
  }

  /* Chevron — a literal typographic affordance, lighter than an icon. */
  .chev {
    color: var(--fg-3);
    font-size: 16px;
    line-height: 1;
    text-align: center;
    transition: color var(--dur-micro) var(--ease), transform var(--dur-micro) var(--ease);
  }
  .list-row:hover .chev,
  .list-row[data-state="active"] .chev { color: var(--fg-1); transform: translateX(2px); }

  .mono { font-family: theme("fontFamily.mono"); }

  :global(.empty code) {
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: var(--fg-0);
    background: var(--bg-2);
    padding: 1px 5px;
    border-radius: 3px;
  }
</style>
