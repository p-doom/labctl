<script lang="ts">
  import { onMount } from "svelte";
  import { store, loadPipelines, loadPipelineDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterChips from "../components/FilterChips.svelte";
  import FilterInput from "../components/FilterInput.svelte";
  import EmptyState from "../components/EmptyState.svelte";
  import {
    editionNumber,
    formatRelative,
    formatAbsolute,
  } from "../lib/format";
  import { nowSecs } from "../lib/time.svelte";
  import type { ChipDef } from "../lib/filters";
  import PipelineDetail from "./PipelineDetail.svelte";

  onMount(() => loadPipelines());

  let statusFilter = $derived(router.query.get("status"));
  let filterText = $state("");
  let textQuery = $derived(filterText.trim().toLowerCase());

  let allPipelines = $derived(store.pipelines.data ?? []);
  let isLoading = $derived(store.pipelines.loading && allPipelines.length === 0);
  let detail = $derived(router.selected ? store.pipelineDetail(router.selected) : null);

  $effect(() => {
    const id = router.selected;
    if (id && router.view === "pipelines") loadPipelineDetail(id).catch(() => {});
  });
  let counts = $derived.by(() => {
    const c = { running: 0, succeeded: 0, failed: 0, mixed: 0, unknown: 0 };
    for (const p of allPipelines) c[p.status]++;
    return c;
  });
  let haystacks = $derived.by(() => {
    const m = new Map<string, string>();
    for (const p of allPipelines) m.set(p.id, `${p.name}\n${p.id}`.toLowerCase());
    return m;
  });

  let filtered = $derived.by(() => {
    const q = textQuery;
    const useText = q.length > 0;
    const useStatus = statusFilter != null;
    if (!useText && !useStatus) return allPipelines;
    return allPipelines.filter((p) => {
      if (useStatus && p.status !== statusFilter) return false;
      if (useText && !haystacks.get(p.id)!.includes(q)) return false;
      return true;
    });
  });

  let statusChips = $derived<ChipDef[]>([
    { key: null, label: "All", count: allPipelines.length, always: true },
    { key: "running", label: "Running", count: counts.running, dot: "running" },
    { key: "failed", label: "Failed", count: counts.failed, dot: "failed" },
    { key: "succeeded", label: "Succeeded", count: counts.succeeded, dot: "succeeded" },
    { key: "mixed", label: "Mixed", count: counts.mixed, dot: "neutral" },
  ]);

  function statusGroupForPipeline(s: string): "running" | "succeeded" | "failed" | "pending" | "neutral" {
    if (s === "running") return "running";
    if (s === "succeeded") return "succeeded";
    if (s === "failed") return "failed";
    if (s === "mixed") return "neutral";
    return "pending";
  }

  let cursor = $state(0);
  let filterInputEl = $state<HTMLInputElement | null>(null);
  let listEl = $state<HTMLDivElement | null>(null);
  $effect(() => { void statusFilter; void textQuery; cursor = 0; });
  $effect(() => { if (cursor >= filtered.length) cursor = Math.max(0, filtered.length - 1); });

  function openCursorRow() {
    const p = filtered[cursor];
    if (p) router.select("pipelines", p.id);
  }
  function onKey(e: KeyboardEvent) {
    if (router.view !== "pipelines") return;
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
    if (e.key === "j" || e.key === "ArrowDown") {
      e.preventDefault();
      cursor = Math.min(cursor + 1, filtered.length - 1);
      if (router.selected) router.select("pipelines", filtered[cursor]?.id ?? null);
    } else if (e.key === "k" || e.key === "ArrowUp") {
      e.preventDefault();
      cursor = Math.max(cursor - 1, 0);
      if (router.selected) router.select("pipelines", filtered[cursor]?.id ?? null);
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
    if (!listEl) return;
    requestAnimationFrame(() => {
      (listEl!.querySelectorAll<HTMLElement>('.list-row'))[i]?.scrollIntoView({ block: "nearest" });
    });
  });
</script>

<div class="page">
  <FilterBar>
    <FilterChips chips={statusChips} active={statusFilter} onSelect={(k) => router.setQuery({ status: k })} />
    <FilterInput
      bind:inputRef={filterInputEl}
      value={filterText}
      placeholder="Filter series…"
      onInput={(v) => (filterText = v)}
      onEnter={openCursorRow}
    />
  </FilterBar>
  <div class="list" bind:this={listEl}>
    <div class="list-head pipe-head">
      <div>Series</div>
      <div>Name</div>
      <div></div>
      <div>Stages</div>
      <div>Created</div>
      <div></div>
    </div>
    {#if isLoading}
      {#each Array(4) as _, i (i)}
        <div class="list-row pipe-row">
          <div class="skel" style="height: 11px; width: 60px"></div>
          <div class="pipe-cell">
            <div class="skel" style="height: 15px; width: 50%; margin-bottom: 4px"></div>
            <div class="skel" style="height: 11px; width: 40%"></div>
          </div>
          <div class="skel" style="width: 8px; height: 8px; border-radius: 4px"></div>
          <div class="skel" style="height: 12px; width: 30px"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
          <div></div>
        </div>
      {/each}
    {:else if filtered.length === 0}
      <EmptyState title={allPipelines.length === 0 ? "No series yet." : "No series match these filters."}>
        {#snippet sub()}
          {#if allPipelines.length === 0}
            Submit one with <code>labctl run-pipeline</code>. A series collects editions stage by stage.
          {:else}
            Clear the filter chips above to widen the catalogue.
          {/if}
        {/snippet}
      </EmptyState>
    {:else}
      {#each filtered as p, i (p.id)}
        {@const group = statusGroupForPipeline(p.status)}
        {@const pulse = group === "running"}
        <div
          class="list-row pipe-row"
          data-state={
            router.selected === p.id ? "active" :
            (cursor === i && router.selected !== p.id) ? "cursor" :
            undefined
          }
          onclick={() => { cursor = i; router.select("pipelines", p.id); }}
          role="button"
          tabindex="0"
          onkeydown={(e) => e.key === "Enter" && router.select("pipelines", p.id)}
        >
          <span class="seno">Series {editionNumber(p.id)}</span>
          <div class="pipe-cell">
            <span class="pipe-name">{p.name}</span>
            <span class="pipe-meta mono">{p.id.slice(0, 12)}</span>
          </div>
          <div class="status">
            <span class="dot" data-group={group} class:animate-pulse-dot={pulse}></span>
          </div>
          <span class="stages mono">{p.stage_count}</span>
          <span class="rel mono" title={formatAbsolute(p.created_at)}>{formatRelative(p.created_at, nowSecs.value)}</span>
          <span class="chev" aria-hidden="true">›</span>
        </div>
      {/each}
    {/if}
  </div>
</div>

{#if router.view === "pipelines" && router.selected && detail}
  <PipelineDetail detail={detail} />
{/if}

<style>
  .page { display: flex; flex-direction: column; height: 100%; overflow: hidden; }
  .list { flex: 1; overflow-y: auto; }
  /* Stanza pipeline geometry:
     series-no | name-stack | status | stages | created | chev */
  .pipe-head,
  .pipe-row {
    grid-template-columns: 110px 1fr 16px 60px 80px 12px;
    min-height: 56px;
  }
  .pipe-head { min-height: auto; }

  .seno {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--fg-2);
    align-self: start;
    margin-top: 4px;
    font-variant-numeric: tabular-nums;
  }

  .pipe-cell {
    display: flex;
    flex-direction: column;
    gap: 3px;
    overflow: hidden;
    min-width: 0;
  }
  .pipe-name {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    font-size: 15px;
    color: var(--fg-0);
    letter-spacing: -0.005em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-feature-settings: normal;
  }
  .pipe-meta {
    font-size: 11px;
    color: var(--fg-2);
  }

  .status { display: flex; align-items: center; justify-content: center; }
  .dot {
    width: 6px;
    height: 6px;
    border-radius: 999px;
    flex-shrink: 0;
  }
  .dot[data-group="running"]   { background: var(--status-running); }
  .dot[data-group="succeeded"] { background: var(--status-succeeded); }
  .dot[data-group="failed"]    { background: var(--status-failed); }
  .dot[data-group="pending"]   { background: var(--status-pending); }
  .dot[data-group="neutral"]   { background: var(--status-neutral); }

  .stages {
    font-size: 12px;
    color: var(--fg-1);
    font-variant-numeric: tabular-nums;
    text-align: right;
  }
  .rel {
    font-size: 12px;
    color: var(--fg-2);
    font-variant-numeric: tabular-nums;
    text-align: right;
  }
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
