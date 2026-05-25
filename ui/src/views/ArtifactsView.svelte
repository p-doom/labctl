<script lang="ts">
  import { onMount } from "svelte";
  import { store, loadArtifacts, loadArtifactDetail } from "../lib/store.svelte";
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
  import ArtifactPanel from "./ArtifactPanel.svelte";

  onMount(() => loadArtifacts());

  let kindFilter = $derived(router.query.get("kind"));
  let filterText = $state("");
  let textQuery = $derived(filterText.trim().toLowerCase());
  let cursor = $state(0);
  let listEl = $state<HTMLDivElement | null>(null);
  let filterInputEl = $state<HTMLInputElement | null>(null);
  $effect(() => { void kindFilter; void textQuery; cursor = 0; });
  $effect(() => { if (cursor >= filtered.length) cursor = Math.max(0, filtered.length - 1); });
  function openCursorRow() {
    const a = filtered[cursor];
    if (a) router.select("artifacts", a.id);
  }
  function onKey(e: KeyboardEvent) {
    if (router.view !== "artifacts") return;
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
      if (router.selected) router.select("artifacts", filtered[cursor]?.id ?? null);
    } else if (e.key === "k" || e.key === "ArrowUp") {
      e.preventDefault();
      cursor = Math.max(cursor - 1, 0);
      if (router.selected) router.select("artifacts", filtered[cursor]?.id ?? null);
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
  let allArtifacts = $derived(store.artifacts.data ?? []);
  let isLoading = $derived(store.artifacts.loading && allArtifacts.length === 0);

  // Hover prefetch — 100ms cursor stability before firing.
  let prefetchTimer: number | null = null;
  let prefetchTarget: string | null = null;
  function onRowEnter(id: string) {
    prefetchTarget = id;
    if (prefetchTimer) clearTimeout(prefetchTimer);
    prefetchTimer = window.setTimeout(() => {
      if (prefetchTarget === id) loadArtifactDetail(id).catch(() => {});
    }, 100);
  }
  function onRowLeave() {
    if (prefetchTimer) clearTimeout(prefetchTimer);
    prefetchTimer = null;
    prefetchTarget = null;
  }
  let haystacks = $derived.by(() => {
    const m = new Map<string, string>();
    for (const a of allArtifacts) {
      m.set(
        a.id,
        `${a.kind}\n${a.id}\n${a.path}\n${(a.aliases ?? []).join("\n")}`.toLowerCase(),
      );
    }
    return m;
  });

  let filtered = $derived.by(() => {
    const q = textQuery;
    const useText = q.length > 0;
    const useKind = kindFilter != null;
    if (!useText && !useKind) return allArtifacts;
    return allArtifacts.filter((a) => {
      if (useKind && a.kind !== kindFilter) return false;
      if (useText && !haystacks.get(a.id)!.includes(q)) return false;
      return true;
    });
  });
  let kinds = $derived.by(() => {
    const s = new Set<string>();
    for (const a of allArtifacts) s.add(a.kind);
    return [...s].sort();
  });

  let kindChips = $derived<ChipDef[]>([
    { key: null, label: "All", count: allArtifacts.length, always: true },
    ...kinds.map((k) => ({
      key: k,
      label: k,
      count: allArtifacts.filter((a) => a.kind === k).length,
    })),
  ]);
</script>

<div class="page">
  <FilterBar>
    {#if kinds.length > 1}
      <FilterChips
        label="kind"
        chips={kindChips}
        active={kindFilter}
        onSelect={(k) => router.setQuery({ kind: k })}
      />
    {/if}
    <FilterInput
      bind:inputRef={filterInputEl}
      value={filterText}
      placeholder="Filter specimens…"
      onInput={(v) => (filterText = v)}
      onEnter={openCursorRow}
    />
  </FilterBar>
  <div class="list" bind:this={listEl}>
    <div class="list-head art-head">
      <div>Catalog</div>
      <div>Specimen</div>
      <div>Logged</div>
      <div></div>
    </div>
    {#if isLoading}
      {#each Array(6) as _, i (i)}
        <div class="list-row art-row">
          <div class="skel" style="height: 11px; width: 80px"></div>
          <div class="art-cell">
            <div class="skel" style="height: 14px; width: 50%; margin-bottom: 4px"></div>
            <div class="skel" style="height: 11px; width: 70%"></div>
          </div>
          <div class="skel" style="height: 11px; width: 60px"></div>
          <div></div>
        </div>
      {/each}
    {:else if filtered.length === 0}
      <EmptyState title="No specimens.">
        {#snippet sub()}
          Specimens are deposited when an edition produces outputs.
        {/snippet}
      </EmptyState>
    {:else}
      {#each filtered as a, i (a.id)}
        <div
          class="list-row art-row"
          data-state={
            router.selected === a.id ? "active" :
            (cursor === i && !router.selected) ? "cursor" :
            undefined
          }
          onclick={() => { cursor = i; router.select("artifacts", a.id); }}
          onmouseenter={() => onRowEnter(a.id)}
          onmouseleave={onRowLeave}
          role="button"
          tabindex="0"
          onkeydown={(e) => e.key === "Enter" && router.select("artifacts", a.id)}
        >
          <span class="catno">No. {editionNumber(a.id)}</span>
          <div class="art-cell">
            <div class="art-top">
              <span class="kind">{a.kind}</span>
              {#if a.aliases && a.aliases.length}
                {#each a.aliases as alias}
                  <span class="alias mono">{alias}</span>
                {/each}
              {/if}
            </div>
            <div class="art-meta mono" title={a.path}>{a.path}</div>
          </div>
          <span class="rel mono" title={formatAbsolute(a.created_at)}>{formatRelative(a.created_at, nowSecs.value)}</span>
          <span class="chev" aria-hidden="true">›</span>
        </div>
      {/each}
    {/if}
  </div>
</div>

{#if router.view === "artifacts" && router.selected}
  <ArtifactPanel artifactId={router.selected} />
{/if}

<style>
  .page { display: flex; flex-direction: column; height: 100%; overflow: hidden; }
  .list { flex: 1; overflow-y: auto; }
  /* Stanza specimen geometry:
     catalog-no | kind+aliases+path stack | logged | chev */
  .art-head,
  .art-row {
    grid-template-columns: 100px 1fr 80px 12px;
    min-height: 56px;
  }
  .art-head { min-height: auto; }

  .catno {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--fg-2);
    align-self: start;
    margin-top: 4px;
    font-variant-numeric: tabular-nums;
  }

  .art-cell {
    display: flex;
    flex-direction: column;
    gap: 3px;
    overflow: hidden;
    min-width: 0;
  }
  .art-top {
    display: flex;
    align-items: baseline;
    gap: 8px;
    overflow: hidden;
    min-width: 0;
  }
  .kind {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    font-size: 15px;
    color: var(--fg-0);
    letter-spacing: -0.005em;
    flex-shrink: 0;
    font-feature-settings: normal;
  }
  .alias {
    font-size: 11px;
    color: var(--accent-dim);
    background: var(--accent-soft);
    padding: 1px 6px;
    border-radius: 3px;
    flex-shrink: 0;
  }
  .art-meta {
    font-size: 11px;
    color: var(--fg-2);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
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
