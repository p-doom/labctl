<script lang="ts">
  import { onMount } from "svelte";
  import { store, loadArtifacts, loadArtifactDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Hash from "../components/Hash.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterChips from "../components/FilterChips.svelte";
  import FilterInput from "../components/FilterInput.svelte";
  import EmptyState from "../components/EmptyState.svelte";
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

  // Hover prefetch — fires after 100ms of cursor stability over a row.
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
        `${a.kind}\n${a.id}\n${a.content_hash}\n${a.path}\n${(a.aliases ?? []).join("\n")}`.toLowerCase(),
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
      placeholder="Filter artifacts…"
      onInput={(v) => (filterText = v)}
      onEnter={openCursorRow}
    />
  </FilterBar>
  <div class="list" bind:this={listEl}>
    <div class="list-head art-head">
      <div>kind</div>
      <div>aliases / id</div>
      <div>hash</div>
      <div>path</div>
      <div>created</div>
    </div>
    {#if isLoading}
      {#each Array(6) as _, i (i)}
        <div class="list-row art-row">
          <div class="skel" style="height: 14px; width: 50px"></div>
          <div class="skel" style="height: 14px; width: 70%"></div>
          <div class="skel" style="height: 12px; width: 80px"></div>
          <div class="skel" style="height: 12px; width: 50%"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
        </div>
      {/each}
    {:else if filtered.length === 0}
      <EmptyState title="No artifacts">
        {#snippet sub()}
          Artifacts appear once a recipe finishes producing outputs.
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
          <span class="kind">{a.kind}</span>
          <div class="aliases">
            {#if a.aliases && a.aliases.length}
              {#each a.aliases as alias}
                <span class="alias">{alias}</span>
              {/each}
            {:else}
              <span class="dim mono">(no alias)</span>
            {/if}
            <span class="id mono">
              <Hash value={a.id} n={10} />
            </span>
          </div>
          <span class="hash">
            <Hash value={a.content_hash} n={10} />
          </span>
          <span class="path mono" title={a.path}>{a.path}</span>
          <RelativeTime ts={a.created_at} />
        </div>
      {/each}
    {/if}
  </div>
</div>

{#if router.selected}
  <ArtifactPanel artifactId={router.selected} />
{/if}

<style>
  .page { display: flex; flex-direction: column; height: 100%; overflow: hidden; }
  .list { flex: 1; overflow-y: auto; }
  .art-head,
  .art-row {
    grid-template-columns: 80px 1fr 96px 1.5fr 96px;
  }
  .kind {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: var(--accent-dim);
    padding: 2px 6px;
    background: var(--accent-soft);
    border-radius: 3px;
    justify-self: start;
  }
  .aliases { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; min-width: 0; }
  .alias {
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: var(--fg-0);
  }
  .id { font-size: 11px; }
  .path {
    font-size: 11px;
    color: var(--fg-2);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
