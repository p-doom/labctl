<script lang="ts">
  import { onMount } from "svelte";
  import { store, loadArtifacts, loadArtifactDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Hash from "../components/Hash.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterChips from "../components/FilterChips.svelte";
  import type { ChipDef } from "../lib/filters";
  import ArtifactPanel from "./ArtifactPanel.svelte";

  onMount(() => loadArtifacts());

  let kindFilter = $derived(router.query.get("kind"));
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
  let filtered = $derived.by(() => {
    if (!kindFilter) return allArtifacts;
    return allArtifacts.filter((a) => a.kind === kindFilter);
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
  {#if kinds.length > 1}
    <FilterBar>
      <FilterChips
        label="kind"
        chips={kindChips}
        active={kindFilter}
        onSelect={(k) => router.setQuery({ kind: k })}
      />
    </FilterBar>
  {/if}
  <div class="list">
    <div class="header">
      <div>kind</div>
      <div>aliases / id</div>
      <div>hash</div>
      <div>path</div>
      <div>created</div>
    </div>
    {#if isLoading}
      {#each Array(6) as _, i (i)}
        <div class="arow">
          <div class="skel" style="height: 14px; width: 50px"></div>
          <div class="skel" style="height: 14px; width: 70%"></div>
          <div class="skel" style="height: 12px; width: 80px"></div>
          <div class="skel" style="height: 12px; width: 50%"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
        </div>
      {/each}
    {:else if filtered.length === 0}
      <div class="empty">
        <p class="title">No artifacts</p>
        <p class="sub">Artifacts appear once a recipe finishes producing outputs.</p>
      </div>
    {:else}
      {#each filtered as a}
        <div
          class="arow"
          class:is-active={router.selected === a.id}
          onclick={() => router.select("artifacts", a.id)}
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
              <span class="muted mono">(no alias)</span>
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
  .header {
    display: grid;
    grid-template-columns: 80px 1fr 110px 1.5fr 90px;
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
  .arow {
    display: grid;
    grid-template-columns: 80px 1fr 110px 1.5fr 90px;
    column-gap: 14px;
    align-items: center;
    padding: 8px 16px;
    border-bottom: 1px solid theme("colors.line.0");
    cursor: pointer;
    font-size: 13px;
  }
  .arow:hover, .arow.is-active { background: theme("colors.bg.2"); }
  .kind {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.accent.dim");
    padding: 2px 6px;
    background: theme("colors.accent.soft");
    border-radius: 3px;
    justify-self: start;
  }
  .aliases { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; min-width: 0; }
  .alias {
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: theme("colors.fg.0");
  }
  .muted { color: theme("colors.fg.3"); font-size: 11px; }
  .id { font-size: 11px; }
  .path {
    font-size: 11px;
    color: theme("colors.fg.2");
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .empty {
    padding: 80px 24px;
    text-align: center;
  }
  .empty .title { font-size: 14px; color: theme("colors.fg.0"); margin: 0 0 6px 0; }
  .empty .sub { font-size: 13px; color: theme("colors.fg.2"); margin: 0; }
</style>
