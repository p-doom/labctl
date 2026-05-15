<script lang="ts">
  import { onMount } from "svelte";
  import { store, loadPipelines, loadPipelineDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import Pill from "../components/Pill.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Hash from "../components/Hash.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterChips from "../components/FilterChips.svelte";
  import FilterInput from "../components/FilterInput.svelte";
  import EmptyState from "../components/EmptyState.svelte";
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
      placeholder="Filter pipelines…"
      onInput={(v) => (filterText = v)}
      onEnter={openCursorRow}
    />
  </FilterBar>
  <div class="list" bind:this={listEl}>
    <div class="list-head pipe-head">
      <div></div>
      <div>name</div>
      <div>id</div>
      <div>stages</div>
      <div>created</div>
    </div>
    {#if isLoading}
      {#each Array(4) as _, i (i)}
        <div class="list-row pipe-row">
          <div class="skel" style="width: 6px; height: 6px"></div>
          <div class="skel" style="height: 14px; width: 50%"></div>
          <div class="skel" style="height: 12px; width: 100px"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
        </div>
      {/each}
    {:else if filtered.length === 0}
      <EmptyState title={allPipelines.length === 0 ? "No pipelines yet" : "No pipelines match"}>
        {#snippet sub()}
          {#if allPipelines.length === 0}
            Submit one with <code>labctl run-pipeline</code>.
          {:else}
            Clear the filter chips above.
          {/if}
        {/snippet}
      </EmptyState>
    {:else}
      {#each filtered as p, i (p.id)}
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
          <Pill status={p.status} showLabel={false} />
          <span class="name mono">{p.name}</span>
          <span class="id"><Hash value={p.id} n={12} /></span>
          <span class="stages">{p.stage_count}</span>
          <RelativeTime ts={p.created_at} />
        </div>
      {/each}
    {/if}
  </div>
</div>

{#if router.selected && detail}
  <PipelineDetail detail={detail} />
{/if}

<style>
  .page { display: flex; flex-direction: column; height: 100%; overflow: hidden; }
  .list { flex: 1; overflow-y: auto; }
  .pipe-head,
  .pipe-row {
    grid-template-columns: 22px 1fr 140px 80px 100px;
  }
  .name { font-family: theme("fontFamily.mono"); color: var(--fg-0); }
  .stages { font-family: theme("fontFamily.mono"); font-size: 12px; color: var(--fg-1); }
</style>
