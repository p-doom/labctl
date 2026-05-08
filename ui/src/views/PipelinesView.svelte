<script lang="ts">
  import { onMount } from "svelte";
  import { store, loadPipelines, loadPipelineDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import Pill from "../components/Pill.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Hash from "../components/Hash.svelte";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterChips from "../components/FilterChips.svelte";
  import type { ChipDef } from "../lib/filters";
  import PipelineDetail from "./PipelineDetail.svelte";

  onMount(() => loadPipelines());

  let statusFilter = $derived(router.query.get("status"));

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
  let filtered = $derived.by(() => {
    if (!statusFilter) return allPipelines;
    return allPipelines.filter((p) => p.status === statusFilter);
  });

  let statusChips = $derived<ChipDef[]>([
    { key: null, label: "All", count: allPipelines.length, always: true },
    { key: "running", label: "Running", count: counts.running, dot: "running" },
    { key: "failed", label: "Failed", count: counts.failed, dot: "failed" },
    { key: "succeeded", label: "Succeeded", count: counts.succeeded, dot: "succeeded" },
    { key: "mixed", label: "Mixed", count: counts.mixed, dot: "neutral" },
  ]);

</script>

<div class="page">
  <FilterBar>
    <FilterChips chips={statusChips} active={statusFilter} onSelect={(k) => router.setQuery({ status: k })} />
  </FilterBar>
  <div class="list">
    <div class="header">
      <div></div>
      <div>name</div>
      <div>id</div>
      <div>stages</div>
      <div>created</div>
    </div>
    {#if isLoading}
      {#each Array(4) as _, i (i)}
        <div class="prow">
          <div class="skel" style="width: 6px; height: 6px"></div>
          <div class="skel" style="height: 14px; width: 50%"></div>
          <div class="skel" style="height: 12px; width: 100px"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
          <div class="skel" style="height: 12px; width: 60px"></div>
        </div>
      {/each}
    {:else if filtered.length === 0}
      <div class="empty">
        <p class="title">{allPipelines.length === 0 ? "No pipelines yet" : "No pipelines match"}</p>
        {#if allPipelines.length === 0}
          <p class="sub">Submit one with <code>labctl run-pipeline</code>.</p>
        {:else}
          <p class="sub">Clear the filter chips above.</p>
        {/if}
      </div>
    {:else}
      {#each filtered as p}
        <div
          class="prow"
          class:is-active={router.selected === p.id}
          onclick={() => router.select("pipelines", p.id)}
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
  .header {
    display: grid;
    grid-template-columns: 22px 1fr 140px 80px 100px;
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
  .prow {
    display: grid;
    grid-template-columns: 22px 1fr 140px 80px 100px;
    column-gap: 12px;
    align-items: center;
    padding: 8px 16px;
    border-bottom: 1px solid theme("colors.line.0");
    cursor: pointer;
    font-size: 13px;
  }
  .prow:hover, .prow.is-active { background: theme("colors.bg.2"); }
  .name { font-family: theme("fontFamily.mono"); color: theme("colors.fg.0"); }
  .stages { font-family: theme("fontFamily.mono"); font-size: 12px; color: theme("colors.fg.1"); }
  .empty {
    padding: 80px 24px;
    text-align: center;
  }
  .empty .title { font-size: 14px; color: theme("colors.fg.0"); margin: 0 0 6px 0; }
  .empty .sub { font-size: 13px; color: theme("colors.fg.2"); margin: 0; }
  .empty code {
    font-family: theme("fontFamily.mono");
    background: theme("colors.bg.2");
    padding: 1px 5px;
    border-radius: 3px;
    color: theme("colors.fg.1");
  }
</style>
