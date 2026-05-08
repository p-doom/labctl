<script lang="ts">
  import { onMount } from "svelte";
  import { router, type View } from "./lib/router.svelte";
  import { store, loadCluster, connectStream } from "./lib/store.svelte";
  import { panelHistory } from "./lib/panel.svelte";

  import LeftRail from "./components/LeftRail.svelte";
  import TopBar from "./components/TopBar.svelte";
  import Palette from "./components/Palette.svelte";
  import CompareBar from "./components/CompareBar.svelte";

  import RunsView from "./views/RunsView.svelte";
  import RunPanel from "./views/RunPanel.svelte";
  import PipelinesView from "./views/PipelinesView.svelte";
  import ArtifactsView from "./views/ArtifactsView.svelte";
  import EvalsView from "./views/EvalsView.svelte";
  import LineageView from "./views/LineageView.svelte";
  import RecipeView from "./views/RecipeView.svelte";
  import CompareView from "./views/CompareView.svelte";

  let cluster = $derived(store.cluster);
  let paletteOpen = $state(false);

  onMount(() => {
    connectStream();
    loadCluster().catch(() => {});
  });

  $effect(() => {
    if (cluster?.name) document.title = `${cluster.name} · labctl`;
  });

  // g-prefix nav: press "g" then a letter within 1.2s.
  let gPending = $state(false);
  let gTimer: number | null = null;

  function onKey(e: KeyboardEvent) {
    const target = e.target as HTMLElement | null;
    const inField =
      target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable);

    // Cmd/Ctrl-K opens palette.
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      paletteOpen = true;
      return;
    }
    // "/" opens palette too (familiar from many apps).
    if (e.key === "/" && !inField && !paletteOpen) {
      e.preventDefault();
      paletteOpen = true;
      return;
    }
    // While the palette is open, it owns the keyboard. Don't double-handle.
    if (paletteOpen) return;
    if (inField) return;

    if (gPending) {
      const map: Record<string, View> = { r: "runs", p: "pipelines", a: "artifacts", e: "evals" };
      const v = map[e.key.toLowerCase()];
      if (v) {
        e.preventDefault();
        router.go(v);
      }
      gPending = false;
      if (gTimer) {
        clearTimeout(gTimer);
        gTimer = null;
      }
      return;
    }
    if (e.key === "g") {
      e.preventDefault();
      gPending = true;
      gTimer = window.setTimeout(() => {
        gPending = false;
      }, 1200);
      return;
    }
    if (e.key === "[" && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      panelHistory.back();
    } else if (e.key === "]" && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      panelHistory.forward();
    }
  }

  $effect(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });
</script>

<div class="shell">
  <LeftRail />
  <main>
    {#if router.view !== "lineage" && router.view !== "recipes" && router.view !== "compare"}
      <TopBar {cluster} onOpenPalette={() => (paletteOpen = true)} />
    {/if}
    <div class="content">
      <!-- All four list views are always mounted; we only toggle which one
           is visible. This preserves scroll, cursor, and filter state when
           the user moves between views. The lineage view is full-viewport
           and only mounted on demand. -->
      <div class="view" data-active={router.view === "runs"}>
        <RunsView />
        {#if router.view === "runs" && router.selected}<RunPanel runId={router.selected} />{/if}
      </div>
      <div class="view" data-active={router.view === "pipelines"}>
        <PipelinesView />
      </div>
      <div class="view" data-active={router.view === "artifacts"}>
        <ArtifactsView />
      </div>
      <div class="view" data-active={router.view === "evals"}>
        <EvalsView />
      </div>
      {#if router.view === "lineage" && router.selected}
        <div class="view" data-active="true">
          <LineageView artifactId={router.selected} />
        </div>
      {:else if router.view === "recipes" && router.selected}
        <div class="view" data-active="true">
          <RecipeView recipeName={router.selected} />
        </div>
      {:else if router.view === "compare"}
        <div class="view" data-active="true">
          <CompareView />
        </div>
      {/if}
    </div>
  </main>
  <Palette open={paletteOpen} onClose={() => (paletteOpen = false)} />
  <CompareBar />
</div>

<style>
  .shell {
    display: flex;
    height: 100vh;
    width: 100vw;
    overflow: hidden;
  }
  main {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-width: 0;
  }
  .content {
    flex: 1;
    min-height: 0;
    position: relative;
    background: theme("colors.bg.0");
    display: flex;
    flex-direction: column;
  }
  .view {
    flex: 1;
    min-height: 0;
    display: none;
  }
  .view[data-active="true"] {
    display: flex;
    flex-direction: column;
  }
</style>
