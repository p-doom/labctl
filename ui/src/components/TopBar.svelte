<script lang="ts">
  import { router } from "../lib/router.svelte";
  import { theme } from "../lib/theme.svelte";
  import Icon from "./Icon.svelte";
  import LiveIndicator from "./LiveIndicator.svelte";
  import type { ClusterInfo } from "../lib/types";

  interface Props {
    cluster: ClusterInfo | null;
    onOpenPalette: () => void;
  }
  let { cluster, onOpenPalette }: Props = $props();

  const titles: Record<string, string> = {
    runs: "Runs",
    pipelines: "Pipelines",
    artifacts: "Artifacts",
    evals: "Evals",
  };
</script>

<header class="top">
  <div class="left">
    <h1>{titles[router.view]}</h1>
    {#if cluster}
      <span class="cluster" title={cluster.registry_db}>
        <span class="sep">/</span>
        <span class="name">{cluster.name}</span>
      </span>
    {/if}
  </div>
  <div class="right">
    <button type="button" class="search" onclick={onOpenPalette} aria-label="Search">
      <Icon name="search" size={12} />
      <span class="text">Search</span>
      <span class="kbds">
        <span class="kbd">⌘</span><span class="kbd">K</span>
      </span>
    </button>
    <button
      type="button"
      class="iconbtn"
      onclick={() => theme.cycle()}
      aria-label={`Theme: ${theme.pref}`}
      title={`Theme: ${theme.pref} (click to cycle)`}
    >
      {#if theme.pref === "dark"}
        <Icon name="moon" size={14} />
      {:else if theme.pref === "light"}
        <Icon name="sun" size={14} />
      {:else}
        <Icon name="system" size={14} />
      {/if}
    </button>
    <LiveIndicator />
  </div>
</header>

<style>
  .top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    border-bottom: 1px solid theme("colors.line.0");
    background: theme("colors.bg.0");
    height: 48px;
    flex-shrink: 0;
  }
  .left {
    display: flex;
    align-items: baseline;
    gap: 8px;
  }
  h1 {
    font-size: 18px;
    font-weight: 500;
    color: theme("colors.fg.0");
    margin: 0;
    letter-spacing: -0.01em;
  }
  .cluster {
    display: inline-flex;
    align-items: baseline;
    gap: 6px;
    font-size: 13px;
    color: theme("colors.fg.2");
  }
  .cluster .name {
    font-family: theme("fontFamily.mono");
    color: theme("colors.fg.1");
  }
  .right {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .search {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 4px 8px 4px 8px;
    background: theme("colors.bg.1");
    border: 1px solid theme("colors.line.1");
    border-radius: 4px;
    color: theme("colors.fg.1");
    cursor: pointer;
    min-width: 220px;
  }
  .search:hover {
    border-color: theme("colors.line.2");
    color: theme("colors.fg.0");
  }
  .search .text {
    font-size: 12px;
    flex: 1;
    text-align: left;
  }
  .kbds {
    display: inline-flex;
    gap: 2px;
  }
</style>
