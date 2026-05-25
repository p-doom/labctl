<script lang="ts">
  import { router } from "../lib/router.svelte";
  import { theme } from "../lib/theme.svelte";
  import Icon from "./Icon.svelte";
  import LiveIndicator from "./LiveIndicator.svelte";
  import type { ClusterInfo } from "../lib/types";

  interface Props {
    cluster: ClusterInfo | null;
  }
  let { cluster }: Props = $props();

  // The Stanza vocabulary: a run is an *edition*, a pipeline is a
  // *series*, an artifact is a *specimen*, a policy is a *rule*. The
  // language is consistent with the website's "No. 5" framing of
  // research releases.
  const titles: Record<string, string> = {
    runs: "Editions",
    pipelines: "Series",
    artifacts: "Specimens",
    policies: "Rules",
  };
</script>

<header class="top">
  <div class="left">
    <h1 class="masthead">{titles[router.view] ?? router.view}</h1>
    {#if cluster}
      <span class="cluster" title={cluster.registry_db}>
        <span class="sep">·</span>
        <span class="name">{cluster.name}</span>
      </span>
    {/if}
  </div>
  <div class="right">
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
    padding: 14px 20px;
    border-bottom: 1px solid theme("colors.line.1");
    background: theme("colors.bg.0");
    height: 52px;
    flex-shrink: 0;
  }
  .left {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  /* Masthead-styled view name. The actual title typography is the small-
   * caps class globally — this just sets the size up a touch. */
  h1.masthead {
    font-size: 12px;
    margin: 0;
  }
  .cluster {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: theme("colors.fg.2");
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }
  .cluster .sep { color: theme("colors.fg.3"); }
  .cluster .name {
    font-family: theme("fontFamily.mono");
    color: theme("colors.fg.1");
    letter-spacing: 0.04em;
    text-transform: none;
    font-weight: 400;
  }
  .right {
    display: flex;
    align-items: center;
    gap: 8px;
  }
</style>
