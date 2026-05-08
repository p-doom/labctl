<script lang="ts">
  // Floating action bar that appears once the user has shift-selected one
  // or more runs to compare. Bottom-fixed, escapes the panel-stacking
  // context so it stays visible above row hovers.

  import { compareSelection } from "../lib/compare.svelte";
  import { router } from "../lib/router.svelte";
  import Icon from "./Icon.svelte";

  let count = $derived(compareSelection.size);

  function go() {
    const ids = compareSelection.ids;
    if (ids.length === 0) return;
    const q = new URLSearchParams({ runs: ids.join(",") });
    router.go("compare", null, q);
  }

  function clear() {
    compareSelection.clear();
  }
</script>

{#if count > 0}
  <div class="bar" role="region" aria-label="Compare selection">
    <span class="count mono">{count}</span>
    <span class="label">{count === 1 ? "run selected" : "runs selected"}</span>
    {#if count < 2}
      <span class="hint">pick another to compare</span>
    {/if}
    <div class="spacer"></div>
    <button type="button" class="ghost" onclick={clear}>Clear</button>
    <button type="button" class="primary" onclick={go} disabled={count < 2}>
      <span>Compare</span>
      <Icon name="chevron-right" size={12} />
    </button>
  </div>
{/if}

<style>
  .bar {
    position: fixed;
    bottom: 16px;
    left: 50%;
    transform: translateX(-50%);
    display: inline-flex;
    align-items: center;
    gap: 12px;
    background: var(--bg-2);
    border: 1px solid var(--line-2);
    border-radius: 999px;
    padding: 6px 6px 6px 14px;
    box-shadow: 0 8px 24px -8px var(--shadow-panel-1);
    z-index: 50;
    animation: rise 200ms cubic-bezier(0.2, 0, 0, 1);
  }
  @keyframes rise {
    from {
      opacity: 0;
      transform: translate(-50%, 12px);
    }
    to {
      opacity: 1;
      transform: translate(-50%, 0);
    }
  }
  .count {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 22px;
    height: 22px;
    padding: 0 7px;
    background: var(--accent-soft);
    color: var(--accent);
    border-radius: 999px;
    font-size: 11px;
    font-weight: 500;
  }
  .label {
    font-size: 12px;
    color: var(--fg-0);
  }
  .hint {
    font-family: theme("fontFamily.mono");
    font-size: 10px;
    color: var(--fg-3);
  }
  .spacer { width: 12px; }
  .ghost,
  .primary {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    padding: 5px 12px;
    border-radius: 999px;
    cursor: pointer;
    border: 1px solid transparent;
  }
  .ghost {
    background: transparent;
    color: var(--fg-1);
  }
  .ghost:hover {
    color: var(--fg-0);
  }
  .primary {
    background: var(--accent-soft);
    color: var(--accent-dim);
    border-color: var(--accent-soft);
  }
  .primary:hover {
    background: var(--accent);
    color: var(--bg-0);
    border-color: var(--accent);
  }
  .primary:disabled {
    opacity: 0.4;
    cursor: not-allowed;
    pointer-events: none;
  }
</style>
