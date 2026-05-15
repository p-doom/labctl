<script lang="ts">
  // Metric selector chip row used at the top of PolicyDetail, Compare,
  // and Recipe views. One chip per metric with a count-of-runs badge.
  // Stateless: parent owns active + setActive.

  interface Props {
    metrics: string[];
    active: string | null;
    /** Map of metric name → run count (so each chip can show its coverage). */
    runCount: (m: string) => number;
    /** Total number of runs in the view (denominator for the count badge). */
    totalRuns: number;
    onSelect: (m: string) => void;
  }
  let { metrics, active, runCount, totalRuns, onSelect }: Props = $props();
</script>

<div class="metric-chips">
  <span class="m-label mono">metric</span>
  {#each metrics as m (m)}
    {@const count = runCount(m)}
    <button
      type="button"
      class="m-chip"
      class:active={active === m}
      onclick={() => onSelect(m)}
    >
      <span class="text mono">{m}</span>
      <span class="count mono">{count}/{totalRuns}</span>
    </button>
  {/each}
</div>

<style>
  .metric-chips {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
  }
  .m-label {
    font-size: 11px;
    color: var(--fg-3);
    margin-right: 4px;
  }
  .m-chip {
    display: inline-flex;
    align-items: baseline;
    gap: 6px;
    padding: 3px 10px;
    background: transparent;
    border: 1px solid var(--line-1);
    border-radius: 999px;
    color: var(--fg-1);
    cursor: pointer;
    font: inherit;
    transition: background-color var(--dur-micro) var(--ease),
      color var(--dur-micro) var(--ease),
      border-color var(--dur-micro) var(--ease);
  }
  .m-chip:hover {
    background: var(--bg-2);
    color: var(--fg-0);
  }
  .m-chip.active {
    background: var(--accent-soft);
    border-color: var(--accent-dim);
    color: var(--accent-dim);
  }
  .m-chip .text { font-size: 12px; }
  .m-chip .count {
    font-size: 11px;
    color: var(--fg-3);
  }
  .m-chip.active .count {
    color: var(--accent-dim);
    opacity: 0.8;
  }
</style>
