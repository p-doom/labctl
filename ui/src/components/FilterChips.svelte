<script lang="ts">
  // Reusable chip-group filter. One row per call; multiple rows stack
  // inside <FilterBar>. Each chip has a key (null for "All"), a label,
  // optional count, and optional status dot color.

  import type { ChipDef } from "../lib/filters";

  interface Props {
    chips: ChipDef[];
    active: string | null;
    label?: string;
    onSelect: (key: string | null) => void;
  }
  let { chips, active, label, onSelect }: Props = $props();

  function shouldRender(c: ChipDef, idx: number): boolean {
    if (c.always ?? c.key === null) return true;
    if (active === c.key) return true;
    if (c.count == null) return true;
    return c.count > 0;
  }
</script>

<div class="row">
  {#if label}
    <span class="label mono">{label}</span>
  {/if}
  <div class="chips">
    {#each chips as c, i (c.key ?? `__all-${i}`)}
      {#if shouldRender(c, i)}
        <button
          type="button"
          class="chip"
          class:active={active === c.key}
          data-dot={c.dot ?? null}
          onclick={() => onSelect(active === c.key ? null : c.key)}
        >
          {#if c.dot}
            <span class="dot"></span>
          {/if}
          <span class="text">{c.label}</span>
          {#if c.count != null}
            <span class="count">{c.count}</span>
          {/if}
        </button>
      {/if}
    {/each}
  </div>
</div>

<style>
  .row {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }
  .label {
    font-size: 11px;
    color: theme("colors.fg.2");
    flex-shrink: 0;
  }
  .chips {
    display: flex;
    align-items: center;
    gap: 4px;
    flex-wrap: wrap;
  }
  .chip {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: transparent;
    border: 1px solid transparent;
    color: theme("colors.fg.1");
    font-size: 12px;
    padding: 3px 9px;
    border-radius: 999px;
    cursor: pointer;
  }
  .chip:hover {
    background: theme("colors.bg.2");
    color: theme("colors.fg.0");
  }
  .chip.active {
    background: theme("colors.bg.2");
    color: theme("colors.fg.0");
    border-color: theme("colors.line.1");
  }
  .chip .text {
    font-family: theme("fontFamily.sans");
  }
  .chip .count {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.fg.2");
  }
  .chip.active .count {
    color: theme("colors.fg.1");
  }
  .chip .dot {
    width: 6px;
    height: 6px;
    border-radius: 999px;
    background: theme("colors.fg.3");
  }
  .chip[data-dot="running"] .dot { background: theme("colors.status.running.DEFAULT"); }
  .chip[data-dot="succeeded"] .dot { background: theme("colors.status.succeeded.DEFAULT"); }
  .chip[data-dot="failed"] .dot { background: theme("colors.status.failed.DEFAULT"); }
  .chip[data-dot="pending"] .dot { background: theme("colors.status.pending.DEFAULT"); }
  .chip[data-dot="neutral"] .dot { background: theme("colors.fg.3"); }
</style>
