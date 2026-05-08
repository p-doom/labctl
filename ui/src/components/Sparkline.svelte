<script lang="ts">
  // Per-recipe pass/fail history. Each cell is a thin vertical bar colored
  // by status group. Newest on the right. Hover shows the run.
  import type { RunStatus } from "../lib/types";
  import { statusGroup } from "../lib/format";

  interface Props {
    history: { status: RunStatus | string; created_at: number }[];
    max?: number;
  }
  let { history, max = 16 }: Props = $props();

  let cells = $derived.by(() => {
    const recent = history.slice(-max);
    const pad = Math.max(0, max - recent.length);
    return [
      ...Array(pad).fill(null),
      ...recent.map((h) => statusGroup(h.status)),
    ] as (ReturnType<typeof statusGroup> | null)[];
  });
</script>

<div class="spark" title="recent run history (oldest → newest)">
  {#each cells as cell, i (i)}
    <span class="cell" data-group={cell ?? "empty"}></span>
  {/each}
</div>

<style>
  .spark {
    display: inline-flex;
    gap: 2px;
    align-items: center;
    height: 16px;
  }
  .cell {
    width: 3px;
    height: 14px;
    border-radius: 1px;
    background: theme("colors.bg.3");
  }
  .cell[data-group="running"] {
    background: theme("colors.status.running.DEFAULT");
  }
  .cell[data-group="succeeded"] {
    background: theme("colors.status.succeeded.DEFAULT");
    opacity: 0.7;
  }
  .cell[data-group="failed"] {
    background: theme("colors.status.failed.DEFAULT");
  }
  .cell[data-group="pending"] {
    background: theme("colors.status.pending.DEFAULT");
    opacity: 0.7;
  }
  .cell[data-group="neutral"] {
    background: theme("colors.fg.3");
  }
  .cell[data-group="empty"] {
    background: theme("colors.bg.2");
  }
</style>
