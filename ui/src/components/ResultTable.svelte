<script lang="ts">
  // Render a conforming `{tasks, primary}` eval result as a small metric
  // table. The shape labctl recognizes (and recipes are encouraged to
  // emit):
  //   {
  //     "tasks": { "<task>": { "value": number, "stderr"?: number, "n"?: number } },
  //     "primary": "<task>"
  //   }
  // Everything else falls through to <JsonTree>.

  interface TaskEntry {
    value: number;
    stderr?: number;
    n?: number;
    [key: string]: unknown;
  }

  interface Props {
    tasks: Record<string, TaskEntry>;
    primary?: string | null;
  }
  let { tasks, primary }: Props = $props();

  let entries = $derived(Object.entries(tasks));

  /** Compact number format: 0.6543 → "0.654"; large ints stay as-is. */
  function fmt(v: number): string {
    if (!Number.isFinite(v)) return String(v);
    if (Number.isInteger(v)) return v.toLocaleString();
    if (Math.abs(v) >= 100) return v.toFixed(1);
    if (Math.abs(v) >= 1) return v.toFixed(3);
    return v.toFixed(4);
  }

  function fmtN(n?: number): string | null {
    if (n == null) return null;
    if (n >= 1000) return `n=${Math.round(n / 1000)}k`;
    return `n=${n}`;
  }
</script>

<div class="rt">
  {#each entries as [task, entry] (task)}
    <div class="row" class:primary={task === primary}>
      <span class="task mono">{task}</span>
      <span class="value mono">{fmt(entry.value)}</span>
      {#if entry.stderr != null}
        <span class="se mono">±{fmt(entry.stderr)}</span>
      {:else}
        <span class="se"></span>
      {/if}
      <span class="n mono">{fmtN(entry.n) ?? ""}</span>
    </div>
  {/each}
</div>

<style>
  .rt {
    display: flex;
    flex-direction: column;
    gap: 1px;
    background: theme("colors.bg.0");
    border: 1px solid theme("colors.line.0");
    border-radius: 6px;
    overflow: hidden;
  }
  .row {
    display: grid;
    grid-template-columns: 1fr auto auto auto;
    column-gap: 14px;
    align-items: baseline;
    padding: 7px 12px;
    background: theme("colors.bg.1");
    font-size: 12px;
    line-height: 1.2;
  }
  .row.primary {
    background: theme("colors.accent.soft");
    position: relative;
  }
  .row.primary::before {
    content: "";
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 2px;
    background: theme("colors.accent.DEFAULT");
  }
  .task {
    font-size: 12px;
    color: theme("colors.fg.0");
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .row.primary .task {
    color: theme("colors.accent.dim");
  }
  .value {
    font-size: 13px;
    color: theme("colors.fg.0");
    font-variant-numeric: tabular-nums;
    text-align: right;
    min-width: 56px;
  }
  .row.primary .value {
    color: theme("colors.accent.DEFAULT");
    font-weight: 500;
  }
  .se {
    font-size: 10px;
    color: theme("colors.fg.3");
    font-variant-numeric: tabular-nums;
    min-width: 48px;
    text-align: right;
  }
  .n {
    font-size: 10px;
    color: theme("colors.fg.3");
    text-align: right;
    min-width: 56px;
  }
</style>
