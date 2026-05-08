<script lang="ts">
  import { Handle, Position, type NodeProps } from "@xyflow/svelte";
  import { statusGroup } from "../lib/format";
  import type { RunSummary } from "../lib/types";

  type Props = NodeProps<{ run: RunSummary; direction?: "TB" | "LR" }>;
  let { data }: Props = $props();
  let dir = $derived(data.direction ?? "LR");
  let group = $derived(statusGroup(data.run.status));
</script>

<div class="rnode" data-group={group}>
  <Handle
    type="target"
    position={dir === "LR" ? Position.Left : Position.Top}
    style="opacity: 0; pointer-events: none;"
  />
  <div class="row1">
    <span class="dot"></span>
    <span class="recipe mono">{data.run.recipe_name}</span>
  </div>
  {#if data.run.stage_name}
    <div class="stage mono">{data.run.stage_name}</div>
  {/if}
  <Handle
    type="source"
    position={dir === "LR" ? Position.Right : Position.Bottom}
    style="opacity: 0; pointer-events: none;"
  />
</div>

<style>
  .rnode {
    position: relative;
    background: theme("colors.bg.2");
    border: 1px solid theme("colors.line.1");
    border-radius: 6px;
    padding: 8px 10px;
    width: 140px;
    cursor: pointer;
    transition: border-color 150ms cubic-bezier(0.2, 0, 0, 1),
      background 150ms cubic-bezier(0.2, 0, 0, 1);
  }
  .rnode::before {
    content: "";
    position: absolute;
    top: 8px;
    bottom: 8px;
    left: 0;
    width: 2px;
    border-radius: 0 1px 1px 0;
    background: theme("colors.fg.3");
    transition: background 150ms cubic-bezier(0.2, 0, 0, 1);
  }
  .rnode[data-group="running"]::before { background: theme("colors.status.running.DEFAULT"); }
  .rnode[data-group="succeeded"]::before { background: theme("colors.status.succeeded.DEFAULT"); }
  .rnode[data-group="failed"]::before { background: theme("colors.status.failed.DEFAULT"); }
  .rnode[data-group="pending"]::before { background: theme("colors.status.pending.DEFAULT"); }
  .rnode:hover {
    background: theme("colors.bg.3");
    border-color: theme("colors.line.2");
  }
  .row1 {
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .dot {
    width: 6px;
    height: 6px;
    border-radius: 999px;
    background: theme("colors.fg.3");
    flex-shrink: 0;
  }
  .rnode[data-group="running"] .dot { background: theme("colors.status.running.DEFAULT"); }
  .rnode[data-group="succeeded"] .dot { background: theme("colors.status.succeeded.DEFAULT"); }
  .rnode[data-group="failed"] .dot { background: theme("colors.status.failed.DEFAULT"); }
  .rnode[data-group="pending"] .dot { background: theme("colors.status.pending.DEFAULT"); }
  .recipe {
    font-size: 11px;
    color: theme("colors.fg.0");
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    flex: 1;
  }
  .stage {
    font-size: 10px;
    color: theme("colors.fg.2");
    margin-top: 2px;
    padding-left: 12px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
