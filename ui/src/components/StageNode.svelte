<script lang="ts">
  import { Handle, Position, type NodeProps } from "@xyflow/svelte";
  import Duration from "./Duration.svelte";
  import { statusGroup, shortStatus } from "../lib/format";
  import type { RunSummary } from "../lib/types";

  type Props = NodeProps<{ run: RunSummary; stageName: string }>;
  let { data }: Props = $props();
  let group = $derived(statusGroup(data.run.status));
</script>

<div class="node" data-group={group}>
  <Handle type="target" position={Position.Top} style="opacity: 0; pointer-events: none;" />
  <div class="row1">
    <span class="stage mono">{data.stageName}</span>
    <span class="status mono">{shortStatus(data.run.status)}</span>
  </div>
  <div class="row2 mono">{data.run.recipe_name}</div>
  <div class="row3"><Duration run={data.run} /></div>
  <Handle type="source" position={Position.Bottom} style="opacity: 0; pointer-events: none;" />
</div>

<style>
  .node {
    position: relative;
    background: theme("colors.bg.1");
    border: 1px solid theme("colors.line.1");
    border-radius: 6px;
    padding: 9px 11px;
    width: 200px;
    display: flex;
    flex-direction: column;
    gap: 3px;
    cursor: pointer;
    transition: border-color 150ms cubic-bezier(0.2, 0, 0, 1),
      background 150ms cubic-bezier(0.2, 0, 0, 1);
    font-family: theme("fontFamily.sans");
  }
  .node::before {
    content: "";
    position: absolute;
    top: 9px;
    bottom: 9px;
    left: 0;
    width: 2px;
    border-radius: 0 1px 1px 0;
    background: theme("colors.fg.3");
    transition: background 150ms cubic-bezier(0.2, 0, 0, 1);
  }
  .node[data-group="running"]::before { background: theme("colors.status.running.DEFAULT"); }
  .node[data-group="succeeded"]::before { background: theme("colors.status.succeeded.DEFAULT"); }
  .node[data-group="failed"]::before { background: theme("colors.status.failed.DEFAULT"); }
  .node[data-group="pending"]::before { background: theme("colors.status.pending.DEFAULT"); }
  .node:hover {
    background: theme("colors.bg.2");
    border-color: theme("colors.line.2");
  }
  .row1 {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 8px;
  }
  .stage {
    font-size: 12px;
    color: theme("colors.fg.0");
    letter-spacing: 0.005em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .status {
    font-size: 10px;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: theme("colors.fg.2");
    flex-shrink: 0;
  }
  .node[data-group="running"] .status { color: theme("colors.status.running.fg"); }
  .node[data-group="succeeded"] .status { color: theme("colors.status.succeeded.fg"); }
  .node[data-group="failed"] .status { color: theme("colors.status.failed.fg"); }
  .node[data-group="pending"] .status { color: theme("colors.status.pending.fg"); }
  .row2 {
    font-size: 11px;
    color: theme("colors.fg.1");
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .row3 {
    font-size: 11px;
  }
</style>
