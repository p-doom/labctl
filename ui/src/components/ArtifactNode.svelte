<script lang="ts">
  import { Handle, Position, type NodeProps } from "@xyflow/svelte";
  import type { ArtifactSummary } from "../lib/types";

  type Props = NodeProps<{
    artifact: ArtifactSummary & { is_root?: boolean };
    aliases: string[];
    direction?: "TB" | "LR";
  }>;
  let { data }: Props = $props();
  let dir = $derived(data.direction ?? "LR");
  let displayName = $derived(data.aliases[0] ?? data.artifact.id);
</script>

<div class="anode" class:focal={data.artifact.is_root}>
  <Handle
    type="target"
    position={dir === "LR" ? Position.Left : Position.Top}
    style="opacity: 0; pointer-events: none;"
  />
  <div class="kind">{data.artifact.kind}</div>
  <div class="name mono">{displayName}</div>
  {#if data.aliases.length > 1}
    <div class="extra mono">+{data.aliases.length - 1}</div>
  {/if}
  <Handle
    type="source"
    position={dir === "LR" ? Position.Right : Position.Bottom}
    style="opacity: 0; pointer-events: none;"
  />
</div>

<style>
  .anode {
    position: relative;
    background: theme("colors.bg.1");
    border: 1px solid theme("colors.line.1");
    border-radius: 6px;
    padding: 8px 10px;
    width: 140px;
    cursor: pointer;
    transition: border-color 150ms cubic-bezier(0.2, 0, 0, 1),
      background 150ms cubic-bezier(0.2, 0, 0, 1),
      transform 150ms cubic-bezier(0.2, 0, 0, 1),
      box-shadow 150ms cubic-bezier(0.2, 0, 0, 1);
  }
  .anode::before {
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
  .anode:hover {
    background: theme("colors.bg.2");
    border-color: theme("colors.line.2");
  }
  .anode:hover::before {
    background: theme("colors.accent.dim");
  }
  .anode.focal {
    border-color: theme("colors.accent.dim");
    box-shadow:
      0 0 0 1px theme("colors.accent.dim"),
      0 8px 24px -12px rgba(189, 242, 109, 0.35);
    background: theme("colors.bg.2");
  }
  .anode.focal::before {
    background: theme("colors.accent.DEFAULT");
  }
  .kind {
    font-family: theme("fontFamily.mono");
    font-size: 10px;
    color: theme("colors.fg.3");
    letter-spacing: 0.05em;
    text-transform: uppercase;
    margin-bottom: 4px;
    line-height: 1;
  }
  .anode.focal .kind {
    color: theme("colors.accent.dim");
  }
  .name {
    font-size: 12px;
    color: theme("colors.fg.0");
    line-height: 1.3;
    word-break: break-all;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
  }
  .extra {
    font-size: 10px;
    color: theme("colors.fg.3");
    margin-top: 2px;
    line-height: 1;
  }
</style>
