<script lang="ts">
  import { SvelteFlow, Background, Controls, type Node, type Edge } from "@xyflow/svelte";
  import "@xyflow/svelte/dist/style.css";
  import StageNode from "../components/StageNode.svelte";
  import SidePanel from "../components/SidePanel.svelte";
  import Pill from "../components/Pill.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Hash from "../components/Hash.svelte";
  import Icon from "../components/Icon.svelte";
  import MetaRow from "../components/MetaRow.svelte";
  import { layout as dagLayout } from "../lib/dag";
  import { router } from "../lib/router.svelte";
  import { panelHistory } from "../lib/panel.svelte";
  import { copy } from "../lib/format";
  import type { PipelineDetail } from "../lib/types";

  interface Props {
    detail: PipelineDetail;
  }
  let { detail }: Props = $props();

  // Build dag-flow nodes/edges from stages. dependency_on is JSON: { stage_name: parent_run_id }
  const NODE_W = 200;
  const NODE_H = 76;
  let layout = $derived.by(() => {
    const nodes = detail.stages.map((s) => ({
      id: s.stage_name ?? s.id,
      width: NODE_W,
      height: NODE_H,
      parents: extractParents(s.dependency_on),
    }));
    return dagLayout(nodes, { direction: "TB", nodesep: 32, ranksep: 64 });
  });

  function extractParents(dep: unknown): string[] {
    if (!dep || typeof dep !== "object") return [];
    return Object.keys(dep as Record<string, unknown>);
  }

  let nodes = $state<Node[]>([]);
  let edges = $state<Edge[]>([]);

  $effect(() => {
    const nodeArr: Node[] = detail.stages.map((s) => {
      const id = s.stage_name ?? s.id;
      const p = layout.get(id) ?? { id, x: 0, y: 0 };
      return {
        id,
        type: "stage",
        position: { x: p.x, y: p.y },
        data: { run: s, stageName: s.stage_name ?? "(unnamed)" },
      };
    });
    const edgeArr: Edge[] = [];
    for (const s of detail.stages) {
      const to = s.stage_name ?? s.id;
      for (const p of extractParents(s.dependency_on)) {
        edgeArr.push({
          id: `${p}->${to}`,
          source: p,
          target: to,
          type: "smoothstep",
          animated: !s.is_terminal,
        });
      }
    }
    nodes = nodeArr;
    edges = edgeArr;
  });

  function close() {
    router.select("pipelines", null);
  }

  function onNodeClick({ node }: { node: Node }) {
    const stage = detail.stages.find((s) => (s.stage_name ?? s.id) === node.id);
    if (stage) router.go("runs", stage.id);
  }
</script>

<SidePanel
  onClose={close}
  onBack={panelHistory.back}
  onForward={panelHistory.forward}
  canBack={panelHistory.canBack}
  canForward={panelHistory.canForward}
>
  {#snippet title()}
    <div class="title-row">
      <Pill status={detail.pipeline.status} />
      <span class="name mono">{detail.pipeline.name}</span>
    </div>
  {/snippet}
  {#snippet actions()}
    <button
      type="button"
      class="iconbtn"
      onclick={() => copy(detail.pipeline.id)}
      title="Copy pipeline id"
      aria-label="Copy pipeline id"
    >
      <Icon name="copy" />
    </button>
  {/snippet}

  <section class="meta">
    <MetaRow label="id"><Hash value={detail.pipeline.id} n={20} /></MetaRow>
    <MetaRow label="stages">
      <span class="mono">{detail.stages.length}</span>
    </MetaRow>
    <MetaRow label="created"><RelativeTime ts={detail.pipeline.created_at} /></MetaRow>
    {#if detail.pipeline.pipeline_path}
      <MetaRow label="source" path={detail.pipeline.pipeline_path} />
    {/if}
  </section>

  <div class="dag">
    <SvelteFlow
      bind:nodes
      bind:edges
      nodeTypes={{ stage: StageNode }}
      fitView
      fitViewOptions={{ padding: 0.2 }}
      panOnDrag
      zoomOnScroll
      onnodeclick={onNodeClick}
      proOptions={{ hideAttribution: true }}
    >
      <Background />
      <Controls showLock={false} />
    </SvelteFlow>
  </div>
</SidePanel>

<style>
  .title-row { display: flex; align-items: center; gap: 10px; }
  .name { font-size: 14px; color: var(--fg-0); }
  .meta { padding: 12px 16px 4px 16px; border-bottom: 1px solid var(--line-0); }
  .dag {
    flex: 1;
    min-height: 480px;
    height: calc(100vh - 240px);
    background: theme("colors.bg.0");
  }
</style>
