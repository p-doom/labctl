<script lang="ts">
  import { SvelteFlow, Background, Controls, type Node, type Edge } from "@xyflow/svelte";
  import "@xyflow/svelte/dist/style.css";
  import StageNode from "../components/StageNode.svelte";
  import SidePanel from "../components/SidePanel.svelte";
  import Icon from "../components/Icon.svelte";
  import { layout as dagLayout } from "../lib/dag";
  import { router } from "../lib/router.svelte";
  import { panelHistory } from "../lib/panel.svelte";
  import {
    copy,
    editionNumber,
    formatEditionDate,
    formatEditionTime,
    formatRelative,
    formatAbsolute,
  } from "../lib/format";
  import { nowSecs } from "../lib/time.svelte";
  import type { PipelineDetail } from "../lib/types";

  interface Props {
    detail: PipelineDetail;
  }
  let { detail }: Props = $props();

  // Build dag-flow nodes/edges from stages. dependency_on is JSON.
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
    <span class="title-edno">Series {editionNumber(detail.pipeline.id)}</span>
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

  <!-- ============ MASTHEAD ============ -->
  <header class="masthead-block">
    <div class="masthead-line">
      <span class="masthead">Series {editionNumber(detail.pipeline.id)}</span>
      <span class="spacer-dot">·</span>
      <span class="masthead">{formatEditionDate(detail.pipeline.created_at)}</span>
      <span class="spacer-dot">·</span>
      <span class="masthead">{formatEditionTime(detail.pipeline.created_at)}</span>
    </div>

    <h1 class="title-display headline">{detail.pipeline.name}</h1>

    <div class="meta-line">
      <span>{detail.stages.length} {detail.stages.length === 1 ? "stage" : "stages"}</span>
      <span class="spacer-dot">·</span>
      <span class="mono" title={formatAbsolute(detail.pipeline.created_at)}>{formatRelative(detail.pipeline.created_at, nowSecs.value)}</span>
      {#if detail.pipeline.pipeline_path}
        <span class="spacer-dot">·</span>
        <span class="mono src" title={detail.pipeline.pipeline_path}>{detail.pipeline.pipeline_path}</span>
      {/if}
    </div>
  </header>

  <section class="block first">
    <h2 class="section-h masthead">Stages</h2>
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

  <!-- ============ COLOPHON ============ -->
  <footer class="colophon">
    Recorded by labctl
    · series <span class="mono">{detail.pipeline.id}</span>
    <span class="sig">— p(doom)</span>
  </footer>
</SidePanel>

<style>
  .title-edno {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--fg-2);
    font-variant-numeric: tabular-nums;
  }

  /* ============ Masthead ============ */
  .masthead-block {
    padding: 32px 24px 24px;
    border-bottom: 1px solid var(--line-1);
  }
  .masthead-line {
    display: flex;
    align-items: baseline;
    gap: 8px;
    flex-wrap: wrap;
    margin-bottom: 10px;
  }
  .spacer-dot { color: var(--fg-3); font-size: 11px; }
  .title-display {
    font-size: 32px;
    color: var(--fg-0);
    margin: 0;
    line-height: 1.1;
    word-break: break-word;
  }
  .meta-line {
    margin-top: 12px;
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: var(--fg-1);
    flex-wrap: wrap;
  }
  .meta-line .src {
    color: var(--fg-2);
    font-size: 11px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 320px;
  }

  .block { padding: 24px 24px 0; }
  .block.first { padding-top: 24px; padding-bottom: 0; }
  .section-h {
    margin: 0;
    color: var(--fg-2);
  }

  .dag {
    flex: 1;
    min-height: 480px;
    height: calc(100vh - 360px);
    background: var(--bg-0);
  }

  /* ============ Colophon ============ */
  .colophon {
    padding: 24px;
    margin-top: 16px;
    border-top: 1px solid var(--line-1);
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--fg-3);
    line-height: 1.7;
  }
  .colophon .sig {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    font-size: 13px;
    text-transform: none;
    letter-spacing: 0;
    color: var(--fg-2);
    margin-left: 6px;
  }
  .colophon .mono { color: var(--fg-2); text-transform: none; font-weight: 400; letter-spacing: 0.02em; font-family: theme("fontFamily.mono"); }
</style>
