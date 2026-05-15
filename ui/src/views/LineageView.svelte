<script lang="ts">
  import {
    SvelteFlow,
    Background,
    BackgroundVariant,
    MiniMap,
    type Node,
    type Edge,
  } from "@xyflow/svelte";
  import "@xyflow/svelte/dist/style.css";

  import { store, loadLineage } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { layout as dagLayout } from "../lib/dag";

  import ArtifactNode from "../components/ArtifactNode.svelte";
  import RunDagNode from "../components/RunDagNode.svelte";
  import DetailHeader from "../components/DetailHeader.svelte";

  interface Props {
    artifactId: string;
  }
  let { artifactId }: Props = $props();

  let lineage = $derived(store.lineage(artifactId));
  let error = $state<string | null>(null);
  let hopLimit = $state<number | null>(null); // null = all hops
  let hovered = $state<string | null>(null);

  const NODE_W = 140;
  const NODE_H = 64;

  $effect(() => {
    if (!artifactId) return;
    loadLineage(artifactId).then(() => {
      error = null;
    }).catch((e) => {
      error = e instanceof Error ? e.message : String(e);
    });
  });

  // Adjacency for ancestry walks. Built once per lineage payload.
  let adjacency = $derived.by(() => {
    if (!lineage) return { incoming: new Map<string, Set<string>>(), outgoing: new Map<string, Set<string>>() };
    const incoming = new Map<string, Set<string>>();
    const outgoing = new Map<string, Set<string>>();
    for (const e of lineage.edges) {
      if (!outgoing.has(e.from)) outgoing.set(e.from, new Set());
      outgoing.get(e.from)!.add(e.to);
      if (!incoming.has(e.to)) incoming.set(e.to, new Set());
      incoming.get(e.to)!.add(e.from);
    }
    return { incoming, outgoing };
  });

  // Hop-distance from focal node, separately upstream/downstream. -1 means
  // "unreachable" (shouldn't happen in this dataset).
  let hopDistance = $derived.by(() => {
    if (!lineage) return new Map<string, number>();
    const dist = new Map<string, number>();
    dist.set(lineage.root_id, 0);
    const bfs = (start: string, side: "incoming" | "outgoing") => {
      const adj = side === "incoming" ? adjacency.incoming : adjacency.outgoing;
      const queue: [string, number][] = [[start, 0]];
      while (queue.length) {
        const [n, d] = queue.shift()!;
        for (const next of adj.get(n) ?? []) {
          const existing = dist.get(next);
          if (existing == null || existing > d + 1) {
            dist.set(next, d + 1);
            queue.push([next, d + 1]);
          }
        }
      }
    };
    bfs(lineage.root_id, "incoming");
    bfs(lineage.root_id, "outgoing");
    return dist;
  });

  // Apply hop filter — drop nodes/edges beyond the limit.
  let filtered = $derived.by(() => {
    if (!lineage) return null;
    if (hopLimit == null) return lineage;
    const keep = new Set<string>();
    for (const [id, d] of hopDistance) {
      if (d <= hopLimit) keep.add(id);
    }
    return {
      ...lineage,
      artifacts: lineage.artifacts.filter((a) => keep.has(a.id)),
      runs: lineage.runs.filter((r) => keep.has(r.id)),
      edges: lineage.edges.filter((e) => keep.has(e.from) && keep.has(e.to)),
    };
  });

  // Node + edge arrays. Position via dagre, L→R orientation.
  let nodes = $state<Node[]>([]);
  let edges = $state<Edge[]>([]);

  // Cached layout — only depends on the filtered topology, not the hover.
  // Hover state is composited in afterwards as opacity, so dragging the
  // mouse around doesn't trigger dagre re-runs.
  let positions = $derived.by(() => {
    if (!filtered) return new Map<string, { id: string; x: number; y: number }>();
    const incoming = new Map<string, string[]>();
    for (const e of filtered.edges) {
      const arr = incoming.get(e.to) ?? [];
      arr.push(e.from);
      incoming.set(e.to, arr);
    }
    const allIds = [
      ...filtered.artifacts.map((a) => a.id),
      ...filtered.runs.map((r) => r.id),
    ];
    return dagLayout(
      allIds.map((id) => ({ id, width: NODE_W, height: NODE_H, parents: incoming.get(id) ?? [] })),
      { direction: "LR", nodesep: 24, ranksep: 80 },
    );
  });

  $effect(() => {
    if (!filtered) {
      nodes = [];
      edges = [];
      return;
    }
    const dimNodes = highlightSet != null;

    const nArr: Node[] = [];
    for (const a of filtered.artifacts) {
      const p = positions.get(a.id) ?? { id: a.id, x: 0, y: 0 };
      const dim = dimNodes && !highlightSet!.has(a.id);
      nArr.push({
        id: a.id,
        type: "artifact",
        position: { x: p.x, y: p.y },
        data: { artifact: a, aliases: a.aliases ?? [], direction: "LR" },
        style: dim ? "opacity: 0.16;" : "opacity: 1;",
      });
    }
    for (const r of filtered.runs) {
      const p = positions.get(r.id) ?? { id: r.id, x: 0, y: 0 };
      const dim = dimNodes && !highlightSet!.has(r.id);
      nArr.push({
        id: r.id,
        type: "run",
        position: { x: p.x, y: p.y },
        data: { run: r, direction: "LR" },
        style: dim ? "opacity: 0.16;" : "opacity: 1;",
      });
    }
    const eArr: Edge[] = filtered.edges.map((e, i) => {
      const onPath = highlightSet ? highlightSet.has(e.from) && highlightSet.has(e.to) : false;
      const dim = dimNodes && !onPath;
      const dashed = e.kind === "consumed_by";
      const stroke = onPath ? "var(--edge-on)" : "var(--edge-off)";
      const dashStyle = dashed ? "stroke-dasharray: 4 3;" : "";
      const opacityStyle = dim ? "opacity: 0.10;" : "opacity: 1;";
      return {
        id: `${e.from}-${e.to}-${i}`,
        source: e.from,
        target: e.to,
        type: "smoothstep",
        style: `stroke: ${stroke}; stroke-width: ${onPath ? 1.6 : 1.3}px; ${dashStyle} ${opacityStyle}`,
        data: { kind: e.kind, role: e.role },
      };
    });
    nodes = nArr;
    edges = eArr;
  });

  // Compute the highlighted set when a node is hovered: itself + all
  // ancestors + all descendants.
  let highlightSet = $derived.by(() => {
    if (!hovered || !lineage) return null;
    const set = new Set<string>([hovered]);
    const walk = (start: string, dir: "incoming" | "outgoing") => {
      const adj = dir === "incoming" ? adjacency.incoming : adjacency.outgoing;
      const stack = [start];
      while (stack.length) {
        const n = stack.pop()!;
        for (const next of adj.get(n) ?? []) {
          if (!set.has(next)) {
            set.add(next);
            stack.push(next);
          }
        }
      }
    };
    walk(hovered, "incoming");
    walk(hovered, "outgoing");
    return set;
  });

  function onNodeMouseEnter({ node }: { node: Node }) {
    hovered = node.id;
  }
  function onNodeMouseLeave() {
    hovered = null;
  }
  function onNodeClick({ node }: { node: Node }) {
    if (node.id === lineage?.root_id) return; // don't navigate away from focal
    if (node.type === "run") router.go("runs", node.id);
    else if (node.type === "artifact") router.go("artifacts", node.id);
  }

  function setHopLimit(n: number | null) {
    hopLimit = n;
  }

  function close() {
    router.go("artifacts", artifactId);
  }

  // Keyboard: Esc closes back to the artifact panel.
  function onKey(e: KeyboardEvent) {
    if (e.key === "Escape") {
      const target = e.target as HTMLElement | null;
      if (target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA")) return;
      e.preventDefault();
      close();
    }
  }
  $effect(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  let focalArtifact = $derived(
    lineage?.artifacts.find((a) => a.id === lineage!.root_id) ?? null,
  );
  let focalLabel = $derived(
    focalArtifact ? (focalArtifact.aliases?.[0] ?? focalArtifact.id) : artifactId,
  );

  let highlightAttr = $derived(highlightSet ? "true" : "false");
</script>

<div class="lineage" data-highlighting={highlightAttr}>
  <DetailHeader
    label="lineage"
    name={focalLabel}
    meta={focalArtifact?.kind}
    backLabel="Artifact"
    onBack={close}
  >
    {#snippet actions()}
      <div class="hops" role="group" aria-label="Hop limit">
        <span class="h-label mono">hops</span>
        <button type="button" class:active={hopLimit === 1} onclick={() => setHopLimit(1)}>1</button>
        <button type="button" class:active={hopLimit === 3} onclick={() => setHopLimit(3)}>3</button>
        <button type="button" class:active={hopLimit === 5} onclick={() => setHopLimit(5)}>5</button>
        <button type="button" class:active={hopLimit === null} onclick={() => setHopLimit(null)}>all</button>
      </div>
    {/snippet}
  </DetailHeader>

  {#if error}
    <div class="error">{error}</div>
  {:else if !lineage}
    <div class="loading">
      <div class="skel" style="width: 320px; height: 64px"></div>
    </div>
  {:else}
    <div class="canvas">
      <SvelteFlow
        bind:nodes
        bind:edges
        nodeTypes={{ artifact: ArtifactNode, run: RunDagNode }}
        fitView
        fitViewOptions={{ padding: 0.25, minZoom: 0.4, maxZoom: 1.5 }}
        minZoom={0.2}
        maxZoom={2}
        panOnDrag
        zoomOnScroll
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable
        proOptions={{ hideAttribution: true }}
        onnodeclick={onNodeClick}
        onnodepointerenter={onNodeMouseEnter}
        onnodepointerleave={onNodeMouseLeave}
      >
        <Background variant={BackgroundVariant.Dots} gap={24} size={1} bgColor="#0a0b0d" />
        <MiniMap
          pannable
          zoomable
          nodeColor={(n) => (n.type === "artifact" ? "#bdf26d66" : "#7c828d66")}
          maskColor="rgba(10, 11, 13, 0.65)"
        />
      </SvelteFlow>

      <div class="legend mono">
        <span class="leg"><span class="line solid"></span>produces</span>
        <span class="leg"><span class="line dashed"></span>consumed by</span>
        <span class="leg"><span class="dot a"></span>artifact</span>
        <span class="leg"><span class="dot r"></span>run</span>
        <span class="hint">hover · drag · scroll-zoom · esc closes</span>
      </div>
    </div>
  {/if}
</div>

<style>
  .lineage {
    display: flex;
    flex-direction: column;
    height: 100%;
    background: var(--bg-0);
    overflow: hidden;
    --edge-off: var(--line-2);
    --edge-on: var(--accent-dim);
  }
  .hops {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    padding: 2px;
    background: var(--bg-1);
    border: 1px solid var(--line-1);
    border-radius: 4px;
  }
  .hops .h-label {
    font-size: 10px;
    color: var(--fg-3);
    letter-spacing: 0.05em;
    text-transform: uppercase;
    padding: 0 6px;
  }
  .hops button {
    background: transparent;
    border: none;
    color: var(--fg-1);
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    padding: 3px 9px;
    border-radius: 3px;
    cursor: pointer;
  }
  .hops button:hover { color: var(--fg-0); }
  .hops button.active {
    background: var(--bg-3);
    color: var(--fg-0);
  }

  .canvas {
    flex: 1;
    position: relative;
    min-height: 0;
  }
  .canvas :global(.svelte-flow) {
    background: theme("colors.bg.0");
  }
  .canvas :global(.svelte-flow__node) {
    transition: opacity 150ms cubic-bezier(0.2, 0, 0, 1);
  }
  .canvas :global(.svelte-flow__edge) {
    transition: opacity 150ms cubic-bezier(0.2, 0, 0, 1);
  }

  /* Override xyflow background dot color to be subtle, not the default loud one */
  .canvas :global(.svelte-flow__background) {
    background-color: theme("colors.bg.0");
  }
  .canvas :global(.react-flow__background-pattern),
  .canvas :global(.svelte-flow__background-pattern) {
    color: theme("colors.line.0");
  }

  /* MiniMap overrides */
  .canvas :global(.svelte-flow__minimap) {
    background: theme("colors.bg.1") !important;
    border: 1px solid theme("colors.line.1") !important;
    border-radius: 6px !important;
    bottom: 16px !important;
    right: 16px !important;
    width: 180px !important;
    height: 110px !important;
  }
  .canvas :global(.svelte-flow__minimap-mask) {
    fill: rgba(10, 11, 13, 0.5);
  }

  /* Edge stroke + label refinements */
  .canvas :global(.svelte-flow__edge-path) {
    stroke: theme("colors.line.2");
    stroke-width: 1.4px;
  }

  .legend {
    position: absolute;
    bottom: 16px;
    left: 16px;
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 6px 10px;
    background: theme("colors.bg.1");
    border: 1px solid theme("colors.line.1");
    border-radius: 6px;
    font-size: 10px;
    color: theme("colors.fg.2");
    pointer-events: none;
    letter-spacing: 0.03em;
  }
  .leg { display: inline-flex; align-items: center; gap: 6px; }
  .leg .line { display: inline-block; width: 16px; height: 1px; background: theme("colors.line.2"); }
  .leg .line.dashed {
    background: transparent;
    border-top: 1px dashed theme("colors.line.2");
    height: 0;
  }
  .leg .dot { width: 8px; height: 8px; border-radius: 2px; }
  .leg .dot.a { background: theme("colors.accent.dim"); }
  .leg .dot.r { background: theme("colors.fg.2"); border-radius: 999px; }
  .legend .hint {
    margin-left: 12px;
    color: theme("colors.fg.3");
  }

  .loading, .error {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    color: theme("colors.fg.2");
    font-size: 13px;
  }
</style>
