// dagre-backed layout for both the pipeline DAG (T→B) and the artifact
// lineage graph (L→R). Naive topo-sort is gone — dagre does crossing
// minimization, edge routing, and proper layered placement.

import dagre from "@dagrejs/dagre";

export interface Positioned {
  id: string;
  x: number;
  y: number;
}

interface NodeSize {
  id: string;
  width: number;
  height: number;
  parents: string[];
}

export type Direction = "TB" | "LR";

export interface LayoutOptions {
  direction?: Direction;
  /** Spacing between nodes in the same rank. */
  nodesep?: number;
  /** Spacing between ranks (the "flow" axis). */
  ranksep?: number;
  /** Spacing inside each rank. */
  edgesep?: number;
}

/**
 * Layered DAG layout. Pass node sizes — dagre uses them to compute
 * non-overlapping positions. Coordinates returned are CENTER points (xyflow
 * expects top-left, so callers shift by width/2, height/2).
 */
export function layout(nodes: NodeSize[], opts: LayoutOptions = {}): Map<string, Positioned> {
  const direction = opts.direction ?? "LR";
  const g = new dagre.graphlib.Graph({ compound: false });
  g.setGraph({
    rankdir: direction,
    nodesep: opts.nodesep ?? 24,
    ranksep: opts.ranksep ?? 80,
    edgesep: opts.edgesep ?? 16,
    marginx: 16,
    marginy: 16,
    align: "UL",
    ranker: "tight-tree",
  });
  g.setDefaultEdgeLabel(() => ({}));

  for (const n of nodes) {
    g.setNode(n.id, { width: n.width, height: n.height });
  }
  for (const n of nodes) {
    for (const p of n.parents) {
      if (g.hasNode(p)) {
        g.setEdge(p, n.id);
      }
    }
  }
  dagre.layout(g);

  const out = new Map<string, Positioned>();
  for (const n of nodes) {
    const pos = g.node(n.id);
    if (!pos) continue;
    // dagre returns center coordinates; shift to top-left for xyflow.
    out.set(n.id, {
      id: n.id,
      x: pos.x - n.width / 2,
      y: pos.y - n.height / 2,
    });
  }
  return out;
}
