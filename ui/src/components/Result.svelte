<script lang="ts">
  // Smart wrapper: pull a metric table out of any common eval-output shape.
  // No framework-specific code; recognition is purely structural — see
  // `lib/metrics.ts`. Anything that doesn't match a metric pattern falls
  // through to the JSON tree.
  //
  // A GUI rollout is rendered above the metric table when an artifactId is
  // supplied and the result blob matches one of two shapes:
  //   * single — top-level `traj_path`/`gif_path` (legacy single-instruction)
  //   * multi  — a `runs[]` array, one entry per instruction (each with
  //     index/slug/instruction/subdir). The viewer then shows a task picker.

  import ResultTable from "./ResultTable.svelte";
  import JsonTree from "./JsonTree.svelte";
  import RolloutViewer from "./RolloutViewer.svelte";
  import { extractMetrics } from "../lib/metrics";
  import type { RolloutTask } from "../lib/types";

  interface Props {
    /** The full `metadata.result` blob from an eval_result artifact. */
    result: unknown;
    /** Artifact id — required to fetch rollout frames via the API. */
    artifactId?: string;
    /** Optional controlled task selection for the rollout viewer, passed
     *  straight through (used by the rollout browser to keep the instruction
     *  selected across checkpoint switches). */
    task?: number;
    onTaskChange?: (index: number) => void;
  }
  let { result, artifactId, task, onTaskChange }: Props = $props();

  let metrics = $derived(extractMetrics(result));

  /** Multi-instruction tasks parsed from `result.runs[]`, or null for the
   *  single-rollout (legacy) shape. */
  let rolloutTasks = $derived.by<RolloutTask[] | null>(() => {
    if (typeof result !== "object" || result === null) return null;
    const runs = (result as Record<string, unknown>).runs;
    if (!Array.isArray(runs) || runs.length === 0) return null;
    return runs.map((r, i) => {
      const o = (r ?? {}) as Record<string, unknown>;
      return {
        index: typeof o.index === "number" ? o.index : i,
        slug: typeof o.slug === "string" ? o.slug : `task_${i}`,
        instruction: typeof o.instruction === "string" ? o.instruction : "",
      };
    });
  });

  /** True when the result blob is a single (legacy) GUI rollout. */
  let isSingleRollout = $derived.by(() => {
    if (typeof result !== "object" || result === null) return false;
    const r = result as Record<string, unknown>;
    return typeof r.traj_path === "string" || typeof r.gif_path === "string";
  });

  let isRollout = $derived(!!artifactId && (isSingleRollout || !!rolloutTasks));
</script>

{#if isRollout && artifactId}
  <RolloutViewer {artifactId} tasks={rolloutTasks} {task} {onTaskChange} />
{/if}

{#if metrics}
  <ResultTable tasks={metrics.tasks} primary={metrics.primary} />
{:else if !isRollout}
  <JsonTree value={result} />
{/if}
