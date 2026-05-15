<script lang="ts">
  // Smart wrapper: pull a metric table out of any common eval-output shape.
  // No framework-specific code; recognition is purely structural — see
  // `lib/metrics.ts`. Anything that doesn't match a metric pattern falls
  // through to the JSON tree.
  //
  // If the result contains `traj_path` or `gif_path` (written by
  // osworld_one_task_runner) and an artifactId is supplied, a frame-by-frame
  // RolloutViewer is rendered above the metric table.

  import ResultTable from "./ResultTable.svelte";
  import JsonTree from "./JsonTree.svelte";
  import RolloutViewer from "./RolloutViewer.svelte";
  import { extractMetrics } from "../lib/metrics";

  interface Props {
    /** The full `metadata.result` blob from an eval_result artifact. */
    result: unknown;
    /** Artifact id — required to fetch rollout frames via the API. */
    artifactId?: string;
  }
  let { result, artifactId }: Props = $props();

  let metrics = $derived(extractMetrics(result));

  /** True when the result blob looks like a GUI rollout (has traj_path or
   *  gif_path fields). We only render the viewer when we also have an id. */
  let isRollout = $derived.by(() => {
    if (!artifactId || typeof result !== "object" || result === null) return false;
    const r = result as Record<string, unknown>;
    return typeof r.traj_path === "string" || typeof r.gif_path === "string";
  });
</script>

{#if isRollout && artifactId}
  <RolloutViewer {artifactId} />
{/if}

{#if metrics}
  <ResultTable tasks={metrics.tasks} primary={metrics.primary} />
{:else if !isRollout}
  <JsonTree value={result} />
{/if}
