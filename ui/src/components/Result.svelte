<script lang="ts">
  // Smart wrapper: pull a metric table out of any common eval-output shape.
  // No framework-specific code; recognition is purely structural — see
  // `lib/metrics.ts`. Anything that doesn't match a metric pattern falls
  // through to the JSON tree.

  import ResultTable from "./ResultTable.svelte";
  import JsonTree from "./JsonTree.svelte";
  import { extractMetrics } from "../lib/metrics";

  interface Props {
    /** The full `metadata.result` blob from an eval_result artifact. */
    result: unknown;
  }
  let { result }: Props = $props();

  let metrics = $derived(extractMetrics(result));
</script>

{#if metrics}
  <ResultTable tasks={metrics.tasks} primary={metrics.primary} />
{:else}
  <JsonTree value={result} />
{/if}
