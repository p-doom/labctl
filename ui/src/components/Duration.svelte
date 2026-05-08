<script lang="ts">
  import { liveDuration, formatDuration } from "../lib/format";
  import { nowSecs } from "../lib/time.svelte";
  import type { RunSummary } from "../lib/types";

  interface Props {
    run: RunSummary;
  }
  let { run }: Props = $props();
  let secs = $derived(liveDuration(run, nowSecs.value));
  let label = $derived(formatDuration(secs));
</script>

<span class="dur" class:live={!run.is_terminal}>{label}</span>

<style>
  .dur {
    font-variant-numeric: tabular-nums;
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: theme("colors.fg.1");
  }
  .dur.live {
    color: theme("colors.fg.0");
  }
</style>
