<script lang="ts">
  import { nowSecs } from "../lib/time.svelte";
  import { formatRelative, formatAbsolute } from "../lib/format";

  interface Props {
    ts: number;
  }
  let { ts }: Props = $props();
  let label = $derived(formatRelative(ts, nowSecs.value));
  let abs = $derived(formatAbsolute(ts));
</script>

<span class="rel" title={abs}>{label}</span>

<style>
  .rel {
    font-variant-numeric: tabular-nums;
    color: theme("colors.fg.1");
  }
</style>
