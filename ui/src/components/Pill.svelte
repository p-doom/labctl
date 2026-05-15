<script lang="ts">
  import { statusGroup, shortStatus } from "../lib/format";

  interface Props {
    status: string;
    showLabel?: boolean;
    pulse?: boolean;
  }
  let { status, showLabel = true, pulse }: Props = $props();
  let group = $derived(statusGroup(status));
  let label = $derived(shortStatus(status));
  let dotPulse = $derived(pulse ?? (group === "running" || status === "submitted"));
</script>

<span
  class="pill"
  data-group={group}
  class:no-label={!showLabel}
  style="background: var(--soft); color: var(--fg);"
>
  <span
    class="pill-dot"
    class:animate-pulse-dot={dotPulse}
    style="background: var(--dot);"
  ></span>
  {#if showLabel}
    <span>{label}</span>
  {/if}
</span>

<style>
  .pill[data-group="running"] {
    --soft: theme("colors.status.running.soft");
    --fg: theme("colors.status.running.fg");
    --dot: theme("colors.status.running.DEFAULT");
  }
  .pill[data-group="succeeded"] {
    --soft: theme("colors.status.succeeded.soft");
    --fg: theme("colors.status.succeeded.fg");
    --dot: theme("colors.status.succeeded.DEFAULT");
  }
  .pill[data-group="failed"] {
    --soft: theme("colors.status.failed.soft");
    --fg: theme("colors.status.failed.fg");
    --dot: theme("colors.status.failed.DEFAULT");
  }
  .pill[data-group="pending"] {
    --soft: theme("colors.status.pending.soft");
    --fg: theme("colors.status.pending.fg");
    --dot: theme("colors.status.pending.DEFAULT");
  }
  .pill[data-group="neutral"] {
    --soft: theme("colors.status.neutral.soft");
    --fg: theme("colors.status.neutral.fg");
    --dot: theme("colors.status.neutral.DEFAULT");
  }
  .pill.no-label {
    padding: 0;
    background: transparent !important;
  }
</style>
