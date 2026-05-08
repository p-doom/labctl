<script lang="ts">
  // Visual proof-of-life for the SSE stream. No interaction — there's
  // nothing to pause; updates arrive when they arrive. Pulsing dot when
  // the stream is connected; muted when reconnecting.
  import { stream } from "../lib/store.svelte";
</script>

<div
  class="live"
  class:on={stream.connected}
  title={stream.connected ? "Live — receiving updates" : "Reconnecting…"}
>
  <span class="dot" class:animate-pulse-dot={stream.connected}></span>
  <span class="text">{stream.connected ? "live" : "..."}</span>
</div>

<style>
  .live {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.fg.2");
    padding: 4px 8px;
    border-radius: 4px;
  }
  .live.on .text {
    color: theme("colors.accent.dim");
  }
  .dot {
    width: 6px;
    height: 6px;
    border-radius: 999px;
    background: theme("colors.fg.3");
  }
  .live.on .dot {
    background: theme("colors.accent.DEFAULT");
  }
</style>
