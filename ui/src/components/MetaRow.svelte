<script lang="ts">
  // The canonical key/value row used in side panels. One geometry (96px
  // label column, mono value), one source of truth. Path values get
  // ellipsis-on-overflow; everything else wraps naturally.
  import type { Snippet } from "svelte";

  interface Props {
    label: string;
    /** When set, value is treated as a long filesystem path: monospace,
     *  single-line, truncated with ellipsis, full text in title attr. */
    path?: string;
    /** Free-form content; either pass `path` or this snippet, not both. */
    children?: Snippet;
  }
  let { label, path, children }: Props = $props();
</script>

<div class="meta-row">
  <span class="k">{label}</span>
  {#if path !== undefined}
    <span class="v path" title={path}>{path}</span>
  {:else if children}
    <span class="v">{@render children()}</span>
  {/if}
</div>

<style>
  .meta-row {
    display: grid;
    grid-template-columns: var(--meta-label-w) 1fr;
    align-items: baseline;
    padding: 4px 0;
    font-size: 13px;
    min-width: 0;
  }
  .k {
    font-size: 12px;
    color: var(--fg-1);
  }
  .v {
    font-size: 12px;
    color: var(--fg-1);
    min-width: 0;
  }
  .v.path {
    font-family: theme("fontFamily.mono");
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
