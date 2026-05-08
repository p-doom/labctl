<script lang="ts">
  import type { Snippet } from "svelte";
  interface Props {
    children: Snippet;
  }
  let { children }: Props = $props();
</script>

<div class="filterbar">
  {@render children()}
</div>

<style>
  .filterbar {
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 12px 16px 10px 16px;
    border-bottom: 1px solid theme("colors.line.0");
    background: theme("colors.bg.0");
    flex-shrink: 0;
  }
</style>
