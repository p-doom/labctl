<script lang="ts">
  // The only empty-state shape in the app. Two lines (title + sub).
  // Optional `code` for the inline-monospace hint pattern used in
  // Policies. No illustrations, no CTAs — empty is on purpose.
  import type { Snippet } from "svelte";

  interface Props {
    title: string;
    sub?: Snippet;
  }
  let { title, sub }: Props = $props();
</script>

<div class="empty">
  <p class="title">{title}</p>
  {#if sub}
    <p class="sub">{@render sub()}</p>
  {/if}
</div>

<style>
  .empty {
    padding: var(--empty-pad-y) 24px;
    text-align: center;
    animation: fadeIn 150ms var(--ease);
  }
  .title {
    font-size: 14px;
    color: var(--fg-0);
    margin: 0 0 6px 0;
  }
  .sub {
    font-size: 13px;
    color: var(--fg-2);
    margin: 0 auto;
    max-width: 480px;
  }
  .sub :global(code) {
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: var(--fg-1);
    background: var(--bg-2);
    padding: 1px 5px;
    border-radius: 3px;
  }
  @keyframes fadeIn {
    from { opacity: 0; }
    to { opacity: 1; }
  }
</style>
