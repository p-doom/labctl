<script lang="ts">
  // Stanza empty state. Title set in italic Lora display; sub in Inter
  // body. Two lines. No illustrations, no CTAs — empty is on purpose,
  // and the voice does the work of feeling intentional.
  import type { Snippet } from "svelte";

  interface Props {
    title: string;
    sub?: Snippet;
  }
  let { title, sub }: Props = $props();
</script>

<div class="empty">
  <p class="title headline">{title}</p>
  {#if sub}
    <p class="sub">{@render sub()}</p>
  {/if}
</div>

<style>
  .empty {
    padding: var(--empty-pad-y) 24px;
    text-align: center;
    animation: fadeIn 250ms var(--ease);
  }
  .title {
    font-size: 28px;
    color: var(--fg-0);
    margin: 0 0 14px 0;
    line-height: 1.15;
  }
  .sub {
    font-size: 14px;
    color: var(--fg-1);
    margin: 0 auto;
    max-width: 480px;
    line-height: 1.5;
  }
  .sub :global(code) {
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: var(--fg-0);
    background: var(--bg-2);
    padding: 1px 5px;
    border-radius: 3px;
  }
  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(4px); }
    to { opacity: 1; transform: translateY(0); }
  }
</style>
