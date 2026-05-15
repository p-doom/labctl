<script lang="ts">
  // Top band of a full-bleed detail page (PolicyDetail, Compare, Recipe,
  // Lineage). Back button + label + name + free-form meta + optional
  // right-side actions. One geometry for all four; no view rolls its
  // own header any more.
  import type { Snippet } from "svelte";
  import Icon from "./Icon.svelte";

  interface Props {
    /** Lowercase kind word printed before the name ("policy", "recipe", "lineage"). */
    label: string;
    /** The primary subject (recipe name, policy name, …). Rendered in mono. */
    name: string;
    /** Optional small monospace meta string after the name. */
    meta?: string;
    /** Back-button label and click handler. */
    backLabel: string;
    onBack: () => void;
    /** Optional right-side controls (chips, hop selector, etc). */
    actions?: Snippet;
  }
  let { label, name, meta, backLabel, onBack, actions }: Props = $props();
</script>

<header class="detail-header">
  <button type="button" class="btn-secondary" onclick={onBack} aria-label={`Back to ${backLabel}`}>
    <Icon name="back" size={14} />
    <span>{backLabel}</span>
  </button>
  <div class="title">
    <span class="t-label mono">{label}</span>
    <span class="t-name mono">{name}</span>
    {#if meta}
      <span class="t-meta mono">{meta}</span>
    {/if}
  </div>
  {#if actions}
    <div class="actions">
      {@render actions()}
    </div>
  {:else}
    <span></span>
  {/if}
</header>

<style>
  .detail-header {
    display: grid;
    grid-template-columns: auto 1fr auto;
    align-items: center;
    gap: 16px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--line-0);
    background: var(--bg-0);
    flex-shrink: 0;
  }
  .title {
    display: flex;
    align-items: baseline;
    gap: 10px;
    overflow: hidden;
    min-width: 0;
  }
  .t-label {
    font-size: 11px;
    color: var(--fg-3);
  }
  .t-name {
    font-size: 14px;
    color: var(--fg-0);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }
  .t-meta {
    font-size: 11px;
    color: var(--fg-2);
    flex-shrink: 0;
  }
  .actions {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }
</style>
