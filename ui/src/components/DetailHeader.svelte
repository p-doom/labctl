<script lang="ts">
  // Stanza detail header. The shared top band on full-bleed views
  // (PolicyDetail, Compare, Recipe, Lineage). Renders as a masthead:
  // micro-caps label + italic-Lora subject name + optional meta. The
  // back button sits to the left; optional actions to the right.
  import type { Snippet } from "svelte";
  import Icon from "./Icon.svelte";

  interface Props {
    /** Lowercase kind word, shown in micro-caps before the name. */
    label: string;
    /** The primary subject (recipe / policy / specimen / pipeline name). */
    name: string;
    /** Optional small meta string after the name. */
    meta?: string;
    /** Back-button label and click handler. */
    backLabel: string;
    onBack: () => void;
    /** Optional right-side controls. */
    actions?: Snippet;
  }
  let { label, name, meta, backLabel, onBack, actions }: Props = $props();
</script>

<header class="detail-header">
  <button type="button" class="back" onclick={onBack} aria-label={`Back to ${backLabel}`}>
    <Icon name="back" size={12} />
    <span>{backLabel}</span>
  </button>
  <div class="title">
    <span class="t-label masthead">{label}</span>
    <h1 class="t-name">{name}</h1>
    {#if meta}
      <span class="t-meta masthead">{meta}</span>
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
    gap: 20px;
    padding: 16px 24px;
    border-bottom: 1px solid var(--line-1);
    background: var(--bg-0);
    flex-shrink: 0;
  }
  .back {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: transparent;
    border: none;
    padding: 4px 0;
    color: var(--fg-2);
    cursor: pointer;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    font-family: inherit;
    transition: color var(--dur-micro) var(--ease);
  }
  .back:hover { color: var(--fg-0); }
  .title {
    display: flex;
    align-items: baseline;
    gap: 14px;
    overflow: hidden;
    min-width: 0;
  }
  .t-label {
    flex-shrink: 0;
  }
  /* Subject name in italic Lora — the visual hero of the header. */
  .t-name {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    font-size: 22px;
    color: var(--fg-0);
    letter-spacing: -0.015em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
    margin: 0;
    line-height: 1.1;
    font-feature-settings: normal;
  }
  .t-meta {
    flex-shrink: 0;
    color: var(--fg-3);
  }
  .actions {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }
</style>
