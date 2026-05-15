<script lang="ts">
  import { onMount, type Snippet } from "svelte";
  import Icon from "./Icon.svelte";

  interface Props {
    title?: Snippet;
    actions?: Snippet;
    children: Snippet;
    onClose: () => void;
    onBack?: () => void;
    onForward?: () => void;
    canBack?: boolean;
    canForward?: boolean;
  }
  let { title, actions, children, onClose, onBack, onForward, canBack, canForward }: Props =
    $props();

  function onKey(e: KeyboardEvent) {
    if (e.key !== "Escape") return;
    // Don't hijack Esc inside text inputs — they own that key for clearing
    // their own content (e.g. FilterInput on the underlying list view).
    const t = e.target as HTMLElement | null;
    if (t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.isContentEditable)) return;
    e.preventDefault();
    // Esc always closes. Back-through-history is reserved for the
    // explicit back button (and Cmd+[) — using Esc for it surprised
    // people whose stack contained a stale entry from a prior session.
    onClose();
  }

  onMount(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });
</script>

<button type="button" class="scrim" onclick={onClose} aria-label="Close panel"></button>
<div class="panel" role="dialog" aria-modal="false">
  <header>
    <div class="nav">
      {#if onBack}
        <button
          type="button"
          class="iconbtn"
          onclick={onBack}
          disabled={!canBack}
          aria-label="Back"
          title="Back"
        >
          <Icon name="back" />
        </button>
      {/if}
      {#if onForward}
        <button
          type="button"
          class="iconbtn"
          onclick={onForward}
          disabled={!canForward}
          aria-label="Forward"
          title="Forward"
        >
          <Icon name="forward" />
        </button>
      {/if}
    </div>
    <div class="title">
      {#if title}
        {@render title()}
      {/if}
    </div>
    <div class="actions">
      {#if actions}
        {@render actions()}
      {/if}
      <button type="button" class="iconbtn" onclick={onClose} aria-label="Close" title="Close (Esc)">
        <Icon name="close" />
      </button>
    </div>
  </header>
  <div class="body">
    {@render children()}
  </div>
</div>

<style>
  .scrim {
    position: fixed;
    inset: 0;
    background: var(--scrim);
    z-index: 40;
    animation: fadeIn var(--dur-micro) var(--ease);
    border: none;
    cursor: default;
    padding: 0;
  }
  @keyframes fadeIn {
    from { opacity: 0; }
    to { opacity: 1; }
  }
  .panel {
    position: fixed;
    top: 0;
    right: 0;
    bottom: 0;
    width: min(680px, 60vw);
    background: var(--bg-1);
    border-left: 1px solid var(--line-1);
    box-shadow: theme("boxShadow.panel");
    z-index: 41;
    display: flex;
    flex-direction: column;
    animation: slideIn var(--dur-panel) var(--ease);
  }
  @keyframes slideIn {
    from { transform: translateX(8%); opacity: 0; }
    to { transform: translateX(0); opacity: 1; }
  }
  header {
    display: grid;
    grid-template-columns: auto 1fr auto;
    align-items: center;
    gap: 12px;
    padding: 10px 14px;
    border-bottom: 1px solid var(--line-0);
    background: var(--bg-1);
    flex-shrink: 0;
  }
  .nav {
    display: flex;
    gap: 2px;
  }
  .title {
    overflow: hidden;
  }
  .actions {
    display: flex;
    gap: 2px;
    align-items: center;
  }
  .body {
    flex: 1;
    overflow-y: auto;
    overflow-x: hidden;
  }
</style>
