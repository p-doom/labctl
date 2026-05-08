<script lang="ts">
  import { copy, shortHash } from "../lib/format";

  interface Props {
    value: string;
    n?: number;
    label?: string;
  }
  let { value, n = 8, label }: Props = $props();
  let copied = $state(false);

  function onClick(e: MouseEvent) {
    // Hash buttons live inside clickable list rows. Stop the click from
    // also opening the row's panel.
    e.stopPropagation();
    copy(value);
    copied = true;
    setTimeout(() => (copied = false), 1100);
  }
</script>

<button
  type="button"
  class="hash"
  onclick={onClick}
  title={value}
  aria-label={label ? `Copy ${label}: ${value}` : `Copy ${value}`}
>
  <span class="text">{shortHash(value, n)}</span>
  <span class="badge" class:copied>{copied ? "copied" : "copy"}</span>
</button>

<style>
  .hash {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: theme("colors.fg.1");
    padding: 1px 4px;
    margin: -1px -4px;
    border-radius: 3px;
    background: transparent;
    border: none;
    cursor: pointer;
  }
  .hash:hover {
    background: theme("colors.bg.3");
    color: theme("colors.fg.0");
  }
  .text {
    letter-spacing: 0.01em;
  }
  .badge {
    font-size: 10px;
    color: theme("colors.fg.2");
    opacity: 0;
    transition: opacity 150ms cubic-bezier(0.2, 0, 0, 1);
  }
  .hash:hover .badge {
    opacity: 1;
  }
  .badge.copied {
    color: theme("colors.accent.DEFAULT");
    opacity: 1;
  }
</style>
