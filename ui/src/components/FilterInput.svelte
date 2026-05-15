<script lang="ts">
  // Async-commit filter input — Linear-style.
  //
  // The displayed input value updates synchronously on every keystroke,
  // so typing is *always* paint-fast. The parent's `onInput` callback —
  // which triggers the expensive filter + render — is deferred to a
  // *separate task* via MessageChannel (not rAF, which fires before
  // paint and would still block the input's render). Multiple
  // keystrokes batch into one onInput call.
  //
  // Net result: type → input paints (frame N) → list updates (frame N+1
  // or later). User sees their character immediately; the list catches
  // up one frame behind without ever being on the keystroke's critical
  // path.
  import { onDestroy } from "svelte";
  import { postTask } from "../lib/post-task";
  import Icon from "./Icon.svelte";

  interface Props {
    value: string;
    placeholder: string;
    inputRef?: HTMLInputElement | null;
    onInput: (v: string) => void;
    onEnter?: () => void;
  }
  let {
    value,
    placeholder,
    inputRef = $bindable(null),
    onInput,
    onEnter,
  }: Props = $props();

  // What the input element actually shows. Owned by this component; the
  // parent's `value` prop is treated as the initial value plus a hook
  // for programmatic resets (which we surface via `lastEmitted`).
  let displayed = $state(value);
  let pending = "";
  let taskPending = false;
  // Last value we sent to the parent. Lets us distinguish parent-driven
  // resets (which should sync into `displayed`) from our own emits
  // (which shouldn't override mid-typing).
  let lastEmitted = value;

  $effect(() => {
    // External reset path: parent set value to something we didn't emit
    // and there's no pending keystroke. Sync the display.
    if (value !== lastEmitted && !taskPending && value !== displayed) {
      displayed = value;
      lastEmitted = value;
    }
  });

  function schedule(v: string) {
    displayed = v;
    pending = v;
    if (taskPending) return;
    taskPending = true;
    postTask(() => {
      taskPending = false;
      lastEmitted = pending;
      onInput(pending);
    });
  }

  function flush() {
    if (!taskPending) return;
    taskPending = false;
    lastEmitted = pending;
    onInput(pending);
  }

  function clearAndEmit() {
    taskPending = false;
    displayed = "";
    pending = "";
    lastEmitted = "";
    onInput("");
  }

  onDestroy(() => {
    taskPending = false;
  });
</script>

<div class="text-filter" class:has-value={!!displayed}>
  <Icon name="search" size={12} />
  <input
    bind:this={inputRef}
    type="text"
    {placeholder}
    value={displayed}
    oninput={(e) => schedule((e.currentTarget as HTMLInputElement).value)}
    onkeydown={(e) => {
      const el = e.currentTarget as HTMLInputElement;
      if (e.key === "Escape") {
        // Clear synchronously; let Esc bubble so SidePanel's handler
        // can close any open panel after the input is cleared.
        clearAndEmit();
        el.blur();
      } else if (e.key === "Enter" && onEnter) {
        e.preventDefault();
        // Commit any pending keystroke so onEnter acts on the latest
        // filter result, not the one from the previous frame.
        flush();
        onEnter();
        // Surrender focus so subsequent Esc reaches the freshly-opened
        // side panel instead of being eaten by this input.
        el.blur();
      }
    }}
    autocomplete="off"
    spellcheck="false"
  />
  {#if displayed}
    <button
      type="button"
      class="clear-btn"
      onclick={() => { clearAndEmit(); inputRef?.focus(); }}
      aria-label="Clear filter"
    ><Icon name="close" size={10} /></button>
  {/if}
</div>

<style>
  .text-filter {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    margin-left: auto;
    padding: 3px 8px;
    background: var(--bg-1);
    border: 1px solid var(--line-1);
    border-radius: 4px;
    color: var(--fg-2);
    min-width: 160px;
    flex-shrink: 0;
    transition:
      border-color var(--dur-micro) var(--ease),
      color var(--dur-micro) var(--ease);
  }
  .text-filter:focus-within,
  .text-filter.has-value {
    border-color: var(--line-2);
    color: var(--fg-1);
  }
  .text-filter input {
    flex: 1;
    background: transparent;
    border: none;
    color: var(--fg-0);
    font-size: 12px;
    font-family: inherit;
    outline: none;
    min-width: 0;
  }
  .text-filter input::placeholder { color: var(--fg-3); }
  .clear-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    background: transparent;
    border: none;
    color: var(--fg-3);
    cursor: pointer;
    padding: 0;
    line-height: 1;
  }
  .clear-btn:hover { color: var(--fg-1); }
</style>
