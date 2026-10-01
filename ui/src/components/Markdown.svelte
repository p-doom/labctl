<script lang="ts">
  // Rendered Markdown with an optional "source" toggle, so the exact bytes
  // an agent receives are always one click away. Rendering is sanitized
  // (lib/markdown.ts); package authors are many and the output is {@html}.
  import { renderMarkdown } from "../lib/markdown";
  import CodeBlock from "./CodeBlock.svelte";

  interface Props {
    source: string;
    /** Show the Rendered / Source switch. */
    toggle?: boolean;
    /** Lines shown before the source view collapses. */
    collapsedLines?: number;
  }
  let { source, toggle = true, collapsedLines = 40 }: Props = $props();

  let mode = $state<"rendered" | "source">("rendered");
  let html = $derived(renderMarkdown(source));
</script>

<div class="md-wrap">
  {#if toggle}
    <div class="switch" role="tablist" aria-label="Markdown view">
      <button
        type="button"
        role="tab"
        aria-selected={mode === "rendered"}
        data-on={mode === "rendered"}
        onclick={() => (mode = "rendered")}>Rendered</button
      >
      <button
        type="button"
        role="tab"
        aria-selected={mode === "source"}
        data-on={mode === "source"}
        onclick={() => (mode = "source")}>Source</button
      >
    </div>
  {/if}
  {#if mode === "rendered"}
    <div class="md">{@html html}</div>
  {:else}
    <CodeBlock code={source} lang="markdown" {collapsedLines} />
  {/if}
</div>

<style>
  .md-wrap {
    position: relative;
  }
  .switch {
    position: absolute;
    top: -2px;
    right: 0;
    display: inline-flex;
    border: 1px solid var(--line-1);
    border-radius: 4px;
    overflow: hidden;
    z-index: 1;
  }
  .switch button {
    font: inherit;
    font-size: 11px;
    padding: 3px 8px;
    border: none;
    background: var(--bg-0);
    color: var(--fg-2);
    cursor: pointer;
  }
  .switch button + button {
    border-left: 1px solid var(--line-1);
  }
  .switch button[data-on="true"] {
    background: var(--bg-2);
    color: var(--fg-0);
  }

  /* Prose. Scoped to .md; children come from {@html} so need :global. */
  .md {
    font-size: 13.5px;
    line-height: 1.65;
    color: var(--fg-0);
    max-width: 78ch;
    overflow-wrap: anywhere;
  }
  .md :global(> :first-child) {
    margin-top: 0;
  }
  .md :global(h1),
  .md :global(h2),
  .md :global(h3),
  .md :global(h4) {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    letter-spacing: -0.01em;
    margin: 1.4em 0 0.5em;
    line-height: 1.2;
    font-feature-settings: normal;
  }
  .md :global(h1) { font-size: 20px; }
  .md :global(h2) { font-size: 17px; }
  .md :global(h3) { font-size: 15px; }
  .md :global(h4) { font-size: 14px; }
  .md :global(p),
  .md :global(ul),
  .md :global(ol),
  .md :global(blockquote),
  .md :global(table) {
    margin: 0 0 0.8em;
  }
  .md :global(ul),
  .md :global(ol) {
    padding-left: 1.4em;
  }
  .md :global(ul) { list-style: disc; }
  .md :global(ol) { list-style: decimal; }
  .md :global(li + li) { margin-top: 0.2em; }
  .md :global(a) {
    color: var(--fg-0);
    text-decoration: underline;
    text-decoration-color: var(--line-2);
    text-underline-offset: 2px;
  }
  .md :global(a:not([href])) {
    text-decoration-style: dotted;
    cursor: help;
  }
  .md :global(code) {
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    background: var(--bg-2);
    border-radius: 3px;
    padding: 1px 4px;
  }
  .md :global(pre) {
    background: var(--bg-1);
    border: 1px solid var(--line-0);
    border-radius: 6px;
    padding: 10px 12px;
    overflow-x: auto;
    margin: 0 0 0.9em;
  }
  .md :global(pre code) {
    background: transparent;
    padding: 0;
    font-size: 12px;
    line-height: 1.55;
  }
  .md :global(blockquote) {
    border-left: 2px solid var(--line-2);
    padding-left: 12px;
    color: var(--fg-1);
  }
  .md :global(table) {
    border-collapse: collapse;
    font-size: 12.5px;
  }
  .md :global(th),
  .md :global(td) {
    border: 1px solid var(--line-1);
    padding: 4px 8px;
    text-align: left;
    vertical-align: top;
  }
  .md :global(th) {
    background: var(--bg-1);
    font-weight: 600;
  }
  .md :global(hr) {
    border: none;
    border-top: 1px solid var(--line-1);
    margin: 1.2em 0;
  }
</style>
