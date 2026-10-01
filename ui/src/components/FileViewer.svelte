<script lang="ts">
  // One file of a task package at a revision: Markdown rendered (with a
  // source toggle), code highlighted, big files shown plain, binaries
  // described rather than dumped.
  import { api } from "../lib/api";
  import { highlight } from "../lib/highlight";
  import { errorMessage, formatBytes, isMarkdown, langForPath } from "../lib/envs";
  import { theme } from "../lib/theme.svelte";
  import type { EnvFileContent } from "../lib/types";
  import Markdown from "./Markdown.svelte";

  interface Props {
    taskKey: string;
    path: string;
    rev?: string;
    grader?: boolean;
  }
  let { taskKey, path, rev, grader = false }: Props = $props();

  /** Above this, skip highlighting: shiki on a 500 KB JSONL stalls the tab. */
  const HIGHLIGHT_MAX = 200 * 1024;

  let file = $state<EnvFileContent | null>(null);
  let error = $state<string | null>(null);
  let html = $state<string | null>(null);

  $effect(() => {
    const p = path;
    file = null;
    error = null;
    html = null;
    let cancel = false;
    api
      .envFile(taskKey, p, rev, grader)
      .then((f) => {
        if (!cancel) file = f;
      })
      .catch((e) => {
        if (!cancel) error = errorMessage(e);
      });
    return () => {
      cancel = true;
    };
  });

  let lang = $derived(langForPath(path));

  $effect(() => {
    void theme.effective;
    const f = file;
    if (!f?.text || !lang || isMarkdown(f.path) || f.size > HIGHLIGHT_MAX) return;
    let cancel = false;
    highlight(f.text, lang).then((h) => {
      if (!cancel) html = h;
    });
    return () => {
      cancel = true;
    };
  });
</script>

<div class="viewer">
  <div class="bar">
    <span class="path mono">{path}</span>
    {#if file}
      <span class="size mono">{formatBytes(file.size)}</span>
    {/if}
  </div>
  {#if error}
    <div class="note">{error}</div>
  {:else if !file}
    <div class="pad"><div class="skel" style="height: 160px; width: 100%"></div></div>
  {:else if file.binary}
    <div class="note">Binary file, {formatBytes(file.size)}. Not shown.</div>
  {:else}
    {#if file.truncated}
      <div class="trunc">
        Showing the first {formatBytes(file.text?.length ?? 0)} of {formatBytes(file.size)}.
      </div>
    {/if}
    {#if isMarkdown(file.path)}
      <div class="pad md-pad"><Markdown source={file.text ?? ""} /></div>
    {:else if html}
      <div class="code hl">{@html html}</div>
    {:else}
      <pre class="code raw"><code>{file.text}</code></pre>
    {/if}
  {/if}
</div>

<style>
  .viewer {
    display: flex;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
    height: 100%;
  }
  .bar {
    display: flex;
    align-items: baseline;
    gap: 10px;
    padding: 8px 12px;
    border-bottom: 1px solid var(--line-0);
    background: var(--bg-1);
    flex-shrink: 0;
  }
  .path {
    font-size: 12px;
    color: var(--fg-0);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }
  .size {
    font-size: 11px;
    color: var(--fg-2);
    margin-left: auto;
    flex-shrink: 0;
  }
  .pad {
    padding: 12px;
  }
  .md-pad {
    padding: 16px 18px;
    overflow: auto;
  }
  .note,
  .trunc {
    padding: 12px;
    font-size: 12px;
    color: var(--fg-2);
  }
  .trunc {
    padding: 6px 12px;
    border-bottom: 1px solid var(--line-0);
    background: var(--bg-2);
  }
  .code {
    flex: 1;
    min-height: 0;
    overflow: auto;
    margin: 0;
  }
  .code :global(pre),
  .raw {
    background: transparent !important;
    padding: 10px 12px;
    margin: 0;
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    line-height: 1.55;
    tab-size: 4;
  }
  .raw {
    color: var(--fg-1);
    white-space: pre;
  }
</style>
