<script lang="ts">
  import { highlight } from "../lib/highlight";
  import { theme } from "../lib/theme.svelte";

  interface Props {
    code: string;
    lang?: "toml" | "json" | "bash";
    collapsedLines?: number;
  }
  let { code, lang = "toml", collapsedLines = 12 }: Props = $props();

  let html = $state<string | null>(null);
  let expanded = $state(false);

  $effect(() => {
    // Re-run when the active theme flips so the highlight palette tracks.
    void theme.effective;
    let cancel = false;
    highlight(code, lang).then((h) => {
      if (!cancel) html = h;
    });
    return () => {
      cancel = true;
    };
  });

  let lines = $derived(code.split("\n"));
  // +2 is a deliberate threshold: don't bother collapsing if the user would
  // gain less than a couple of lines. The "Show 1 more line" button reads
  // worse than just showing the line.
  let canCollapse = $derived(lines.length > collapsedLines + 2);
</script>

<div class="codeblock" class:collapsed={canCollapse && !expanded}>
  {#if html}
    <div class="hl">{@html html}</div>
  {:else}
    <pre class="raw"><code>{code}</code></pre>
  {/if}
  {#if canCollapse && !expanded}
    <button type="button" class="more" onclick={() => (expanded = true)}>
      Show {lines.length - collapsedLines} more lines
    </button>
  {:else if canCollapse && expanded}
    <button type="button" class="less" onclick={() => (expanded = false)}>
      Collapse
    </button>
  {/if}
</div>

<style>
  .codeblock {
    position: relative;
    background: theme("colors.bg.0");
    border: 1px solid theme("colors.line.0");
    border-radius: 6px;
    overflow: hidden;
  }
  .codeblock.collapsed .hl,
  .codeblock.collapsed .raw {
    max-height: 240px;
    overflow: hidden;
    mask-image: linear-gradient(to bottom, black 80%, transparent);
    -webkit-mask-image: linear-gradient(to bottom, black 80%, transparent);
  }
  :global(.codeblock pre) {
    background: transparent !important;
    padding: 12px 14px;
    margin: 0;
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    line-height: 1.55;
    overflow-x: auto;
  }
  .raw {
    padding: 12px 14px;
    margin: 0;
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    line-height: 1.55;
    color: theme("colors.fg.1");
    overflow-x: auto;
  }
  .more, .less {
    position: absolute;
    bottom: 8px;
    left: 50%;
    transform: translateX(-50%);
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    background: theme("colors.bg.2");
    color: theme("colors.fg.1");
    border: 1px solid theme("colors.line.1");
    padding: 3px 10px;
    border-radius: 999px;
    cursor: pointer;
  }
  .less { position: static; transform: none; margin: 8px auto; display: block; }
  .more:hover, .less:hover { color: theme("colors.fg.0"); border-color: theme("colors.line.2"); }
</style>
