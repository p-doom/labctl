<script lang="ts">
  // A tree of one part of a task package (workspace/, scoring/,
  // reference/solution/) beside a viewer for the selected file. Opens on
  // the README, or `initial`, or the first file.
  import { buildTree, formatBytes, type TreeNode } from "../lib/envs";
  import type { EnvFileEntry } from "../lib/types";
  import FileViewer from "./FileViewer.svelte";
  import Icon from "./Icon.svelte";

  interface Props {
    taskKey: string;
    files: EnvFileEntry[];
    /** e.g. "workspace/"; tree paths are shown relative to it. */
    prefix: string;
    rev?: string;
    grader?: boolean;
    /** File to open first, relative to the task (e.g. "reference/solution/solve.sh"). */
    initial?: string;
    height?: number;
  }
  let { taskKey, files, prefix, rev, grader = false, initial, height = 520 }: Props = $props();

  let tree = $derived(buildTree(files, prefix));
  let inPrefix = $derived(files.filter((f) => f.path.startsWith(prefix)));

  function defaultFile(): string | null {
    if (initial && inPrefix.some((f) => f.path === initial)) return initial;
    const readme = inPrefix.find((f) => /^readme\.md$/i.test(f.path.slice(prefix.length)));
    return readme?.path ?? inPrefix[0]?.path ?? null;
  }

  let selected = $state<string | null>(null);
  // Reset when the file set changes (new revision, grader toggle).
  $effect(() => {
    void inPrefix;
    if (!selected || !inPrefix.some((f) => f.path === selected)) selected = defaultFile();
  });

  // Top level open; deeper directories open on demand, and the ones on
  // the selected file's path open by themselves.
  let open = $state<Set<string>>(new Set());
  function isOpen(n: TreeNode, depth: number): boolean {
    if (open.has(n.path)) return true;
    if (open.has("!" + n.path)) return false;
    return depth === 0 || (selected?.startsWith(n.path + "/") ?? false);
  }
  function toggle(n: TreeNode, depth: number) {
    const next = new Set(open);
    const was = isOpen(n, depth);
    next.delete(n.path);
    next.delete("!" + n.path);
    next.add(was ? "!" + n.path : n.path);
    open = next;
  }
</script>

{#if inPrefix.length === 0}
  <p class="empty">No files under <code>{prefix}</code>.</p>
{:else}
  <div class="browser" style="height: {height}px">
    <div class="tree" role="tree" aria-label="Files">
      {#snippet nodes(list: TreeNode[], depth: number)}
        {#each list as n (n.path)}
          {#if n.dir}
            <button
              type="button"
              class="row dir"
              style="padding-left: {8 + depth * 14}px"
              role="treeitem"
              aria-selected="false"
              aria-expanded={isOpen(n, depth)}
              onclick={() => toggle(n, depth)}
            >
              <Icon name={isOpen(n, depth) ? "chevron-down" : "chevron-right"} size={11} />
              <span class="name">{n.name}</span>
              <span class="count mono">{n.count}</span>
            </button>
            {#if isOpen(n, depth)}
              {@render nodes(n.children, depth + 1)}
            {/if}
          {:else}
            <button
              type="button"
              class="row file"
              style="padding-left: {8 + depth * 14 + 15}px"
              role="treeitem"
              aria-selected={selected === n.path}
              data-on={selected === n.path}
              title="{n.path} · {formatBytes(n.file?.size ?? 0)}"
              onclick={() => (selected = n.path)}
            >
              <span class="name">{n.name}</span>
              {#if n.file?.executable}<span class="x mono" title="executable">x</span>{/if}
            </button>
          {/if}
        {/each}
      {/snippet}
      {@render nodes(tree, 0)}
    </div>
    <div class="pane">
      {#if selected}
        {#key `${selected}@${rev ?? ""}`}
          <FileViewer {taskKey} path={selected} {rev} {grader} />
        {/key}
      {/if}
    </div>
  </div>
{/if}

<style>
  .browser {
    display: grid;
    grid-template-columns: minmax(180px, 260px) minmax(0, 1fr);
    border: 1px solid var(--line-1);
    border-radius: 6px;
    overflow: hidden;
    background: var(--bg-0);
  }
  .tree {
    overflow: auto;
    border-right: 1px solid var(--line-1);
    background: var(--bg-1);
    padding: 4px 0;
  }
  .pane {
    min-width: 0;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }
  .row {
    display: flex;
    align-items: center;
    gap: 5px;
    width: 100%;
    padding: 3px 8px;
    border: none;
    background: transparent;
    color: var(--fg-1);
    font: inherit;
    font-size: 12px;
    text-align: left;
    cursor: pointer;
    white-space: nowrap;
  }
  .row:hover {
    background: var(--bg-2);
    color: var(--fg-0);
  }
  .row[data-on="true"] {
    background: var(--bg-3);
    color: var(--fg-0);
  }
  .name {
    overflow: hidden;
    text-overflow: ellipsis;
    min-width: 0;
  }
  .dir .name {
    color: var(--fg-0);
  }
  .count,
  .x {
    margin-left: auto;
    font-size: 10px;
    color: var(--fg-3);
    padding-left: 6px;
  }
  .empty {
    font-size: 12px;
    color: var(--fg-2);
  }
  .empty code {
    font-family: theme("fontFamily.mono");
  }
</style>
