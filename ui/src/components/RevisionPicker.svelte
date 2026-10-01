<script lang="ts">
  // Which commit of the env repo the browser reads. The button names the
  // revision and the commit it resolved to; the popover lists branches,
  // tags and recent commits that touched the tasks, plus a free-form
  // field for any sha. Choosing the default clears `?rev=`.
  import { api } from "../lib/api";
  import { errorMessage, formatCommitDate, shortRev } from "../lib/envs";
  import type { EnvCommit, EnvRevisions } from "../lib/types";
  import Icon from "./Icon.svelte";

  interface Props {
    /** The `?rev=` in the URL; null means the configured default. */
    rev: string | null;
    /** What the current view resolved to. */
    commit: EnvCommit | null;
    defaultRev: string | null;
    onSelect: (rev: string | null) => void;
  }
  let { rev, commit, defaultRev, onSelect }: Props = $props();

  let open = $state(false);
  let data = $state<EnvRevisions | null>(null);
  let error = $state<string | null>(null);
  let typed = $state("");
  let root = $state<HTMLDivElement | null>(null);

  function show() {
    open = !open;
    if (open && !data) {
      api.envRevisions().then((d) => (data = d)).catch((e) => {
        error = errorMessage(e);
      });
    }
  }

  function choose(r: string | null) {
    open = false;
    typed = "";
    onSelect(r && r !== defaultRev ? r : null);
  }

  $effect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (root && !root.contains(e.target as Node)) open = false;
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") open = false;
    };
    document.addEventListener("mousedown", onDoc);
    window.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDoc);
      window.removeEventListener("keydown", onKey);
    };
  });

  let label = $derived(shortRev(rev ?? defaultRev ?? "default"));
  let current = $derived(commit?.sha ?? null);
</script>

<div class="picker" bind:this={root}>
  <button
    type="button"
    class="btn-secondary trigger"
    data-state={rev ? "active" : undefined}
    aria-haspopup="dialog"
    aria-expanded={open}
    title={commit ? `${commit.sha}\n${commit.subject}` : undefined}
    onclick={show}
  >
    <Icon name="commit" size={13} />
    <span class="rev mono">{label}</span>
    {#if commit && commit.short !== label}
      <span class="sha mono">{commit.short}</span>
    {/if}
    {#if commit}
      <span class="date">{formatCommitDate(commit.date)}</span>
    {/if}
    <Icon name="chevron-down" size={11} />
  </button>

  {#if open}
    <div class="pop" role="dialog" aria-label="Choose revision">
      <form
        class="goto"
        onsubmit={(e) => {
          e.preventDefault();
          if (typed.trim()) choose(typed.trim());
        }}
      >
        <input
          class="mono"
          type="text"
          placeholder="Branch, tag or sha…"
          bind:value={typed}
          spellcheck="false"
          autocomplete="off"
        />
      </form>

      {#if error}
        <p class="err">{error}</p>
      {:else if !data}
        <div class="skel" style="height: 120px; margin: 8px"></div>
      {:else}
        <div class="scroll">
          <p class="masthead group">Default</p>
          <button type="button" class="item" data-on={!rev} onclick={() => choose(null)}>
            <span class="mono name">{data.default_rev}</span>
            {#if data.default}
              <span class="mono meta">{data.default.short}</span>
            {:else}
              <span class="warn">doesn't resolve — run git fetch?</span>
            {/if}
          </button>

          {#if data.refs.length > 0}
            <p class="masthead group">Branches &amp; tags</p>
            {#each data.refs.filter((r) => r.name !== data?.default_rev) as r (r.kind + r.name)}
              <button type="button" class="item" data-on={rev === r.name} onclick={() => choose(r.name)}>
                <span class="mono name">{r.name}</span>
                <span class="kind">{r.kind}</span>
                <span class="mono meta">{r.sha.slice(0, 7)}</span>
              </button>
            {/each}
          {/if}

          {#if data.commits.length > 0}
            <p class="masthead group">Commits touching tasks</p>
            {#each data.commits as c (c.sha)}
              <button
                type="button"
                class="item commit"
                data-on={current === c.sha && !!rev}
                title={c.sha}
                onclick={() => choose(c.sha)}
              >
                <span class="mono meta">{c.short}</span>
                <span class="subject">{c.subject}</span>
                <span class="date">{formatCommitDate(c.date)}</span>
              </button>
            {/each}
          {/if}
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .picker {
    position: relative;
  }
  .trigger {
    max-width: 380px;
  }
  .rev {
    font-size: 11px;
    color: var(--fg-0);
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .sha {
    font-size: 11px;
    color: var(--fg-2);
  }
  .date {
    font-size: 11px;
    color: var(--fg-2);
    white-space: nowrap;
  }
  .pop {
    position: absolute;
    right: 0;
    top: calc(100% + 4px);
    width: 420px;
    max-height: 460px;
    display: flex;
    flex-direction: column;
    background: var(--bg-0);
    border: 1px solid var(--line-2);
    border-radius: 6px;
    box-shadow: 0 8px 24px var(--shadow-panel-1);
    z-index: 20;
  }
  .goto {
    padding: 8px;
    border-bottom: 1px solid var(--line-1);
  }
  .goto input {
    width: 100%;
    font-size: 12px;
    padding: 5px 8px;
    border: 1px solid var(--line-1);
    border-radius: 4px;
    background: var(--bg-1);
    color: var(--fg-0);
  }
  .scroll {
    overflow-y: auto;
    padding: 4px 0 8px;
  }
  .group {
    padding: 8px 12px 4px;
    margin: 0;
  }
  .item {
    display: flex;
    align-items: baseline;
    gap: 8px;
    width: 100%;
    padding: 5px 12px;
    border: none;
    background: transparent;
    font: inherit;
    font-size: 12px;
    color: var(--fg-1);
    text-align: left;
    cursor: pointer;
  }
  .item:hover {
    background: var(--bg-2);
    color: var(--fg-0);
  }
  .item[data-on="true"] {
    background: var(--accent-soft);
    color: var(--fg-0);
  }
  .name {
    color: var(--fg-0);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }
  .kind {
    font-size: 10px;
    color: var(--fg-3);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }
  .meta {
    font-size: 11px;
    color: var(--fg-2);
    margin-left: auto;
    flex-shrink: 0;
  }
  .commit .meta {
    margin-left: 0;
  }
  .subject {
    flex: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .warn,
  .err {
    font-size: 11px;
    color: var(--status-failed-fg);
  }
  .err {
    padding: 12px;
    margin: 0;
  }
</style>
