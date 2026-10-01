<script lang="ts">
  // Environment list. One row per task package in the [envs] repo at a
  // revision (`?rev=`, default [envs].rev). Search covers key, title,
  // tags, authors and the prompt; tag chips narrow further. Read from git
  // by the server — see src/envs.rs.
  import { api } from "../lib/api";
  import { router } from "../lib/router.svelte";
  import { errorMessage, filterTasks, formatBytes, formatSecs, haystack, tagCounts } from "../lib/envs";
  import type { EnvsList, EnvSummary } from "../lib/types";
  import FilterBar from "../components/FilterBar.svelte";
  import FilterInput from "../components/FilterInput.svelte";
  import EmptyState from "../components/EmptyState.svelte";
  import RevisionPicker from "../components/RevisionPicker.svelte";

  let active = $derived(router.view === "envs" && !router.selected);
  let rev = $derived(active ? router.query.get("rev") : null);

  let data = $state<EnvsList | null>(null);
  let loadedRev = $state<string | null | undefined>(undefined);
  let error = $state<string | null>(null);

  // Fetch lazily: only once the view is shown, and again when ?rev= moves.
  $effect(() => {
    if (!active || loadedRev === rev) return;
    const want = rev;
    loadedRev = want;
    error = null;
    api
      .envs(want ?? undefined)
      .then((d) => {
        if (loadedRev === want) data = d;
      })
      .catch((e) => {
        if (loadedRev === want) error = errorMessage(e);
      });
  });

  type Configured = Extract<EnvsList, { configured: true }>;
  // Narrowed once here: TypeScript doesn't carry {#if} narrowing into snippets.
  let cfg = $derived<Configured | null>(data?.configured ? data : null);
  let tasks = $derived(cfg?.tasks ?? []);
  let filterText = $state("");
  let selectedTags = $state<Set<string>>(new Set());
  let showAllTags = $state(false);

  let hay = $derived.by(() => {
    const m = new Map<string, string>();
    for (const t of tasks) m.set(t.key, haystack(t));
    return m;
  });
  let shown = $derived(filterTasks(tasks, filterText, selectedTags, (t) => hay.get(t.key) ?? ""));
  let tags = $derived(tagCounts(tasks));
  // Tags on a single task are mostly noise in the chip row; keep them
  // one click away rather than crowding out the shared ones.
  let visibleTags = $derived(
    showAllTags ? tags : tags.filter(([t, n]) => n > 1 || selectedTags.has(t)),
  );

  function toggleTag(t: string) {
    const next = new Set(selectedTags);
    if (next.has(t)) next.delete(t);
    else next.add(t);
    selectedTags = next;
  }

  function open(t: EnvSummary) {
    const q = new URLSearchParams();
    if (rev) q.set("rev", rev);
    router.go("envs", t.key, q);
  }

  function setRev(r: string | null) {
    router.setQuery({ rev: r });
  }

  // Keyboard: same grammar as the other lists.
  let cursor = $state(0);
  let listEl = $state<HTMLDivElement | null>(null);
  let filterInputEl = $state<HTMLInputElement | null>(null);
  $effect(() => {
    void filterText;
    void selectedTags;
    cursor = 0;
  });
  function onKey(e: KeyboardEvent) {
    if (!active) return;
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      filterInputEl?.focus();
      filterInputEl?.select();
      return;
    }
    const t = e.target as HTMLElement | null;
    const inField = t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA");
    if (e.key === "/" && !inField) {
      e.preventDefault();
      filterInputEl?.focus();
      return;
    }
    if (inField) return;
    if (e.key === "j" || e.key === "ArrowDown") {
      e.preventDefault();
      cursor = Math.min(cursor + 1, shown.length - 1);
    } else if (e.key === "k" || e.key === "ArrowUp") {
      e.preventDefault();
      cursor = Math.max(cursor - 1, 0);
    } else if (e.key === "Enter" && shown[cursor]) {
      open(shown[cursor]);
    }
  }
  $effect(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });
  $effect(() => {
    const i = cursor;
    if (!listEl) return;
    requestAnimationFrame(() => {
      listEl!.querySelectorAll<HTMLElement>(".list-row")[i]?.scrollIntoView({ block: "nearest" });
    });
  });
</script>

<div class="page">
  <FilterBar>
    <FilterInput
      bind:inputRef={filterInputEl}
      value={filterText}
      placeholder="Filter environments…"
      onInput={(v) => (filterText = v)}
      onEnter={() => shown[cursor] && open(shown[cursor])}
    />
    {#if data?.configured}
      <span class="spacer"></span>
      <span class="count mono">{shown.length} of {tasks.length}</span>
      <RevisionPicker
        {rev}
        commit={data.commit}
        defaultRev={data.default_rev}
        onSelect={setRev}
      />
    {/if}
  </FilterBar>

  {#if data?.configured && tags.length > 0}
    <div class="tags">
      {#each visibleTags as [t, n] (t)}
        <button
          type="button"
          class="chip"
          data-on={selectedTags.has(t)}
          aria-pressed={selectedTags.has(t)}
          onclick={() => toggleTag(t)}
        >
          {t}<span class="n mono">{n}</span>
        </button>
      {/each}
      {#if tags.length > visibleTags.length || showAllTags}
        <button type="button" class="more" onclick={() => (showAllTags = !showAllTags)}>
          {showAllTags ? "Fewer tags" : `${tags.length - visibleTags.length} more`}
        </button>
      {/if}
      {#if selectedTags.size > 0}
        <button type="button" class="more" onclick={() => (selectedTags = new Set())}>Clear</button>
      {/if}
    </div>
  {/if}

  <div class="list" bind:this={listEl}>
    {#if error}
      <EmptyState title="Couldn't read the environments.">
        {#snippet sub()}<span class="mono err">{error}</span>{/snippet}
      </EmptyState>
    {:else if !data}
      {#each Array(6) as _, i (i)}
        <div class="list-row env-row">
          <div class="skel" style="height: 14px; width: 60%"></div>
          <div class="skel" style="height: 12px; width: 70%"></div>
          <div class="skel" style="height: 12px; width: 70%"></div>
          <div class="skel" style="height: 12px; width: 50px"></div>
          <div class="skel" style="height: 12px; width: 40px"></div>
        </div>
      {/each}
    {:else if !data.configured}
      <EmptyState title="No environments configured.">
        {#snippet sub()}
          Add the crowd-cast-envs checkout to <code>[repos]</code> and point
          <code>[envs]</code> at it in the cluster config:
          <code class="block">[envs]<br />repo = "crowd-cast-envs"</code>
          Tasks are read from git at <code>origin/main</code>; run
          <code>git fetch</code> in that checkout to pick up new ones.
        {/snippet}
      </EmptyState>
    {:else if tasks.length === 0}
      <EmptyState title="No task packages at this revision.">
        {#snippet sub()}
          Nothing under <code>{cfg?.repo.tasks_dir}</code> has a <code>task.json</code> at
          <span class="mono">{cfg?.commit.short}</span>.
          {#if (cfg?.skipped ?? []).length > 0}
            {(cfg?.skipped ?? []).length} director{(cfg?.skipped ?? []).length === 1 ? "y" : "ies"} without one —
            likely a commit from before the RL-Env-Contract layout.
          {/if}
        {/snippet}
      </EmptyState>
    {:else}
      <div class="list-head env-row">
        <div>World</div>
        <div>Tags</div>
        <div>Authors</div>
        <div class="r">Workspace</div>
        <div class="r">Agent time</div>
      </div>
      {#if shown.length === 0}
        <p class="none">No environments match.</p>
      {/if}
      {#each shown as t, i (t.key)}
        <button
          type="button"
          class="list-row env-row"
          data-state={cursor === i ? "cursor" : undefined}
          onclick={() => {
            cursor = i;
            open(t);
          }}
        >
          <div class="world">
            <span class="title">{t.title ?? t.key}</span>
            <span class="key mono">{t.key}{#if t.error}<span class="bad"> · {t.error}</span>{/if}</span>
          </div>
          <div class="row-tags">
            {#each t.tags as tag (tag)}
              <span class="tag" data-on={selectedTags.has(tag)}>{tag}</span>
            {/each}
          </div>
          <div class="authors">{t.authors.join(", ") || "—"}</div>
          <div class="r mono small">{t.workspace_files} files · {formatBytes(t.workspace_bytes)}</div>
          <div class="r mono small">{formatSecs(t.agent_timeout_secs)}</div>
        </button>
      {/each}
      {#if data.skipped.length > 0}
        <p class="none">
          {data.skipped.length} director{data.skipped.length === 1 ? "y" : "ies"} under
          <code>{data.repo.tasks_dir}</code> without a <code>task.json</code> not shown:
          <span class="mono">{data.skipped.join(", ")}</span>
        </p>
      {/if}
    {/if}
  </div>
</div>

<style>
  .page {
    display: flex;
    flex-direction: column;
    height: 100%;
    overflow: hidden;
  }
  .spacer {
    flex: 1;
  }
  .count {
    font-size: 11px;
    color: var(--fg-2);
  }
  .tags {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    padding: 8px 16px;
    border-bottom: 1px solid var(--line-0);
    flex-shrink: 0;
  }
  .chip,
  .more {
    display: inline-flex;
    align-items: baseline;
    gap: 5px;
    font: inherit;
    font-size: 11px;
    padding: 2px 8px;
    border-radius: 999px;
    border: 1px solid var(--line-1);
    background: var(--bg-0);
    color: var(--fg-1);
    cursor: pointer;
  }
  .chip:hover,
  .more:hover {
    border-color: var(--line-2);
    color: var(--fg-0);
  }
  .chip[data-on="true"] {
    background: var(--accent-soft);
    border-color: var(--accent);
    color: var(--accent-dim);
  }
  .chip .n {
    font-size: 10px;
    color: var(--fg-3);
  }
  .more {
    border-style: dashed;
    color: var(--fg-2);
  }
  .list {
    flex: 1;
    overflow-y: auto;
  }
  .env-row {
    grid-template-columns: minmax(0, 2.2fr) minmax(0, 1.6fr) minmax(0, 1fr) 136px 84px;
  }
  .env-row.list-row {
    padding-top: 12px;
    padding-bottom: 12px;
  }
  .r {
    text-align: right;
  }
  .world {
    display: flex;
    flex-direction: column;
    gap: 3px;
    min-width: 0;
  }
  .title {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    font-size: 15px;
    color: var(--fg-0);
    letter-spacing: -0.005em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-feature-settings: normal;
  }
  .key {
    font-size: 11px;
    color: var(--fg-2);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .bad,
  .err {
    color: var(--status-failed-fg);
  }
  .row-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    min-width: 0;
    max-height: 40px;
    overflow: hidden;
  }
  .tag {
    font-size: 11px;
    color: var(--fg-2);
    background: var(--bg-2);
    border-radius: 3px;
    padding: 0 5px;
    white-space: nowrap;
  }
  .tag[data-on="true"] {
    color: var(--accent-dim);
    background: var(--accent-soft);
  }
  .authors {
    font-size: 12px;
    color: var(--fg-1);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .small {
    font-size: 11px;
    color: var(--fg-2);
  }
  .none {
    padding: 16px;
    font-size: 12px;
    color: var(--fg-2);
  }
  code {
    font-family: theme("fontFamily.mono");
    font-size: 12px;
  }
  code.block {
    display: block;
    margin: 10px auto;
    padding: 8px 12px;
    width: max-content;
    text-align: left;
    background: var(--bg-1);
    border: 1px solid var(--line-1);
    border-radius: 4px;
  }
</style>
