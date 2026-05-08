<script lang="ts">
  // Cmd-K command palette. Targets:
  //  - jump to run by id prefix or recipe name
  //  - jump to artifact by alias / id
  //  - jump to pipeline
  //  - switch view, toggle live updates
  // Hand-rolled rather than pulling in cmdk-sv: the surface is small enough
  // that a 200-line component beats a dependency for tight visual control.

  import { tick } from "svelte";
  import { store } from "../lib/store.svelte";
  import { router, type View } from "../lib/router.svelte";
  import Icon from "./Icon.svelte";

  interface Props {
    open: boolean;
    onClose: () => void;
  }
  let { open, onClose }: Props = $props();

  let query = $state("");
  let cursor = $state(0);
  let inputEl: HTMLInputElement | null = $state(null);

  $effect(() => {
    if (open) {
      query = "";
      cursor = 0;
      tick().then(() => inputEl?.focus());
    }
  });

  type Action = {
    id: string;
    label: string;
    sub?: string;
    section: string;
    kbd?: string;
    onSelect: () => void;
  };

  let actions = $derived.by<Action[]>(() => {
    const q = query.trim().toLowerCase();
    const results: Action[] = [];

    // Always-visible navigation actions when query is empty.
    const navActions: Action[] = [
      { id: "go-runs", label: "Go to Runs", section: "navigate", kbd: "g r", onSelect: () => router.go("runs") },
      { id: "go-pipelines", label: "Go to Pipelines", section: "navigate", kbd: "g p", onSelect: () => router.go("pipelines") },
      { id: "go-artifacts", label: "Go to Artifacts", section: "navigate", kbd: "g a", onSelect: () => router.go("artifacts") },
      { id: "go-evals", label: "Go to Evals", section: "navigate", kbd: "g e", onSelect: () => router.go("evals") },
    ];

    if (!q) {
      results.push(...navActions);
      // Recent runs as default suggestions.
      for (const r of (store.runs.data ?? []).slice(0, 8)) {
        results.push({
          id: `run-${r.id}`,
          label: r.recipe_name,
          sub: r.id,
          section: "recent runs",
          onSelect: () => router.go("runs", r.id),
        });
      }
      return results;
    }

    // Filter nav.
    for (const a of navActions) {
      if (a.label.toLowerCase().includes(q)) results.push(a);
    }

    // Runs by id prefix or recipe name.
    let runHits = 0;
    for (const r of store.runs.data ?? []) {
      if (
        r.id.toLowerCase().includes(q) ||
        r.recipe_name.toLowerCase().includes(q) ||
        (r.job_id?.toLowerCase().includes(q) ?? false)
      ) {
        results.push({
          id: `run-${r.id}`,
          label: r.recipe_name,
          sub: r.id,
          section: "runs",
          onSelect: () => router.go("runs", r.id),
        });
        if (++runHits >= 12) break;
      }
    }

    let pipeHits = 0;
    for (const p of store.pipelines.data ?? []) {
      if (p.id.toLowerCase().includes(q) || p.name.toLowerCase().includes(q)) {
        results.push({
          id: `pipe-${p.id}`,
          label: p.name,
          sub: p.id,
          section: "pipelines",
          onSelect: () => router.go("pipelines", p.id),
        });
        if (++pipeHits >= 8) break;
      }
    }

    let artHits = 0;
    for (const a of store.artifacts.data ?? []) {
      const alias = (a.aliases ?? []).find((al) => al.toLowerCase().includes(q));
      if (
        alias ||
        a.id.toLowerCase().includes(q) ||
        a.kind.toLowerCase().includes(q) ||
        a.content_hash.toLowerCase().startsWith(q)
      ) {
        results.push({
          id: `art-${a.id}`,
          label: alias ?? a.id,
          sub: `${a.kind} · ${a.id}`,
          section: "artifacts",
          onSelect: () => router.go("artifacts", a.id),
        });
        if (++artHits >= 12) break;
      }
    }
    return results;
  });

  $effect(() => {
    if (cursor >= actions.length) cursor = Math.max(0, actions.length - 1);
  });

  function pick(a: Action) {
    a.onSelect();
    onClose();
  }

  function onKey(e: KeyboardEvent) {
    if (!open) return;
    if (e.key === "Escape") {
      e.preventDefault();
      onClose();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      cursor = Math.min(cursor + 1, actions.length - 1);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      cursor = Math.max(cursor - 1, 0);
    } else if (e.key === "Enter") {
      e.preventDefault();
      const a = actions[cursor];
      if (a) pick(a);
    }
  }

  $effect(() => {
    if (open) {
      window.addEventListener("keydown", onKey);
      return () => window.removeEventListener("keydown", onKey);
    }
  });

  // Group actions by section, preserving order of first occurrence.
  let grouped = $derived.by(() => {
    const order: string[] = [];
    const map = new Map<string, Action[]>();
    actions.forEach((a, i) => {
      if (!map.has(a.section)) {
        map.set(a.section, []);
        order.push(a.section);
      }
      map.get(a.section)!.push({ ...a, id: `${a.id}__${i}` });
    });
    return order.map((s) => ({ section: s, items: map.get(s)! }));
  });

  let flat = $derived(grouped.flatMap((g) => g.items));
</script>

{#if open}
  <button type="button" class="scrim" onclick={onClose} aria-label="Close palette"></button>
  <div class="palette" role="dialog" aria-modal="true" aria-label="Command palette">
    <div class="input-wrap">
      <Icon name="search" size={14} />
      <input
        bind:this={inputEl}
        type="text"
        placeholder="Jump to a run, pipeline, artifact…"
        value={query}
        oninput={(e) => {
          query = (e.currentTarget as HTMLInputElement).value;
          cursor = 0;
        }}
        autocomplete="off"
        spellcheck="false"
      />
      <span class="hint kbd">Esc</span>
    </div>
    <div class="results">
      {#if actions.length === 0}
        <div class="noresult">No matches.</div>
      {:else}
        {#each grouped as g}
          <div class="section">
            <div class="section-h">{g.section}</div>
            {#each g.items as item}
              {@const idx = flat.findIndex((f) => f.id === item.id)}
              <button
                type="button"
                class="item"
                class:active={idx === cursor}
                onmouseenter={() => (cursor = idx)}
                onclick={() => pick(item)}
              >
                <span class="label">{item.label}</span>
                {#if item.sub}<span class="sub mono">{item.sub}</span>{/if}
                {#if item.kbd}<span class="kbd">{item.kbd}</span>{/if}
              </button>
            {/each}
          </div>
        {/each}
      {/if}
    </div>
    <footer>
      <span class="kbd">↑</span><span class="kbd">↓</span><span class="hint">navigate</span>
      <span class="kbd">↵</span><span class="hint">select</span>
      <span class="kbd">Esc</span><span class="hint">close</span>
    </footer>
  </div>
{/if}

<style>
  .scrim {
    position: fixed;
    inset: 0;
    background: rgba(10, 11, 13, 0.6);
    z-index: 50;
    animation: fadeIn 150ms cubic-bezier(0.2, 0, 0, 1);
    backdrop-filter: blur(2px);
    border: none;
    cursor: default;
    padding: 0;
  }
  @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
  .palette {
    position: fixed;
    top: 14vh;
    left: 50%;
    transform: translateX(-50%);
    width: min(620px, 92vw);
    background: theme("colors.bg.1");
    border: 1px solid theme("colors.line.1");
    border-radius: 8px;
    box-shadow: theme("boxShadow.panel");
    z-index: 51;
    overflow: hidden;
    animation: scaleIn 150ms cubic-bezier(0.2, 0, 0, 1);
    display: flex;
    flex-direction: column;
    max-height: 70vh;
  }
  @keyframes scaleIn {
    from { opacity: 0; transform: translate(-50%, -4px) scale(0.99); }
    to { opacity: 1; transform: translate(-50%, 0) scale(1); }
  }
  .input-wrap {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 12px 14px;
    border-bottom: 1px solid theme("colors.line.0");
    color: theme("colors.fg.2");
  }
  .input-wrap input {
    flex: 1;
    background: transparent;
    border: none;
    color: theme("colors.fg.0");
    font-size: 14px;
    outline: none;
  }
  .input-wrap input::placeholder { color: theme("colors.fg.3"); }
  .hint { color: theme("colors.fg.3"); font-size: 11px; }
  .results { flex: 1; overflow-y: auto; padding: 6px 0; }
  .section { padding: 4px 0; }
  .section-h {
    font-family: theme("fontFamily.mono");
    font-size: 10px;
    color: theme("colors.fg.3");
    letter-spacing: 0.06em;
    text-transform: uppercase;
    padding: 6px 14px 4px 14px;
  }
  .item {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 7px 14px;
    cursor: pointer;
    background: transparent;
    border: none;
    width: 100%;
    text-align: left;
    color: theme("colors.fg.1");
    font-size: 13px;
  }
  .item.active {
    background: theme("colors.bg.2");
    color: theme("colors.fg.0");
  }
  .item .label { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .item .sub { font-size: 11px; color: theme("colors.fg.2"); }
  .item.active .sub { color: theme("colors.fg.1"); }
  .item .kbd { margin-left: auto; }
  .noresult {
    padding: 24px 14px;
    text-align: center;
    font-size: 13px;
    color: theme("colors.fg.2");
  }
  footer {
    display: flex;
    align-items: center;
    gap: 4px;
    padding: 8px 14px;
    border-top: 1px solid theme("colors.line.0");
    background: theme("colors.bg.1");
    font-size: 11px;
    color: theme("colors.fg.2");
  }
  footer .hint { margin-right: 12px; }
  footer .kbd { margin: 0; }
</style>
