<script lang="ts">
  import { store, loadRunDetail, loadRunLog, loadRunEvents } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { compareSelection } from "../lib/compare.svelte";
  import {
    copy,
    editionNumber,
    formatEditionDate,
    formatEditionTime,
    formatAbsolute,
    formatRelative,
    formatDuration,
    statusGroup,
    statusGlyph,
    shortStatus,
    methodsParagraph,
  } from "../lib/format";
  import { panelHistory } from "../lib/panel.svelte";
  import { nowSecs } from "../lib/time.svelte";

  import SidePanel from "../components/SidePanel.svelte";
  import Icon from "../components/Icon.svelte";
  import CodeBlock from "../components/CodeBlock.svelte";
  import Result from "../components/Result.svelte";
  import EvalSeriesCard from "../components/EvalSeriesCard.svelte";

  interface Props {
    runId: string;
  }
  let { runId }: Props = $props();

  let detail = $derived(store.runDetail(runId));
  let log = $derived(store.runLog(runId));
  let events = $derived(store.runEvents(runId) ?? []);
  let error = $state<string | null>(null);
  let logsOpen = $state(false);

  $effect(() => {
    if (!runId) return;
    // Reset per-run UI state.
    logsOpen = false;
    Promise.all([
      loadRunDetail(runId),
      loadRunLog(runId),
      loadRunEvents(runId),
    ]).then(() => {
      error = null;
    }).catch((e) => {
      error = e instanceof Error ? e.message : String(e);
    });
  });

  let wandb = $derived(detail?.tracking?.wandb ?? null);

  let evalOutput = $derived.by(() => {
    const out = detail?.outputs.find((o) => o.kind === "eval_result");
    return out as (typeof out & { result?: unknown }) | undefined;
  });
  let evalResult = $derived<unknown>(evalOutput?.result ?? null);
  let recipeToml = $derived.by(() => {
    if (!detail) return "";
    return tomlSerialize(detail.run.recipe as Record<string, unknown>);
  });

  let methods = $derived.by(() => (detail ? methodsParagraph(detail) : []));
  let duration = $derived.by(() => {
    if (!detail) return null;
    const r = detail.run;
    if (r.finished_at != null) return r.finished_at - r.created_at;
    if (r.is_terminal) return null;
    return Math.max(0, nowSecs.value - r.created_at);
  });

  function close() {
    router.select("runs", null);
  }

  function tomlSerialize(o: Record<string, unknown>): string {
    const lines: string[] = [];
    const top: [string, unknown][] = [];
    const tables: [string, unknown][] = [];
    for (const [k, v] of Object.entries(o)) {
      if (v && typeof v === "object" && !Array.isArray(v)) tables.push([k, v]);
      else top.push([k, v]);
    }
    for (const [k, v] of top) lines.push(`${k} = ${formatTomlValue(v)}`);
    for (const [k, v] of tables) {
      lines.push("");
      lines.push(`[${k}]`);
      const sub = v as Record<string, unknown>;
      const subTables: [string, Record<string, unknown>][] = [];
      for (const [k2, v2] of Object.entries(sub)) {
        if (v2 && typeof v2 === "object" && !Array.isArray(v2)) {
          subTables.push([k2, v2 as Record<string, unknown>]);
        } else {
          lines.push(`${k2} = ${formatTomlValue(v2)}`);
        }
      }
      for (const [k2, v2] of subTables) {
        lines.push("");
        lines.push(`[${k}.${k2}]`);
        for (const [k3, v3] of Object.entries(v2)) {
          lines.push(`${k3} = ${formatTomlValue(v3)}`);
        }
      }
    }
    return lines.join("\n");
  }
  function formatTomlValue(v: unknown): string {
    if (v == null) return '""';
    if (typeof v === "string") return JSON.stringify(v);
    if (typeof v === "number" || typeof v === "boolean") return String(v);
    if (Array.isArray(v))
      return `[${v.map((x) => formatTomlValue(x)).join(", ")}]`;
    return JSON.stringify(v);
  }
</script>

<SidePanel
  onClose={close}
  onBack={panelHistory.back}
  onForward={panelHistory.forward}
  canBack={panelHistory.canBack}
  canForward={panelHistory.canForward}
>
  {#snippet title()}
    {#if detail}
      <span class="title-edno">No. {editionNumber(detail.run.id)}</span>
    {:else if error}
      <span class="title-error">{error}</span>
    {/if}
  {/snippet}

  {#snippet actions()}
    {#if detail}
      <button
        type="button"
        class="iconbtn"
        onclick={() => copy(detail.run.id)}
        aria-label="Copy run id"
        title="Copy run id"
      >
        <Icon name="copy" />
      </button>
    {/if}
  {/snippet}

  {#if !detail && !error}
    <div class="loading">
      <div class="skel" style="height: 14px; width: 25%; margin-bottom: 14px"></div>
      <div class="skel" style="height: 36px; width: 70%; margin-bottom: 24px"></div>
      <div class="skel" style="height: 14px; width: 90%; margin-bottom: 8px"></div>
      <div class="skel" style="height: 14px; width: 85%; margin-bottom: 8px"></div>
      <div class="skel" style="height: 14px; width: 70%"></div>
    </div>
  {:else if error}
    <div class="error">
      <p class="headline">An edition is missing.</p>
      <p class="error-sub">{error}</p>
    </div>
  {:else if detail}
    {@const r = detail.run}
    {@const group = statusGroup(r.status)}
    {@const pulse = group === "running" || r.status === "submitted"}

    <!-- ============ MASTHEAD ============
         Edition header — small-caps top line (No. + date + byline),
         the recipe name as italic-Lora display title, and a methods one-
         liner in mono. The view's centerpiece. -->
    <header class="masthead-block">
      <div class="masthead-line">
        <span class="masthead">No. {editionNumber(r.id)}</span>
        <span class="spacer-dot">·</span>
        <span class="masthead">{formatEditionDate(r.created_at)}</span>
        <span class="spacer-dot">·</span>
        <span class="masthead">{formatEditionTime(r.created_at)}</span>
        {#if r.submitted_by}
          <span class="spacer-dot">·</span>
          <span class="masthead by">{r.submitted_by}</span>
        {/if}
      </div>

      <h1 class="title-display headline">
        <span>{r.recipe_name}</span>
        {#if r.stage_name}
          <span class="stage-tail"> / {r.stage_name}</span>
        {/if}
      </h1>

      <div class="status-line">
        <span class="dot" data-group={group} class:animate-pulse-dot={pulse}></span>
        <span class="glyph" data-group={group}>{statusGlyph(r.status)}</span>
        <span class="status-name">{shortStatus(r.status)}</span>
        <span class="spacer-dot">·</span>
        <span class="mono dur" class:live={!r.is_terminal}>{formatDuration(duration)}</span>
        <span class="spacer-dot">·</span>
        <span class="mono backend">{r.repo}@{r.recipe_hash.slice(0, 7)}</span>
      </div>
    </header>

    <!-- ============ METHODS ============
         Prose paragraph generated from the run metadata. Reads like a
         methods section: terse, factual, indicative. -->
    <section class="block first">
      <h2 class="section-h masthead">Methods</h2>
      <p class="methods">
        {#each methods as seg, i (i)}
          {#if seg.kind === "code"}<code class="mono">{seg.value}</code>{:else}{seg.value}{/if}
        {/each}
      </p>
    </section>

    <!-- ============ SPECIMENS ============
         Inputs and outputs as a single numbered list of specimens. The
         old two-column "inputs / outputs" framing is replaced by a single
         catalog ordered by role/kind; consumed vs produced is annotated
         in micro-caps. -->
    {#if detail.inputs.length || detail.outputs.length}
      <section class="block">
        <h2 class="section-h masthead">Specimens</h2>
        <ol class="spec-list">
          {#each detail.inputs as inp, i (i)}
            <li class="spec">
              <span class="spec-no masthead">CONSUMED · {String(i + 1).padStart(2, "0")}</span>
              <div class="spec-body">
                <div class="spec-top">
                  <span class="spec-kind">{inp.role}</span>
                  {#if inp.artifact_id}
                    <button
                      type="button"
                      class="spec-id mono"
                      onclick={() => router.go("artifacts", inp.artifact_id!)}
                    >{inp.artifact_id.slice(0, 14)}<span class="chev">›</span></button>
                  {:else}
                    <span class="spec-id mono unresolved">unresolved</span>
                  {/if}
                </div>
                <div class="spec-path mono">{inp.resolved_path}</div>
              </div>
            </li>
          {/each}
          {#each detail.outputs as out, i (i)}
            <li class="spec">
              <span class="spec-no masthead">PRODUCED · {String(i + 1).padStart(2, "0")}</span>
              <div class="spec-body">
                <div class="spec-top">
                  <span class="spec-kind">{out.kind}</span>
                  <button
                    type="button"
                    class="spec-id mono"
                    onclick={() => router.go("artifacts", out.id)}
                  >{out.id.slice(0, 14)}<span class="chev">›</span></button>
                </div>
                {#if out.aliases && out.aliases.length}
                  <div class="aliases">
                    {#each out.aliases as a}
                      <span class="alias mono">{a}</span>
                    {/each}
                  </div>
                {/if}
                <div class="spec-path mono">{out.path}</div>
              </div>
            </li>
          {/each}
        </ol>
      </section>
    {/if}

    <!-- ============ FIGURES ============
         Eval series as plates with figure captions in micro-caps.
         Reuses the existing EvalSeriesCard component. -->
    {#if detail.eval_series.length}
      <section class="block">
        <h2 class="section-h masthead">Figures</h2>
        <div class="figs">
          {#each detail.eval_series as series, i (series.policy_id)}
            <figure class="fig">
              <figcaption class="fig-cap masthead">
                Fig. {i + 1} · {series.metric_name ?? series.policy_id}
              </figcaption>
              <EvalSeriesCard {series} />
            </figure>
          {/each}
        </div>
      </section>
    {/if}

    <!-- ============ RESULT ============
         The eval_result artifact, if any. Surfaced because it's often
         the actual headline of the edition. -->
    {#if evalResult}
      <section class="block">
        <h2 class="section-h masthead">Result</h2>
        <Result result={evalResult} artifactId={evalOutput?.id} />
      </section>
    {/if}

    <!-- ============ RECIPE ============
         The recipe TOML, as a code block. -->
    <section class="block">
      <h2 class="section-h masthead">
        Recipe
        <span class="src mono" title={r.source_path}>{r.source_path}</span>
      </h2>
      <CodeBlock code={recipeToml} lang="toml" collapsedLines={12} />
    </section>

    <!-- ============ TIMELINE ============
         Stage events in vertical chronological order. -->
    {#if events.length}
      <section class="block">
        <h2 class="section-h masthead">Timeline</h2>
        <ol class="timeline">
          {#each events as ev}
            <li class="ev">
              <span class="ev-ts mono" title={formatAbsolute(ev.created_at)}>{formatRelative(ev.created_at, nowSecs.value)}</span>
              <span class="ev-type">{ev.event_type}</span>
            </li>
          {/each}
        </ol>
      </section>
    {/if}

    <!-- ============ LOG ============
         Collapsed by default; expand to read. Tail-of-N truncation is
         already done server-side. -->
    <section class="block">
      <h2 class="section-h masthead">
        Log
        {#if log?.path}
          <span class="src mono" title={log.path}>{log.path}</span>
        {/if}
        {#if log && log.lines.length}
          <button
            type="button"
            class="log-toggle"
            onclick={() => (logsOpen = !logsOpen)}
          >[ {logsOpen ? "collapse" : "expand"} ]</button>
        {/if}
      </h2>
      {#if !log}
        <div class="skel" style="height: 60px"></div>
      {:else if log.lines.length === 0}
        <p class="muted">No log file yet.</p>
      {:else if !logsOpen}
        <p class="muted">{log.lines.length} {log.lines.length === 1 ? "line" : "lines"} recorded.{#if log.truncated} Earlier lines truncated.{/if}</p>
      {:else}
        <div class="logbox">
          {#if log.truncated}
            <div class="trunc">… earlier lines truncated, showing tail of {log.lines.length}</div>
          {/if}
          <pre class="loglines"><code>{log.lines.join("\n")}</code></pre>
        </div>
      {/if}
    </section>

    <!-- ============ ACTIONS ============
         Moved from the top to here. Pen on paper, not toolbar in the
         masthead. -->
    <section class="block actions">
      {#if wandb}
        <a class="btn-secondary" href={wandb.url} target="_blank" rel="noopener" title={`${wandb.entity}/${wandb.project}`}>
          <span>Open in W&amp;B</span>
          <Icon name="external" size={12} />
        </a>
      {/if}
      {#if r.pipeline_id}
        <button
          type="button"
          class="btn-secondary"
          onclick={() => router.go("pipelines", r.pipeline_id)}
        >
          <span>View series</span>
          <Icon name="chevron-right" size={12} />
        </button>
      {/if}
      <button
        type="button"
        class="btn-secondary"
        onclick={() => router.go("recipes", r.recipe_name)}
        title={`All editions of ${r.recipe_name}`}
      >
        <span>All editions of recipe</span>
        <Icon name="chevron-right" size={12} />
      </button>
      <button
        type="button"
        class="btn-secondary"
        data-state={compareSelection.has(r.id) ? "active" : undefined}
        onclick={() => compareSelection.toggle(r.id)}
        title={compareSelection.has(r.id)
          ? "Remove from comparison"
          : "Add to comparison"}
      >
        <span>{compareSelection.has(r.id) ? "In comparison" : "Add to compare"}</span>
      </button>
    </section>

    <!-- ============ COLOPHON ============ -->
    <footer class="colophon">
      Recorded by labctl
      · run <span class="mono">{r.id}</span>
      · recipe <span class="mono">{r.recipe_hash.slice(0, 12)}</span>
      <span class="sig">— p(doom)</span>
    </footer>
  {/if}
</SidePanel>

<style>
  /* Title in the SidePanel header — just the edition number. The real
   * title (the recipe name) is the masthead's display headline. */
  .title-edno {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--fg-2);
    font-variant-numeric: tabular-nums;
  }
  .title-error {
    color: var(--status-failed-fg);
    font-size: 13px;
  }

  .loading { padding: 32px 24px; }
  .error { padding: 48px 24px; text-align: center; }
  .error .headline { font-size: 22px; color: var(--fg-0); margin: 0 0 8px 0; }
  .error-sub { font-size: 13px; color: var(--fg-2); margin: 0; }

  /* ============ Masthead block ============ */
  .masthead-block {
    padding: 32px 24px 24px;
    border-bottom: 1px solid var(--line-1);
  }
  .masthead-line {
    display: flex;
    align-items: baseline;
    gap: 8px;
    flex-wrap: wrap;
    margin-bottom: 10px;
  }
  .masthead-line .by { color: var(--fg-1); text-transform: none; letter-spacing: 0.04em; }
  .spacer-dot { color: var(--fg-3); font-size: 11px; }
  .title-display {
    font-size: 32px;
    color: var(--fg-0);
    margin: 0;
    line-height: 1.1;
    word-break: break-word;
  }
  .title-display .stage-tail {
    color: var(--fg-2);
    font-style: italic;
    font-weight: 400;
  }
  .status-line {
    margin-top: 14px;
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: var(--fg-1);
    flex-wrap: wrap;
  }
  .status-line .dot {
    width: 7px;
    height: 7px;
    border-radius: 999px;
  }
  .status-line .dot[data-group="running"]   { background: var(--status-running); }
  .status-line .dot[data-group="succeeded"] { background: var(--status-succeeded); }
  .status-line .dot[data-group="failed"]    { background: var(--status-failed); }
  .status-line .dot[data-group="pending"]   { background: var(--status-pending); }
  .status-line .dot[data-group="neutral"]   { background: var(--status-neutral); }
  .status-line .glyph {
    font-size: 13px;
    line-height: 1;
  }
  .status-line .glyph[data-group="running"]   { color: var(--status-running-fg); }
  .status-line .glyph[data-group="succeeded"] { color: var(--status-succeeded-fg); }
  .status-line .glyph[data-group="failed"]    { color: var(--status-failed-fg); }
  .status-line .glyph[data-group="pending"]   { color: var(--status-pending-fg); }
  .status-line .glyph[data-group="neutral"]   { color: var(--status-neutral-fg); }
  .status-line .status-name {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--fg-1);
  }
  .status-line .dur { color: var(--fg-1); font-variant-numeric: tabular-nums; }
  .status-line .dur.live { color: var(--fg-0); }
  .status-line .backend { color: var(--fg-2); font-size: 11px; }

  /* ============ Section blocks ============
   * Each block is separated by whitespace, with a small-caps section
   * label. No card chrome, no borders inside — sections are typeset
   * pages, not collapsed accordions. */
  .block {
    padding: 24px 24px 0;
  }
  .block.first { padding-top: 24px; }
  .section-h {
    margin: 0 0 14px 0;
    display: flex;
    align-items: baseline;
    gap: 12px;
    color: var(--fg-2);
  }
  .section-h .src {
    font-size: 11px;
    color: var(--fg-3);
    text-transform: none;
    letter-spacing: 0.02em;
    font-weight: 400;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 50%;
  }
  .log-toggle {
    background: transparent;
    border: none;
    padding: 0;
    margin-left: auto;
    cursor: pointer;
    color: var(--fg-2);
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    font-family: inherit;
  }
  .log-toggle:hover { color: var(--fg-0); }

  /* ============ Methods paragraph ============ */
  .methods {
    font-size: 15px;
    line-height: 1.65;
    color: var(--fg-0);
    margin: 0;
    max-width: 60ch;
  }
  .methods code {
    font-family: theme("fontFamily.mono");
    font-size: 13px;
    color: var(--fg-0);
    background: var(--bg-2);
    padding: 1px 5px;
    border-radius: 3px;
  }

  /* ============ Specimens list ============ */
  .spec-list {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 14px;
  }
  .spec {
    display: grid;
    grid-template-columns: 130px 1fr;
    gap: 16px;
    align-items: baseline;
  }
  .spec-no {
    color: var(--fg-3);
    font-variant-numeric: tabular-nums;
    align-self: start;
    margin-top: 2px;
  }
  .spec-body {
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
  }
  .spec-top {
    display: flex;
    align-items: baseline;
    gap: 10px;
  }
  .spec-kind {
    font-family: theme("fontFamily.mono");
    font-size: 13px;
    color: var(--fg-0);
    letter-spacing: 0.01em;
  }
  .spec-id {
    background: transparent;
    border: none;
    padding: 0;
    cursor: pointer;
    color: var(--fg-1);
    font-size: 12px;
    display: inline-flex;
    align-items: baseline;
    gap: 2px;
    transition: color var(--dur-micro) var(--ease);
  }
  .spec-id:hover { color: var(--accent-dim); }
  .spec-id.unresolved { color: var(--fg-3); cursor: default; }
  .spec-id .chev {
    font-size: 14px;
    line-height: 1;
    color: var(--fg-3);
  }
  .spec-id:hover .chev { color: var(--accent-dim); }
  .spec-path {
    font-size: 11px;
    color: var(--fg-2);
    overflow-wrap: anywhere;
    line-height: 1.5;
  }
  .aliases { display: flex; gap: 4px; flex-wrap: wrap; }
  .alias {
    font-size: 11px;
    color: var(--accent-dim);
    background: var(--accent-soft);
    padding: 1px 6px;
    border-radius: 3px;
  }

  /* ============ Figures ============ */
  .figs { display: flex; flex-direction: column; gap: 18px; }
  .fig { margin: 0; }
  .fig-cap {
    margin-bottom: 8px;
    color: var(--fg-2);
  }

  /* ============ Timeline ============ */
  .timeline {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
  }
  .ev {
    display: grid;
    grid-template-columns: 100px 1fr;
    gap: 14px;
    padding: 6px 0;
    border-bottom: 1px solid var(--line-0);
  }
  .ev:last-child { border-bottom: none; }
  .ev-ts { font-size: 11px; color: var(--fg-2); }
  .ev-type {
    font-family: theme("fontFamily.mono");
    color: var(--fg-0);
    font-size: 12px;
  }

  /* ============ Log ============ */
  .logbox {
    background: var(--bg-1);
    border: 1px solid var(--line-1);
    border-radius: 4px;
    overflow: hidden;
  }
  .trunc {
    padding: 6px 12px;
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: var(--fg-3);
    border-bottom: 1px solid var(--line-0);
    background: var(--bg-2);
  }
  .loglines {
    margin: 0;
    padding: 10px 12px;
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    line-height: 1.5;
    color: var(--fg-1);
    max-height: 360px;
    overflow: auto;
    white-space: pre;
  }
  .muted { color: var(--fg-2); font-size: 13px; margin: 0; }
  .mono { font-family: theme("fontFamily.mono"); }

  /* ============ Actions ============ */
  .actions {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    padding-top: 28px;
    padding-bottom: 8px;
  }

  /* ============ Colophon ============ */
  .colophon {
    padding: 24px;
    margin-top: 16px;
    border-top: 1px solid var(--line-1);
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--fg-3);
    line-height: 1.7;
  }
  .colophon .sig {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    font-size: 13px;
    text-transform: none;
    letter-spacing: 0;
    color: var(--fg-2);
    margin-left: 6px;
  }
  .colophon .mono { color: var(--fg-2); text-transform: none; font-weight: 400; letter-spacing: 0.02em; }
</style>
