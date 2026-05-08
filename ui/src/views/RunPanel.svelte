<script lang="ts">
  import { store, loadRunDetail, loadRunLog, loadRunEvents } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { compareSelection } from "../lib/compare.svelte";
  import { copy, formatAbsolute } from "../lib/format";
  import { panelHistory } from "../lib/panel.svelte";

  import SidePanel from "../components/SidePanel.svelte";
  import Pill from "../components/Pill.svelte";
  import Hash from "../components/Hash.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Duration from "../components/Duration.svelte";
  import Icon from "../components/Icon.svelte";
  import CodeBlock from "../components/CodeBlock.svelte";
  import Result from "../components/Result.svelte";
  import EvalSeriesCard from "../components/EvalSeriesCard.svelte";

  interface Props {
    runId: string;
  }
  let { runId }: Props = $props();

  // SWR: read from cache synchronously (instant render if hovered first or
  // recently visited). Background fetches refresh when stale or when SSE
  // pushes invalidate the entry.
  let detail = $derived(store.runDetail(runId));
  let log = $derived(store.runLog(runId));
  let events = $derived(store.runEvents(runId) ?? []);
  let error = $state<string | null>(null);

  $effect(() => {
    if (!runId) return;
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

  /** Eval-result artifact among this run's outputs, if any. The server
   *  inlines `metadata.result` on eval_result outputs so we don't need
   *  a follow-up fetch. Surfaced prominently above the log. */
  let evalResult = $derived.by<unknown>(() => {
    const out = detail?.outputs.find((o) => o.kind === "eval_result");
    return (out as { result?: unknown } | undefined)?.result ?? null;
  });
  let recipeToml = $derived.by(() => {
    if (!detail) return "";
    return tomlSerialize(detail.run.recipe as Record<string, unknown>);
  });

  function close() {
    router.select("runs", null);
  }

  function tomlSerialize(o: Record<string, unknown>): string {
    // Lightweight TOML emitter for display — recipe shapes are well-known
    // and we don't need round-trip safety. Falls back to JSON for exotic
    // shapes the eye-test wouldn't gain anything from.
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
      <div class="title-row">
        <Pill status={detail.run.status} />
        <span class="recipe">{detail.run.recipe_name}</span>
        {#if detail.run.stage_name}
          <span class="stage">/ {detail.run.stage_name}</span>
        {/if}
      </div>
    {:else if error}
      <span class="title-error">{error}</span>
    {/if}
  {/snippet}

  {#snippet actions()}
    {#if detail}
      <button
        type="button"
        class="iconbtn"
        onclick={() => copy(detail!.run.id)}
        aria-label="Copy run id"
        title="Copy run id"
      >
        <Icon name="copy" />
      </button>
    {/if}
  {/snippet}

  {#if !detail && !error}
    <div class="loading">
      <div class="skel" style="height: 24px; width: 50%; margin-bottom: 12px"></div>
      <div class="skel" style="height: 14px; width: 80%; margin-bottom: 6px"></div>
      <div class="skel" style="height: 14px; width: 65%"></div>
    </div>
  {:else if error}
    <div class="error">
      <p>{error}</p>
    </div>
  {:else if detail}
    {@const r = detail.run}
    <section class="meta">
      <div class="meta-row">
        <span class="k">id</span>
        <span class="v"><Hash value={r.id} n={20} /></span>
      </div>
      {#if r.job_id}
        <div class="meta-row">
          <span class="k">job</span>
          <span class="v"><Hash value={r.job_id} n={16} label="job id" /></span>
        </div>
      {/if}
      <div class="meta-row">
        <span class="k">recipe hash</span>
        <span class="v"><Hash value={r.recipe_hash} n={12} /></span>
      </div>
      <div class="meta-row">
        <span class="k">repo</span>
        <span class="v mono">{r.repo}</span>
      </div>
      <div class="meta-row">
        <span class="k">started</span>
        <span class="v"><RelativeTime ts={r.created_at} /></span>
      </div>
      <div class="meta-row">
        <span class="k">duration</span>
        <span class="v"><Duration run={r} /></span>
      </div>
      {#if r.finished_at}
        <div class="meta-row">
          <span class="k">finished</span>
          <span class="v" title={formatAbsolute(r.finished_at)}>
            <RelativeTime ts={r.finished_at} />
          </span>
        </div>
      {/if}
      <div class="meta-row">
        <span class="k">run dir</span>
        <span class="v mono path" title={r.run_dir}>{r.run_dir}</span>
      </div>
    </section>

    <div class="action-row">
      {#if wandb}
        <a class="primary" href={wandb.url} target="_blank" rel="noopener" title={`${wandb.entity}/${wandb.project}`}>
          <span>Open in W&amp;B</span>
          <Icon name="external" size={12} />
        </a>
      {/if}
      {#if r.pipeline_id}
        <button
          type="button"
          class="secondary"
          onclick={() => router.go("pipelines", r.pipeline_id)}
        >
          <span>View pipeline</span>
          <Icon name="chevron-right" size={12} />
        </button>
      {/if}
      <button
        type="button"
        class="secondary"
        onclick={() => router.go("recipes", r.recipe_name)}
        title={`All runs of ${r.recipe_name}`}
      >
        <span>All runs of recipe</span>
        <Icon name="chevron-right" size={12} />
      </button>
      <button
        type="button"
        class="secondary"
        class:active={compareSelection.has(r.id)}
        onclick={() => compareSelection.toggle(r.id)}
        title={compareSelection.has(r.id)
          ? "Remove from comparison"
          : "Add to comparison (then pick another run)"}
      >
        <span>{compareSelection.has(r.id) ? "In comparison" : "Add to compare"}</span>
      </button>
    </div>

    <section class="block">
      <header class="block-h">
        <h3>Recipe</h3>
        <span class="src" title={r.source_path}>{r.source_path}</span>
      </header>
      <CodeBlock code={recipeToml} lang="toml" collapsedLines={12} />
    </section>

    {#if detail.inputs.length || detail.outputs.length}
      <section class="block io">
        <div class="iocol">
          <header class="block-h">
            <h3>Inputs</h3>
            <span class="count">{detail.inputs.length}</span>
          </header>
          {#if detail.inputs.length === 0}
            <p class="muted">none</p>
          {/if}
          {#each detail.inputs as inp}
            {#if inp.artifact_id}
              <button
                type="button"
                class="card card-clickable"
                onclick={() => router.go("artifacts", inp.artifact_id)}
              >
                <div class="card-h">
                  <span class="role">{inp.role}</span>
                  <span class="link-line">
                    <Hash value={inp.artifact_id} n={10} />
                    <Icon name="chevron-right" size={10} />
                  </span>
                </div>
                <div class="path mono">{inp.resolved_path}</div>
              </button>
            {:else}
              <div class="card">
                <div class="card-h">
                  <span class="role">{inp.role}</span>
                  <span class="muted small">unresolved</span>
                </div>
                <div class="path mono">{inp.resolved_path}</div>
              </div>
            {/if}
          {/each}
        </div>
        <div class="iocol">
          <header class="block-h">
            <h3>Outputs</h3>
            <span class="count">{detail.outputs.length}</span>
          </header>
          {#if detail.outputs.length === 0}
            <p class="muted">none</p>
          {/if}
          {#each detail.outputs as out}
            <button
              type="button"
              class="card card-clickable"
              onclick={() => router.go("artifacts", out.id)}
            >
              <div class="card-h">
                <span class="kind">{out.kind}</span>
                <span class="link-line">
                  <Hash value={out.id} n={10} />
                  <Icon name="chevron-right" size={10} />
                </span>
              </div>
              {#if out.aliases && out.aliases.length}
                <div class="aliases">
                  {#each out.aliases as a}
                    <span class="alias">{a}</span>
                  {/each}
                </div>
              {/if}
              <div class="path mono">{out.path}</div>
            </button>
          {/each}
        </div>
      </section>
    {/if}

    {#if detail.eval_series.length}
      <section class="block">
        <header class="block-h">
          <h3>Evals</h3>
          <span class="count">
            {detail.eval_series.length}
            {detail.eval_series.length === 1 ? "policy" : "policies"}
          </span>
        </header>
        <div class="evals">
          {#each detail.eval_series as series (series.policy_id)}
            <EvalSeriesCard {series} />
          {/each}
        </div>
      </section>
    {/if}

    {#if evalResult}
      <section class="block">
        <header class="block-h">
          <h3>Result</h3>
        </header>
        <Result result={evalResult} />
      </section>
    {/if}

    <section class="block">
      <header class="block-h">
        <h3>Log</h3>
        {#if log?.path}
          <span class="src mono" title={log.path}>{log.path}</span>
        {/if}
      </header>
      {#if !log}
        <div class="skel" style="height: 100px"></div>
      {:else if log.lines.length === 0}
        <p class="muted">No log file yet — usually means the job hasn't started writing stderr.</p>
      {:else}
        <div class="logbox">
          {#if log.truncated}
            <div class="trunc">… earlier lines truncated, showing tail of {log.lines.length}</div>
          {/if}
          <pre class="loglines"><code>{log.lines.join("\n")}</code></pre>
        </div>
      {/if}
    </section>

    {#if events.length}
      <section class="block">
        <header class="block-h">
          <h3>Events</h3>
          <span class="count">{events.length}</span>
        </header>
        <div class="timeline">
          {#each events as ev}
            <div class="ev">
              <span class="ts mono"><RelativeTime ts={ev.created_at} /></span>
              <span class="type">{ev.event_type}</span>
            </div>
          {/each}
        </div>
      </section>
    {/if}
  {/if}
</SidePanel>

<style>
  .title-row {
    display: flex;
    align-items: center;
    gap: 10px;
    overflow: hidden;
  }
  .recipe {
    font-family: theme("fontFamily.mono");
    font-size: 14px;
    color: theme("colors.fg.0");
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .stage {
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: theme("colors.fg.2");
  }
  .title-error {
    color: theme("colors.status.failed.fg");
    font-size: 13px;
  }

  .loading { padding: 24px 18px; }
  .error { padding: 24px 18px; color: theme("colors.status.failed.fg"); font-size: 13px; }

  .meta {
    padding: 14px 18px 6px 18px;
  }
  .meta-row {
    display: grid;
    grid-template-columns: 110px 1fr;
    align-items: baseline;
    padding: 4px 0;
    font-size: 13px;
  }
  .meta-row .k {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.fg.3");
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }
  .meta-row .v.mono {
    font-family: theme("fontFamily.mono");
    font-size: 12px;
    color: theme("colors.fg.1");
  }
  .meta-row .v.path {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .action-row {
    display: flex;
    gap: 8px;
    padding: 10px 18px 14px 18px;
    border-bottom: 1px solid theme("colors.line.0");
  }
  .primary, .secondary {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    padding: 5px 10px;
    border-radius: 4px;
    cursor: pointer;
    text-decoration: none;
  }
  .primary {
    background: theme("colors.accent.soft");
    color: theme("colors.accent.dim");
    border: 1px solid theme("colors.accent.soft");
  }
  .primary:hover {
    background: rgba(189, 242, 109, 0.18);
    border-color: theme("colors.accent.dim");
    color: theme("colors.accent.DEFAULT");
  }
  .secondary {
    background: transparent;
    color: theme("colors.fg.1");
    border: 1px solid theme("colors.line.1");
  }
  .secondary:hover {
    color: theme("colors.fg.0");
    border-color: theme("colors.line.2");
  }
  .secondary.active {
    background: theme("colors.accent.soft");
    color: theme("colors.accent.dim");
    border-color: theme("colors.accent.dim");
  }

  .block {
    padding: 16px 18px;
    border-top: 1px solid theme("colors.line.0");
  }
  .block-h {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 12px;
    margin: 0 0 10px 0;
  }
  .block-h h3 {
    font-size: 11px;
    font-family: theme("fontFamily.mono");
    color: theme("colors.fg.3");
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin: 0;
  }
  .block-h .src {
    font-size: 11px;
    color: theme("colors.fg.3");
    font-family: theme("fontFamily.mono");
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 50%;
  }
  .block-h .count {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.fg.2");
  }

  .io {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
  }
  .iocol { display: flex; flex-direction: column; gap: 8px; }
  .card {
    background: theme("colors.bg.2");
    border: 1px solid theme("colors.line.0");
    border-radius: 6px;
    padding: 8px 10px;
    text-align: left;
    width: 100%;
    color: inherit;
    font: inherit;
  }
  .card-clickable {
    cursor: pointer;
  }
  .card-clickable:hover {
    background: theme("colors.bg.3");
    border-color: theme("colors.line.1");
  }
  .card-clickable:hover .link-line {
    color: theme("colors.fg.0");
  }
  .link-line {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    color: theme("colors.fg.1");
    font-size: 11px;
    flex-shrink: 0;
  }
  .card-h {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    margin-bottom: 4px;
  }
  .role, .kind {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.fg.0");
    letter-spacing: 0.02em;
  }
  .kind { color: theme("colors.accent.dim"); }
  .link {
    background: transparent;
    border: none;
    color: theme("colors.fg.1");
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 2px;
    padding: 0;
    font-size: 11px;
  }
  .link:hover { color: theme("colors.fg.0"); }
  .aliases { display: flex; gap: 4px; flex-wrap: wrap; margin-bottom: 4px; }
  .alias {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.accent.dim");
    background: theme("colors.accent.soft");
    padding: 1px 6px;
    border-radius: 3px;
  }
  .path {
    font-size: 11px;
    color: theme("colors.fg.2");
    overflow-wrap: anywhere;
    line-height: 1.4;
  }
  .mono { font-family: theme("fontFamily.mono"); }
  .muted { color: theme("colors.fg.2"); font-size: 12px; margin: 0; }
  .small { font-size: 11px; }

  .evals { display: flex; flex-direction: column; gap: 8px; }

  .logbox {
    background: theme("colors.bg.0");
    border: 1px solid theme("colors.line.0");
    border-radius: 6px;
    overflow: hidden;
  }
  .trunc {
    padding: 6px 12px;
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.fg.3");
    border-bottom: 1px solid theme("colors.line.0");
    background: theme("colors.bg.1");
  }
  .loglines {
    margin: 0;
    padding: 10px 12px;
    font-family: theme("fontFamily.mono");
    font-size: 11.5px;
    line-height: 1.5;
    color: theme("colors.fg.1");
    max-height: 360px;
    overflow: auto;
    white-space: pre;
  }

  .timeline {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .ev {
    display: grid;
    grid-template-columns: 100px 1fr;
    gap: 10px;
    padding: 4px 0;
    border-bottom: 1px dashed theme("colors.line.0");
    font-size: 12px;
  }
  .ev:last-child { border-bottom: none; }
  .ev .ts { font-size: 11px; color: theme("colors.fg.2"); }
  .ev .type {
    font-family: theme("fontFamily.mono");
    color: theme("colors.fg.0");
    font-size: 12px;
  }
</style>
