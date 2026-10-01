<script lang="ts">
  // One environment, as a single scrolling page with an outline. Agent
  // side first — the prompt, the workspace, the sandbox — then, behind the
  // grader toggle (`?grader=1`), what the agent never sees: rubrics,
  // recorded provenance and the reference solution. Then the commits that
  // touched the task. Everything is read at one commit (`?rev=`).
  import { api } from "../lib/api";
  import { router } from "../lib/router.svelte";
  import { errorMessage, formatBytes, formatCommitDate, formatSecs, rubricSummary, shortRev } from "../lib/envs";
  import type { EnvRubricGroup, EnvTaskResponse } from "../lib/types";
  import DetailHeader from "../components/DetailHeader.svelte";
  import EmptyState from "../components/EmptyState.svelte";
  import FileBrowser from "../components/FileBrowser.svelte";
  import Markdown from "../components/Markdown.svelte";
  import CodeBlock from "../components/CodeBlock.svelte";
  import RevisionPicker from "../components/RevisionPicker.svelte";
  import Icon from "../components/Icon.svelte";

  interface Props {
    taskKey: string;
  }
  let { taskKey }: Props = $props();

  let rev = $derived(router.query.get("rev"));
  let grader = $derived(router.query.get("grader") === "1");

  let data = $state<EnvTaskResponse | null>(null);
  let error = $state<string | null>(null);
  let defaultRev = $state<string | null>(null);

  // Not reactive on purpose: the effect must not depend on `data`.
  let shownId = "";
  $effect(() => {
    const [k, r, g] = [taskKey, rev, grader];
    // Keep the page up while only the grader side changes; blank it for
    // a different task or revision.
    const id = `${k}@${r ?? ""}`;
    if (id !== shownId) data = null;
    shownId = id;
    error = null;
    let cancel = false;
    api
      .envTask(k, r ?? undefined, g)
      .then((d) => {
        if (!cancel) data = d;
      })
      .catch((e) => {
        if (!cancel) error = errorMessage(e);
      });
    return () => {
      cancel = true;
    };
  });

  // The picker wants to know which rev is the default; the list endpoint
  // has it and is cheap (cached by sha server-side).
  $effect(() => {
    if (defaultRev) return;
    api.envs().then((d) => {
      if (d.configured) defaultRev = d.default_rev;
    }).catch(() => {});
  });

  let d = $derived(data?.detail ?? null);
  let task = $derived(d?.task ?? null);
  let title = $derived(task?.metadata?.name ?? taskKey);
  let workspaceFiles = $derived(d?.files.filter((f) => f.area === "workspace") ?? []);
  let g = $derived(d?.grader ?? null);
  let summary = $derived(rubricSummary(g?.rubrics));

  function back() {
    const q = new URLSearchParams();
    if (rev) q.set("rev", rev);
    router.go("envs", null, q);
  }
  function setRev(r: string | null) {
    router.setQuery({ rev: r });
  }
  function setGrader(on: boolean) {
    router.setQueryReplace({ grader: on ? "1" : null });
  }

  interface Section {
    id: string;
    title: string;
    count?: number;
    grader?: boolean;
  }
  let sections = $derived.by<Section[]>(() => {
    if (!d) return [];
    const s: Section[] = [
      { id: "task", title: "The task" },
      { id: "workspace", title: "Workspace", count: workspaceFiles.length },
      { id: "sandbox", title: "Sandbox" },
    ];
    if (g) {
      s.push({ id: "grading", title: "Grading", count: summary.outcomes + summary.invariants + summary.vetoes, grader: true });
      s.push({ id: "provenance", title: "Provenance", count: g.findings?.length, grader: true });
      s.push({ id: "solution", title: "Reference solution", grader: true });
    } else {
      s.push({ id: "grader", title: "Grader side", grader: true });
    }
    s.push({ id: "history", title: "History", count: d.history.length });
    return s;
  });

  // Outline: click scrolls, scrolling highlights.
  let body = $state<HTMLDivElement | null>(null);
  let current = $state("task");
  function jump(id: string) {
    body?.querySelector(`#env-sec-${id}`)?.scrollIntoView({ behavior: "smooth", block: "start" });
  }
  $effect(() => {
    void sections;
    if (!body) return;
    const io = new IntersectionObserver(
      (entries) => {
        const top = entries.filter((e) => e.isIntersecting).sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)[0];
        if (top) current = top.target.id.replace("env-sec-", "");
      },
      { root: body, rootMargin: "0px 0px -70% 0px" },
    );
    body.querySelectorAll("section[id^='env-sec-']").forEach((el) => io.observe(el));
    return () => io.disconnect();
  });

  const ROLE_NOTE: Record<string, string> = {
    outcome: "Any route counts; the end state is what's checked. Earns points.",
    invariant: "Must hold exactly. Earns nothing; if one fails the reward is zero.",
    veto: "Anti-gaming. Earns nothing; tripping one zeros the reward.",
  };
  const EVIDENCE: Record<string, string> = {
    artifacts: "initial and final workspace",
    trace: "the agent's transcript",
    both: "workspace and transcript",
  };
  function scorer(grp: EnvRubricGroup): string {
    if (grp.scoring.type === "python") return grp.scoring.entrypoint ?? "python";
    if (grp.scoring.type === "judge") {
      const ev = grp.scoring.evidence;
      return `model judge · ${ev ? (EVIDENCE[ev] ?? ev) : "evidence unspecified"}`;
    }
    return grp.scoring.type;
  }
  function list(v: unknown): string {
    return Array.isArray(v) ? v.join(", ") : typeof v === "object" && v ? JSON.stringify(v) : String(v);
  }
  function dateRange(ds: string[] | undefined): string {
    if (!ds || ds.length === 0) return "—";
    const sorted = [...ds].sort();
    return sorted.length === 1 ? sorted[0] : `${sorted[0]} → ${sorted[sorted.length - 1]} (${sorted.length} days)`;
  }
</script>

<div class="page">
  <DetailHeader
    label="world"
    name={title}
    meta={d ? taskKey : undefined}
    backLabel="Environments"
    onBack={back}
  >
    {#snippet actions()}
      <RevisionPicker {rev} commit={data?.commit ?? null} {defaultRev} onSelect={setRev} />
      <button
        type="button"
        class="btn-secondary"
        data-state={grader ? "active" : undefined}
        aria-pressed={grader}
        title="Rubrics, provenance and reference solution: material the agent never sees"
        onclick={() => setGrader(!grader)}
      >
        <Icon name={grader ? "eye" : "eye-off"} size={13} />
        <span>{grader ? "Grader side shown" : "Grader side hidden"}</span>
      </button>
    {/snippet}
  </DetailHeader>

  {#if rev}
    <div class="revbar">
      Viewing <span class="mono">{shortRev(rev)}</span>
      {#if data}
        {#if data.commit.short !== shortRev(rev)}(<span class="mono">{data.commit.short}</span>){/if}
        · {data.commit.subject} · {formatCommitDate(data.commit.date)}
      {/if}
      <button type="button" class="link" onclick={() => setRev(null)}>Back to {defaultRev ?? "default"}</button>
    </div>
  {/if}

  {#if error}
    <EmptyState title="Couldn't open this environment.">
      {#snippet sub()}<span class="mono err">{error}</span>{/snippet}
    </EmptyState>
  {:else if !d || !task}
    <div class="loading"><div class="skel" style="height: 280px; width: 100%"></div></div>
  {:else}
    <div class="body" bind:this={body}>
      <div class="layout">
        <nav class="toc" aria-label="On this page">
          <p class="masthead">On this page</p>
          {#each sections as s (s.id)}
            <button type="button" class="o-item" data-on={current === s.id} data-grader={s.grader} onclick={() => jump(s.id)}>
              <span>{s.title}</span>
              {#if s.count != null}<span class="o-count mono">{s.count}</span>{/if}
            </button>
          {/each}
        </nav>

        <div class="sections">
          {#if d.task_error}
            <p class="err">{d.task_error}</p>
          {/if}

          <!-- The task -->
          <section id="env-sec-task">
            <header class="sec-h">
              <h2 class="headline">The task</h2>
              <span class="aside">the prompt, word for word, as the agent's first message</span>
            </header>
            <dl class="meta">
              <dt>Key</dt><dd class="mono">{taskKey}</dd>
              {#if task.metadata?.difficulty}<dt>Difficulty</dt><dd>{task.metadata.difficulty}</dd>{/if}
              {#if task.metadata?.authors?.length}
                <dt>Recorded from</dt><dd>{task.metadata.authors.map((a) => a.name).filter(Boolean).join(", ")}</dd>
              {/if}
              {#if task.metadata?.tags?.length}
                <dt>Tags</dt>
                <dd class="tags">{#each task.metadata.tags as t (t)}<span class="tag">{t}</span>{/each}</dd>
              {/if}
            </dl>
            <div class="panel prompt">
              <Markdown source={d.prompt} />
            </div>
          </section>

          <!-- Workspace -->
          <section id="env-sec-workspace">
            <header class="sec-h">
              <h2 class="headline">Workspace</h2>
              <span class="aside">
                {workspaceFiles.length} files · {formatBytes(workspaceFiles.reduce((n, f) => n + f.size, 0))}
                {#each (task.setup ?? []).filter((s) => s.type === "copy" && s.source === "workspace") as s, i (i)}
                  · copied to <span class="mono">{s.destination}</span> before the agent starts
                {/each}
              </span>
            </header>
            <FileBrowser {taskKey} files={d.files} prefix="workspace/" rev={data?.commit.sha} {grader} />
          </section>

          <!-- Sandbox -->
          <section id="env-sec-sandbox">
            <header class="sec-h">
              <h2 class="headline">Sandbox</h2>
              <span class="aside">from <span class="mono">task.json</span>{#if task.schema_version}{` · RL-Env-Contract ${task.schema_version}`}{/if}</span>
            </header>
            <dl class="meta">
              <dt>Runtime</dt><dd class="mono">{task.runtime ?? "—"}</dd>
              <dt>Agent time</dt><dd>{formatSecs(task.timeout?.agent)}</dd>
              <dt>Scoring time</dt><dd>{formatSecs(task.timeout?.scoring)}</dd>
              {#each Object.entries(task.extensions ?? {}) as [ns, ext] (ns)}
                {#each Object.entries(ext) as [k, v] (k)}
                  <dt>{k.replace(/_/g, " ")}</dt>
                  <dd class="mono">{list(v)} <span class="ns">{ns}</span></dd>
                {/each}
              {/each}
            </dl>
            {#if task.setup?.length}
              <p class="masthead sub-h">Setup</p>
              <div class="table">
                {#each task.setup as s, i (i)}
                  <div class="t-row setup">
                    <span class="mono">{s.type}</span>
                    <span class="mono">{s.source ?? ""}{#if s.destination}{` → ${s.destination}`}{/if}</span>
                    <span class="dim">{s.overwrite ? `overwrite: ${s.overwrite}` : ""}</span>
                  </div>
                {/each}
              </div>
            {/if}
            {#if task.artifacts?.length}
              <p class="masthead sub-h">Captured for scoring</p>
              <div class="table">
                {#each task.artifacts as a (a.name)}
                  <div class="t-row artifact">
                    <span class="mono">{a.name}</span>
                    <span>{a.phase}{a.required ? "" : " · optional"}</span>
                    <span class="mono">{a.source ?? a.type}</span>
                    <span class="dim mono">
                      {[a.max_files ? `≤ ${a.max_files} files` : "", a.max_bytes ? `≤ ${formatBytes(a.max_bytes)}` : ""].filter(Boolean).join(" · ")}
                    </span>
                  </div>
                {/each}
              </div>
            {/if}
          </section>

          {#if !g}
            <section id="env-sec-grader" class="hidden-note">
              <Icon name="eye-off" size={16} />
              <div>
                <p><strong>Grader side hidden.</strong> Rubrics, recorded findings, build notes and the reference solution live in <span class="mono">scoring/</span> and <span class="mono">reference/</span> and are never shown to the agent.</p>
                <button type="button" class="btn-secondary" onclick={() => setGrader(true)}>
                  <Icon name="eye" size={13} /><span>Show grader side</span>
                </button>
              </div>
            </section>
          {:else}
            <!-- Grading -->
            <section id="env-sec-grading">
              <header class="sec-h">
                <h2 class="headline">Grading</h2>
                <span class="aside grader-tag">grader side</span>
              </header>
              {#if g.rubrics}
                <p class="formula">
                  Reward = passed outcome points ÷ <strong>{summary.outcomePoints}</strong>,
                  zero if any of <strong>{summary.invariants}</strong> invariant{summary.invariants === 1 ? "" : "s"} fails,
                  zero if any of <strong>{summary.vetoes}</strong> veto{summary.vetoes === 1 ? "" : "es"} trips.
                  <span class="dim">{summary.judged} criteria judged by a model, {summary.programmatic} checked in code.</span>
                </p>
                {#each g.rubrics.rubrics as grp (grp.name)}
                  <div class="panel group" data-role={grp.role}>
                    <div class="g-head">
                      <span class="role">{grp.role}</span>
                      <span class="g-title">{grp.title ?? grp.name}</span>
                      <span class="g-scorer mono">{scorer(grp)}</span>
                    </div>
                    {#if ROLE_NOTE[grp.role]}<p class="g-note">{ROLE_NOTE[grp.role]}</p>{/if}
                    <ol class="criteria">
                      {#each grp.criteria as c (c.name)}
                        <li>
                          <div class="c-top">
                            <span class="c-name mono">{c.name}</span>
                            {#if c.points != null}<span class="pts mono">{c.points} pt{c.points === 1 ? "" : "s"}</span>{/if}
                          </div>
                          <p class="c-text">{c.criterion}</p>
                          {#if c.creative_ok}<p class="c-guide"><span class="lbl ok">Latitude</span>{c.creative_ok}</p>{/if}
                          {#if c.creative_not_ok}<p class="c-guide"><span class="lbl no">Out of bounds</span>{c.creative_not_ok}</p>{/if}
                        </li>
                      {/each}
                    </ol>
                  </div>
                {/each}
              {:else}
                <p class="dim">No <span class="mono">scoring/rubrics.json</span> at this revision.</p>
              {/if}
              {#if g.judge_system_prompt}
                <details class="panel det">
                  <summary>Judge system prompt <span class="dim mono">scaffold/judge.py</span></summary>
                  <pre class="pre">{g.judge_system_prompt}</pre>
                </details>
              {/if}
              <p class="masthead sub-h">Scorer files</p>
              <FileBrowser {taskKey} files={d.files} prefix="scoring/" rev={data?.commit.sha} grader height={420} initial="scoring/score.py" />
            </section>

            <!-- Provenance -->
            <section id="env-sec-provenance">
              <header class="sec-h">
                <h2 class="headline">Provenance</h2>
                <span class="aside grader-tag">grader side</span>
              </header>
              {#if g.provenance}
                <dl class="meta">
                  {#if g.provenance.person}<dt>Recorded</dt><dd>{g.provenance.person}</dd>{/if}
                  <dt>Dates</dt><dd class="mono">{dateRange(g.provenance.dates)}</dd>
                  {#if g.provenance.segments?.length}
                    <dt>Segments</dt><dd><span class="mono">{g.provenance.segments.length}</span>
                      <details class="inline"><summary>list</summary><span class="mono small">{g.provenance.segments.join(", ")}</span></details></dd>
                  {/if}
                  {#if g.provenance.repo || g.provenance.source_repository}
                    <dt>Source repo</dt><dd class="mono">{g.provenance.repo ?? g.provenance.source_repository}</dd>
                  {/if}
                </dl>
                {#if g.provenance.summary ?? g.provenance.description}
                  <p class="lede">{g.provenance.summary ?? g.provenance.description}</p>
                {/if}
              {/if}
              {#if g.prediction}
                <div class="panel predict">
                  <span class="masthead">Predicted difficulty</span>
                  <span class="range mono">{g.prediction.expected_low ?? "?"}–{g.prediction.expected_high ?? "?"}</span>
                  <span class="dim">before measurement</span>
                  {#if g.prediction.why}<p>{g.prediction.why}</p>{/if}
                </div>
              {/if}
              {#if g.findings?.length}
                <p class="masthead sub-h">Faults observed in the recording</p>
                <div class="table">
                  {#each g.findings as f (f.id)}
                    <div class="t-row finding">
                      <span class="mono">{f.id}</span>
                      <span>{f.symptom}</span>
                      <span class="dim small">{f.source?.replace(/_/g, " ") ?? ""}</span>
                    </div>
                  {/each}
                </div>
              {/if}
              {#if g.checks?.length}
                <p class="masthead sub-h">Independent checks</p>
                <ol class="checks">
                  {#each g.checks as c, i (i)}<li>{c}</li>{/each}
                </ol>
              {/if}
              {#if g.reproduce}
                <p class="masthead sub-h">Reproduce</p>
                <CodeBlock code={g.reproduce} lang="bash" />
              {/if}
              {#if g.notes}
                <p class="masthead sub-h">Builder notes</p>
                <div class="panel notes"><Markdown source={g.notes} /></div>
              {/if}
            </section>

            <!-- Reference solution -->
            <section id="env-sec-solution">
              <header class="sec-h">
                <h2 class="headline">Reference solution</h2>
                <span class="aside grader-tag">grader side</span>
              </header>
              <p class="aside para">
                <span class="mono">solve.sh</span> is the reference fix: the task's gate requires it to score 1.0, and an untouched workspace to score less.
                Other scripts here are kept alongside it.
              </p>
              <FileBrowser
                {taskKey}
                files={d.files}
                prefix="reference/solution/"
                rev={data?.commit.sha}
                grader
                height={460}
                initial="reference/solution/solve.sh"
              />
            </section>
          {/if}

          <!-- History -->
          <section id="env-sec-history">
            <header class="sec-h">
              <h2 class="headline">History</h2>
              <span class="aside">commits that touched this task, reachable from <span class="mono">{shortRev(data?.rev ?? "")}</span></span>
            </header>
            <div class="table">
              {#each d.history as c (c.sha)}
                <button
                  type="button"
                  class="t-row commit"
                  data-on={c.sha === data?.commit.sha}
                  disabled={!c.has_manifest}
                  title={c.has_manifest
                    ? `View the task at ${c.sha}`
                    : `${c.sha}\nNo task.json at this commit: it predates the contract layout, or removed the task`}
                  onclick={() => setRev(c.sha)}
                >
                  <span class="mono">{c.short}</span>
                  <span class="subj">
                    {c.subject}
                    {#if !c.has_manifest}<span class="no-manifest">no task.json</span>{/if}
                  </span>
                  <span class="dim">{c.author}</span>
                  <span class="dim mono">{formatCommitDate(c.date)}</span>
                </button>
              {/each}
            </div>
          </section>
        </div>
      </div>
    </div>
  {/if}
</div>

<style>
  .page {
    display: flex;
    flex-direction: column;
    height: 100%;
    background: var(--bg-0);
    overflow: hidden;
  }
  .revbar {
    padding: 6px 24px;
    font-size: 12px;
    color: var(--fg-1);
    background: var(--accent-soft);
    border-bottom: 1px solid var(--line-1);
    flex-shrink: 0;
  }
  .link {
    background: none;
    border: none;
    padding: 0;
    margin-left: 8px;
    font: inherit;
    color: var(--accent-dim);
    text-decoration: underline;
    cursor: pointer;
  }
  .loading {
    padding: 24px;
  }
  .body {
    flex: 1;
    overflow-y: auto;
  }
  .layout {
    display: grid;
    grid-template-columns: 180px minmax(0, 1fr);
    gap: 32px;
    max-width: 1280px;
    padding: 20px 24px 80px;
  }
  .toc {
    position: sticky;
    top: 20px;
    align-self: start;
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .toc .masthead {
    margin: 0 0 6px 8px;
  }
  .o-item {
    display: flex;
    justify-content: space-between;
    gap: 8px;
    padding: 4px 8px;
    border: none;
    border-left: 2px solid transparent;
    background: transparent;
    font: inherit;
    font-size: 12px;
    color: var(--fg-1);
    text-align: left;
    cursor: pointer;
  }
  .o-item:hover {
    color: var(--fg-0);
  }
  .o-item[data-on="true"] {
    color: var(--fg-0);
    border-left-color: var(--accent);
  }
  .o-item[data-grader="true"] span:first-child::after {
    content: " ·";
    color: var(--accent);
  }
  .o-count {
    font-size: 10px;
    color: var(--fg-3);
  }
  .sections {
    display: flex;
    flex-direction: column;
    gap: 40px;
    min-width: 0;
  }
  section {
    scroll-margin-top: 16px;
  }
  .sec-h {
    display: flex;
    align-items: baseline;
    gap: 12px;
    margin-bottom: 12px;
    flex-wrap: wrap;
  }
  .sec-h h2 {
    font-size: 22px;
    margin: 0;
    color: var(--fg-0);
  }
  .aside {
    font-size: 12px;
    color: var(--fg-2);
  }
  .para {
    margin: 0 0 10px;
  }
  .grader-tag {
    color: var(--accent-dim);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-size: 10px;
    font-weight: 600;
  }
  .panel {
    border: 1px solid var(--line-1);
    border-radius: 6px;
    background: var(--bg-0);
  }
  .prompt {
    padding: 18px 20px;
  }
  .notes {
    padding: 16px 18px;
  }
  .meta {
    display: grid;
    grid-template-columns: 120px minmax(0, 1fr);
    gap: 6px 16px;
    margin: 0 0 14px;
    font-size: 12.5px;
  }
  .meta dt {
    color: var(--fg-2);
    text-transform: capitalize;
  }
  .meta dd {
    margin: 0;
    color: var(--fg-0);
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .ns {
    color: var(--fg-3);
    font-size: 10px;
    margin-left: 6px;
  }
  .tags {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
  }
  .tag {
    font-size: 11px;
    color: var(--fg-1);
    background: var(--bg-2);
    border-radius: 3px;
    padding: 0 6px;
  }
  .sub-h {
    margin: 18px 0 8px;
  }
  .table {
    border: 1px solid var(--line-1);
    border-radius: 6px;
    overflow: hidden;
  }
  .t-row {
    display: grid;
    gap: 12px;
    padding: 7px 12px;
    font-size: 12px;
    align-items: baseline;
    border-bottom: 1px solid var(--line-0);
    width: 100%;
    background: transparent;
    border-left: none;
    border-right: none;
    border-top: none;
    text-align: left;
    font-family: inherit;
    color: var(--fg-0);
  }
  .t-row:last-child {
    border-bottom: none;
  }
  .setup {
    grid-template-columns: 70px minmax(0, 1fr) 140px;
  }
  .artifact {
    grid-template-columns: 150px 120px minmax(0, 1fr) 170px;
  }
  .finding {
    grid-template-columns: 70px minmax(0, 1fr) 110px;
  }
  .commit {
    grid-template-columns: 70px minmax(0, 1fr) 140px 100px;
    cursor: pointer;
  }
  .commit:hover:not(:disabled) {
    background: var(--bg-1);
  }
  .commit:disabled {
    cursor: default;
    color: var(--fg-2);
  }
  .no-manifest {
    margin-left: 8px;
    font-size: 10px;
    color: var(--fg-3);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }
  .commit[data-on="true"] {
    background: var(--accent-soft);
  }
  .subj {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .dim {
    color: var(--fg-2);
  }
  .small {
    font-size: 11px;
  }
  .hidden-note {
    display: flex;
    gap: 14px;
    align-items: flex-start;
    padding: 16px 18px;
    border: 1px dashed var(--line-2);
    border-radius: 6px;
    color: var(--fg-2);
  }
  .hidden-note p {
    margin: 0 0 10px;
    font-size: 13px;
    color: var(--fg-1);
    max-width: 70ch;
  }
  .formula {
    font-size: 13px;
    line-height: 1.6;
    margin: 0 0 14px;
    max-width: 90ch;
  }
  .formula .dim {
    display: block;
    font-size: 12px;
  }
  .group {
    margin-bottom: 12px;
    border-left: 3px solid var(--line-2);
  }
  .group[data-role="outcome"] {
    border-left-color: var(--status-succeeded);
  }
  .group[data-role="invariant"] {
    border-left-color: var(--fg-2);
  }
  .group[data-role="veto"] {
    border-left-color: var(--accent);
  }
  .g-head {
    display: flex;
    align-items: baseline;
    gap: 10px;
    padding: 10px 14px 0;
    flex-wrap: wrap;
  }
  .role {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--fg-1);
  }
  .group[data-role="veto"] .role {
    color: var(--accent-dim);
  }
  .group[data-role="outcome"] .role {
    color: var(--status-succeeded-fg);
  }
  .g-title {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-size: 15px;
    color: var(--fg-0);
    font-feature-settings: normal;
  }
  .g-scorer {
    margin-left: auto;
    font-size: 11px;
    color: var(--fg-2);
  }
  .g-note {
    margin: 4px 14px 0;
    font-size: 11.5px;
    color: var(--fg-2);
  }
  .criteria {
    list-style: none;
    margin: 0;
    padding: 6px 0 4px;
  }
  .criteria li {
    padding: 8px 14px;
    border-top: 1px solid var(--line-0);
  }
  .criteria li:first-child {
    border-top: none;
  }
  .c-top {
    display: flex;
    align-items: baseline;
    gap: 10px;
  }
  .c-name {
    font-size: 11px;
    color: var(--fg-1);
  }
  .pts {
    margin-left: auto;
    font-size: 11px;
    color: var(--fg-0);
    background: var(--bg-2);
    border-radius: 999px;
    padding: 0 7px;
  }
  .c-text {
    margin: 3px 0 0;
    font-size: 13px;
    line-height: 1.55;
    color: var(--fg-0);
    max-width: 95ch;
  }
  .c-guide {
    margin: 4px 0 0;
    font-size: 12px;
    line-height: 1.5;
    color: var(--fg-1);
    max-width: 95ch;
  }
  .lbl {
    display: inline-block;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin-right: 8px;
  }
  .lbl.ok {
    color: var(--status-succeeded-fg);
  }
  .lbl.no {
    color: var(--accent-dim);
  }
  .det {
    margin: 12px 0 0;
    padding: 8px 14px;
    font-size: 12px;
  }
  .det summary {
    cursor: pointer;
    color: var(--fg-1);
  }
  .pre {
    white-space: pre-wrap;
    font-family: theme("fontFamily.mono");
    font-size: 11.5px;
    line-height: 1.55;
    color: var(--fg-1);
    margin: 10px 0 4px;
  }
  .inline {
    display: inline;
    margin-left: 8px;
  }
  .inline summary {
    display: inline;
    cursor: pointer;
    font-size: 11px;
    color: var(--fg-2);
  }
  .lede {
    font-size: 13.5px;
    line-height: 1.65;
    max-width: 78ch;
    margin: 0 0 14px;
  }
  .predict {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 10px;
    padding: 12px 14px;
    font-size: 12px;
  }
  .predict .range {
    font-size: 16px;
    color: var(--fg-0);
  }
  .predict p {
    flex-basis: 100%;
    margin: 0;
    font-size: 12.5px;
    line-height: 1.55;
    color: var(--fg-1);
  }
  .checks {
    margin: 0;
    padding-left: 1.4em;
    list-style: decimal;
    font-size: 12.5px;
    line-height: 1.6;
  }
  .err {
    color: var(--status-failed-fg);
    font-size: 12px;
  }
</style>
