<script lang="ts">
  import { store, loadArtifactDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { panelHistory } from "../lib/panel.svelte";
  import {
    copy,
    editionNumber,
    formatEditionDate,
    formatEditionTime,
    formatAbsolute,
    formatRelative,
  } from "../lib/format";
  import { nowSecs } from "../lib/time.svelte";

  import SidePanel from "../components/SidePanel.svelte";
  import Icon from "../components/Icon.svelte";
  import Result from "../components/Result.svelte";
  import DatasetExplorer from "../components/DatasetExplorer.svelte";

  interface Props {
    artifactId: string;
  }
  let { artifactId }: Props = $props();

  let detail = $derived(store.artifactDetail(artifactId));
  let error = $state<string | null>(null);

  $effect(() => {
    if (!artifactId) return;
    loadArtifactDetail(artifactId).then(() => {
      error = null;
    }).catch((e) => {
      error = e instanceof Error ? e.message : String(e);
    });
  });

  function close() {
    router.select("artifacts", null);
  }
  function openLineage() {
    router.go("lineage", artifactId);
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
      <span class="title-edno">Cat. {editionNumber(detail.artifact.id)}</span>
    {:else if error}
      <span class="title-error">{error}</span>
    {/if}
  {/snippet}
  {#snippet actions()}
    {#if detail}
      <button
        type="button"
        class="iconbtn"
        onclick={() => copy(detail.artifact.id)}
        title="Copy specimen id"
        aria-label="Copy specimen id"
      >
        <Icon name="copy" />
      </button>
    {/if}
  {/snippet}

  {#if !detail && !error}
    <div class="loading">
      <div class="skel" style="height: 14px; width: 30%; margin-bottom: 14px"></div>
      <div class="skel" style="height: 36px; width: 70%"></div>
    </div>
  {:else if error}
    <div class="error">
      <p class="headline">A specimen is missing.</p>
      <p class="error-sub">{error}</p>
    </div>
  {:else if detail}
    {@const a = detail.artifact}

    <!-- ============ MASTHEAD ============ -->
    <header class="masthead-block">
      <div class="masthead-line">
        <span class="masthead">Cat. {editionNumber(a.id)}</span>
        <span class="spacer-dot">·</span>
        <span class="masthead">{formatEditionDate(a.created_at)}</span>
        <span class="spacer-dot">·</span>
        <span class="masthead">{formatEditionTime(a.created_at)}</span>
      </div>

      <h1 class="title-display headline">
        {#if a.aliases && a.aliases.length}
          {a.aliases[0]}
        {:else}
          <span class="kind-name">{a.kind}</span>
        {/if}
      </h1>

      <div class="meta-line">
        <span class="kind-chip">{a.kind}</span>
        <span class="spacer-dot">·</span>
        <span class="mono">{a.id.slice(0, 12)}</span>
      </div>
    </header>

    <!-- ============ PATH ============ -->
    <section class="block first">
      <h2 class="section-h masthead">Path</h2>
      <p class="path mono">{a.path}</p>
    </section>

    {#if a.aliases && a.aliases.length > 1}
      <section class="block">
        <h2 class="section-h masthead">Aliases</h2>
        <div class="aliases">
          {#each a.aliases as al}
            <span class="alias mono">{al}</span>
          {/each}
        </div>
      </section>
    {/if}

    <!-- ============ PROVENANCE ============ -->
    <section class="block">
      <h2 class="section-h masthead">Provenance</h2>
      {#if detail.producer}
        <button
          type="button"
          class="prov-row"
          onclick={() => router.go("runs", detail.producer!.id)}
        >
          <div class="prov-body">
            <span class="prov-name">{detail.producer.recipe_name}</span>
            {#if detail.producer.stage_name}
              <span class="prov-stage mono">/ {detail.producer.stage_name}</span>
            {/if}
          </div>
          <span class="prov-id mono">No. {editionNumber(detail.producer.id)}</span>
          <span class="chev">›</span>
        </button>
      {:else}
        <p class="muted">External or not produced by a tracked edition.</p>
      {/if}
    </section>

    <!-- ============ CONSUMERS ============ -->
    <section class="block">
      <h2 class="section-h masthead">
        Consumed by
        <span class="count">{detail.consumers.length}</span>
      </h2>
      {#if detail.consumers.length === 0}
        <p class="muted">No tracked consumers yet.</p>
      {:else}
        <div class="consumers">
          {#each detail.consumers as c}
            <button
              type="button"
              class="prov-row"
              onclick={() => router.go("runs", c.id)}
            >
              <div class="prov-body">
                <span class="prov-name">{c.recipe_name}</span>
                {#if (c as any).input_role}
                  <span class="prov-stage mono">· {(c as any).input_role}</span>
                {/if}
              </div>
              <span class="prov-id mono">No. {editionNumber(c.id)}</span>
              <span class="chev">›</span>
            </button>
          {/each}
        </div>
      {/if}
    </section>

    {#if detail.artifact.metadata && (detail.artifact.metadata as { result?: unknown }).result}
      <section class="block">
        <h2 class="section-h masthead">Result</h2>
        <Result result={(detail.artifact.metadata as { result: unknown }).result} />
      </section>
    {/if}

    {#if a.kind === "dataset"}
      <section class="block">
        <h2 class="section-h masthead">Browse</h2>
        <DatasetExplorer artifactId={a.id} />
      </section>
    {/if}

    <section class="block actions">
      <button type="button" class="btn-secondary" onclick={openLineage}>
        <span>Open in catalog</span>
        <Icon name="chevron-right" size={12} />
      </button>
    </section>

    <!-- ============ COLOPHON ============ -->
    <footer class="colophon">
      Recorded by labctl
      · specimen <span class="mono">{a.id}</span>
      · <span class="rel">{formatRelative(a.created_at, nowSecs.value)}</span><span class="when-abs" title={formatAbsolute(a.created_at)}></span>
      <span class="sig">— p(doom)</span>
    </footer>
  {/if}
</SidePanel>

<style>
  .title-edno {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--fg-2);
    font-variant-numeric: tabular-nums;
  }
  .title-error { color: var(--status-failed-fg); font-size: 13px; }

  .loading { padding: 32px 24px; }
  .error { padding: 48px 24px; text-align: center; }
  .error .headline { font-size: 22px; color: var(--fg-0); margin: 0 0 8px 0; }
  .error-sub { font-size: 13px; color: var(--fg-2); margin: 0; }

  /* ============ Masthead ============ */
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
  .spacer-dot { color: var(--fg-3); font-size: 11px; }
  .title-display {
    font-size: 32px;
    color: var(--fg-0);
    margin: 0;
    line-height: 1.1;
    word-break: break-word;
  }
  .title-display .kind-name {
    color: var(--fg-1);
  }
  .meta-line {
    margin-top: 12px;
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: var(--fg-1);
  }
  .kind-chip {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: var(--accent-dim);
    background: var(--accent-soft);
    padding: 2px 6px;
    border-radius: 3px;
  }

  /* ============ Section blocks ============ */
  .block { padding: 24px 24px 0; }
  .block.first { padding-top: 24px; }
  .section-h {
    margin: 0 0 12px 0;
    display: flex;
    align-items: baseline;
    gap: 10px;
    color: var(--fg-2);
  }
  .section-h .count {
    color: var(--fg-3);
    font-weight: 600;
    letter-spacing: 0.06em;
  }

  .path {
    font-size: 12px;
    color: var(--fg-1);
    overflow-wrap: anywhere;
    margin: 0;
    line-height: 1.6;
  }
  .aliases { display: flex; gap: 4px; flex-wrap: wrap; }
  .alias {
    font-size: 11px;
    color: var(--accent-dim);
    background: var(--accent-soft);
    padding: 1px 6px;
    border-radius: 3px;
  }

  /* ============ Provenance / Consumers ============ */
  .prov-row {
    display: grid;
    grid-template-columns: 1fr auto auto;
    align-items: baseline;
    gap: 12px;
    padding: 10px 0;
    background: transparent;
    border: none;
    border-bottom: 1px solid var(--line-0);
    cursor: pointer;
    color: inherit;
    font: inherit;
    text-align: left;
    width: 100%;
    transition: background-color var(--dur-micro) var(--ease);
  }
  .prov-row:last-child { border-bottom: none; }
  .prov-row:hover { background: var(--bg-1); }
  .prov-body {
    display: flex;
    align-items: baseline;
    gap: 8px;
    overflow: hidden;
    min-width: 0;
  }
  .prov-name {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    font-size: 14px;
    color: var(--fg-0);
    letter-spacing: -0.005em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-feature-settings: normal;
  }
  .prov-stage {
    font-size: 11px;
    color: var(--fg-2);
  }
  .prov-id {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--fg-2);
  }
  .chev {
    color: var(--fg-3);
    font-size: 16px;
    line-height: 1;
    transition: transform var(--dur-micro) var(--ease);
  }
  .prov-row:hover .chev { color: var(--fg-1); transform: translateX(2px); }

  .consumers { display: flex; flex-direction: column; }
  .muted { color: var(--fg-2); font-size: 13px; margin: 0; }
  .mono { font-family: theme("fontFamily.mono"); }

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
  .colophon .rel { text-transform: none; font-weight: 400; letter-spacing: 0.02em; color: var(--fg-2); }
</style>
