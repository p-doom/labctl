<script lang="ts">
  import { store, loadArtifactDetail } from "../lib/store.svelte";
  import { router } from "../lib/router.svelte";
  import { panelHistory } from "../lib/panel.svelte";
  import { copy } from "../lib/format";

  import SidePanel from "../components/SidePanel.svelte";
  import Hash from "../components/Hash.svelte";
  import RelativeTime from "../components/RelativeTime.svelte";
  import Icon from "../components/Icon.svelte";
  import Result from "../components/Result.svelte";

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
      <div class="title-row">
        <span class="kind">{detail.artifact.kind}</span>
        <span class="name mono">
          {detail.artifact.aliases?.[0] ?? detail.artifact.id}
        </span>
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
        onclick={() => copy(detail!.artifact.content_hash)}
        title="Copy content hash"
        aria-label="Copy content hash"
      >
        <Icon name="copy" />
      </button>
    {/if}
  {/snippet}

  {#if !detail && !error}
    <div class="loading">
      <div class="skel" style="height: 24px; width: 50%; margin-bottom: 12px"></div>
      <div class="skel" style="height: 14px; width: 80%"></div>
    </div>
  {:else if error}
    <div class="error">{error}</div>
  {:else if detail}
    <section class="meta">
      <div class="meta-row">
        <span class="k">id</span>
        <span class="v"><Hash value={detail.artifact.id} n={20} /></span>
      </div>
      <div class="meta-row">
        <span class="k">content hash</span>
        <span class="v"><Hash value={detail.artifact.content_hash} n={16} /></span>
      </div>
      {#if detail.artifact.aliases && detail.artifact.aliases.length}
        <div class="meta-row">
          <span class="k">aliases</span>
          <div class="v aliases">
            {#each detail.artifact.aliases as a}
              <span class="alias">{a}</span>
            {/each}
          </div>
        </div>
      {/if}
      <div class="meta-row">
        <span class="k">created</span>
        <span class="v"><RelativeTime ts={detail.artifact.created_at} /></span>
      </div>
      <div class="meta-row">
        <span class="k">path</span>
        <span class="v mono path" title={detail.artifact.path}>{detail.artifact.path}</span>
      </div>
    </section>

    <div class="action-row">
      <button type="button" class="primary" onclick={openLineage}>
        <span>Open lineage</span>
        <Icon name="external" size={12} />
      </button>
    </div>

    {#if detail.artifact.metadata && (detail.artifact.metadata as { result?: unknown }).result}
      <section class="block">
        <header class="block-h">
          <h3>Result</h3>
        </header>
        <Result result={(detail.artifact.metadata as { result: unknown }).result} />
      </section>
    {/if}

    <section class="block">
      <header class="block-h">
        <h3>Producer</h3>
      </header>
      {#if detail.producer}
        <button
          type="button"
          class="runlink"
          onclick={() => router.go("runs", detail!.producer!.id)}
        >
          <span class="r mono">{detail.producer.recipe_name}</span>
          {#if detail.producer.stage_name}
            <span class="s mono">/ {detail.producer.stage_name}</span>
          {/if}
          <span class="rid mono"><Hash value={detail.producer.id} n={10} /></span>
          <Icon name="chevron-right" size={12} />
        </button>
      {:else}
        <p class="muted">External or not produced by a tracked run.</p>
      {/if}
    </section>

    <section class="block">
      <header class="block-h">
        <h3>Consumers</h3>
        <span class="count">{detail.consumers.length}</span>
      </header>
      {#if detail.consumers.length === 0}
        <p class="muted">No tracked consumers yet.</p>
      {:else}
        <div class="consumers">
          {#each detail.consumers as c}
            <button
              type="button"
              class="runlink"
              onclick={() => router.go("runs", c.id)}
            >
              <span class="r mono">{c.recipe_name}</span>
              {#if (c as any).input_role}
                <span class="role mono">·{(c as any).input_role}</span>
              {/if}
              <span class="rid mono"><Hash value={c.id} n={10} /></span>
              <Icon name="chevron-right" size={12} />
            </button>
          {/each}
        </div>
      {/if}
    </section>
  {/if}
</SidePanel>

<style>
  .title-row { display: flex; align-items: center; gap: 10px; overflow: hidden; }
  .kind {
    font-family: theme("fontFamily.mono");
    font-size: 10px;
    color: theme("colors.accent.dim");
    background: theme("colors.accent.soft");
    padding: 2px 6px;
    border-radius: 3px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }
  .name {
    font-size: 14px;
    color: theme("colors.fg.0");
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .title-error { color: theme("colors.status.failed.fg"); font-size: 13px; }

  .loading { padding: 24px 18px; }
  .error { padding: 24px 18px; color: theme("colors.status.failed.fg"); font-size: 13px; }

  .meta { padding: 14px 18px 6px 18px; }
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
    overflow-wrap: anywhere;
    line-height: 1.45;
  }
  .meta-row .v.aliases { display: flex; gap: 4px; flex-wrap: wrap; }
  .alias {
    font-family: theme("fontFamily.mono");
    font-size: 11px;
    color: theme("colors.accent.dim");
    background: theme("colors.accent.soft");
    padding: 1px 6px;
    border-radius: 3px;
  }

  .action-row { padding: 10px 18px 14px 18px; border-bottom: 1px solid theme("colors.line.0"); }
  .primary {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    padding: 5px 10px;
    border-radius: 4px;
    cursor: pointer;
    background: theme("colors.bg.2");
    color: theme("colors.fg.1");
    border: 1px solid theme("colors.line.1");
  }
  .primary:hover { color: theme("colors.fg.0"); border-color: theme("colors.line.2"); }

  .block { padding: 16px 18px; border-top: 1px solid theme("colors.line.0"); }
  .block-h {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
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
  .block-h .count { font-family: theme("fontFamily.mono"); font-size: 11px; color: theme("colors.fg.2"); }

  .consumers { display: flex; flex-direction: column; gap: 4px; }
  .runlink {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 7px 10px;
    background: theme("colors.bg.2");
    border: 1px solid theme("colors.line.0");
    border-radius: 4px;
    cursor: pointer;
    color: theme("colors.fg.1");
    width: 100%;
    text-align: left;
  }
  .runlink:hover {
    background: theme("colors.bg.3");
    color: theme("colors.fg.0");
    border-color: theme("colors.line.1");
  }
  .runlink .r { color: theme("colors.fg.0"); font-size: 12px; flex: 1; }
  .runlink .s { color: theme("colors.fg.2"); font-size: 11px; }
  .runlink .role { color: theme("colors.accent.dim"); font-size: 11px; }
  .runlink .rid { color: theme("colors.fg.2"); font-size: 11px; }

  .muted { color: theme("colors.fg.2"); font-size: 12px; margin: 0; }
</style>
