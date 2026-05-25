<script lang="ts">
  import { store } from "../lib/store.svelte";
  let cluster = $derived(store.cluster);
</script>

<div class="page">
  <article class="page-inner">
    <header class="head">
      <p class="masthead">COLOPHON</p>
      <h1 class="headline">labctl</h1>
    </header>

    <p class="lede">
      labctl is the press of <a class="wm" href="https://pdoom.org" target="_blank" rel="noopener">p(doom)</a>.
      It records the operations of the lab — every run submitted, every
      specimen deposited, every lineage edge formed. It is itself an
      instrument of the work it describes; what passes through it is
      both labor and data.
    </p>

    <p class="body">
      Editions are bound by ledger. Specimens are catalogued by content
      hash. Pipelines are series of editions, each numbered. Failures
      share the colour of the brand; this is by design.
    </p>

    {#if cluster}
      <section class="meta">
        <dl>
          <div class="row">
            <dt class="masthead">Cluster</dt>
            <dd class="mono">{cluster.name}</dd>
          </div>
          {#if cluster.registry_db}
            <div class="row">
              <dt class="masthead">Registry</dt>
              <dd class="mono">{cluster.registry_db}</dd>
            </div>
          {/if}
        </dl>
      </section>
    {/if}

    <footer class="foot">
      <p class="body">
        Released as part of
        <a href="https://pdoom.org/crowd_cast.html" target="_blank" rel="noopener">crowd-cast</a>.
      </p>
      <p class="sig">— p(doom)</p>
    </footer>
  </article>
</div>

<style>
  .page {
    height: 100%;
    overflow-y: auto;
    background: var(--bg-0);
    padding: 64px 24px 96px;
    display: flex;
    justify-content: center;
  }
  .page-inner {
    width: 100%;
    max-width: 640px;
  }
  .head { margin-bottom: 48px; }
  .head .masthead { margin: 0 0 18px 0; }
  .head .headline {
    font-size: 72px;
    color: var(--fg-0);
    margin: 0;
    line-height: 1;
    letter-spacing: -0.03em;
  }
  .lede {
    font-size: 18px;
    line-height: 1.6;
    color: var(--fg-0);
    margin: 0 0 24px 0;
  }
  .body {
    font-size: 15px;
    line-height: 1.7;
    color: var(--fg-1);
    margin: 0 0 16px 0;
  }
  a {
    color: var(--accent-dim);
    text-decoration: none;
    border-bottom: 1px solid var(--accent-soft);
    transition: border-color var(--dur-micro) var(--ease);
  }
  a:hover { border-bottom-color: var(--accent-dim); }
  a.wm {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
  }
  .meta {
    margin: 32px 0;
    padding: 24px 0;
    border-top: 1px solid var(--line-1);
    border-bottom: 1px solid var(--line-1);
  }
  dl { margin: 0; }
  .row {
    display: grid;
    grid-template-columns: 120px 1fr;
    gap: 16px;
    padding: 6px 0;
  }
  dt { color: var(--fg-2); }
  dd {
    margin: 0;
    font-size: 13px;
    color: var(--fg-1);
  }
  .mono {
    font-family: theme("fontFamily.mono");
  }
  .foot {
    margin-top: 32px;
  }
  .sig {
    font-family: theme("fontFamily.serif");
    font-style: italic;
    font-weight: 500;
    font-size: 16px;
    color: var(--fg-2);
    margin: 16px 0 0 0;
  }
</style>
