<script lang="ts">
  // Single legend row used by PolicyDetail, Compare, Recipe, and the
  // EvalSeriesCard expanded view. Geometry is parameterized via the
  // --legend-cols custom property on the parent so the chart card and
  // its legend rows share the same column template.
  //
  // Two shapes are supported via the `recipe` prop:
  //   - with recipe: swatch | recipe | id | value | Δ | step
  //   - without:     swatch | id              | value | Δ | step
  // The parent supplies a matching --legend-cols template.

  import { router } from "../lib/router.svelte";
  import { shortId } from "../lib/format";

  interface Props {
    /** Run-level identity. */
    runId: string;
    /** When set, renders a recipe link before the id link. */
    recipe?: string;
    /** Chart color for the swatch and (visually) the line. */
    color: string;
    /** Latest value (rightmost numeric column). null → em-dash. */
    latestValue: number | null;
    /** Δ vs previous evaluation. null → em-dash. */
    delta: number | null;
    /** Latest step (printed as "step 12k" form). null → em-dash. */
    latestStep: number | null;
    /** True if this row is currently visible in the chart. */
    visible: boolean;
    /** True if a sibling row is currently focused (this one is dimmed). */
    dimmed: boolean;
    onToggleVisible: () => void;
    /** Hover handlers — mirror highlight into the chart. */
    onEnter: () => void;
    onLeave: () => void;
    /** Truncation width for the id link. */
    idLen?: number;
  }
  let {
    runId,
    recipe,
    color,
    latestValue,
    delta,
    latestStep,
    visible,
    dimmed,
    onToggleVisible,
    onEnter,
    onLeave,
    idLen = 12,
  }: Props = $props();

  function fmtValue(v: number | null): string {
    if (v == null) return "—";
    if (Math.abs(v) >= 100) return v.toFixed(1);
    if (Math.abs(v) >= 1) return v.toFixed(3);
    return v.toFixed(4);
  }
  function fmtStep(s: number | null): string {
    if (s == null) return "—";
    if (s >= 1000) return `${(s / 1000).toFixed(s % 1000 === 0 ? 0 : 1)}k`;
    return String(s);
  }
  function fmtDelta(d: number): string {
    const sign = d >= 0 ? "+" : "−";
    const abs = Math.abs(d);
    if (abs >= 1) return `${sign}${abs.toFixed(2)}`;
    if (abs >= 0.001) return `${sign}${abs.toFixed(3)}`;
    return `${sign}${abs.toFixed(4)}`;
  }
</script>

<div
  class="leg-row"
  class:is-dim={dimmed}
  class:is-hidden={!visible}
  onmouseenter={onEnter}
  onmouseleave={onLeave}
  role="presentation"
>
  <button
    type="button"
    class="leg-toggle"
    onclick={onToggleVisible}
    aria-label={visible ? "Hide this run" : "Show this run"}
    aria-pressed={visible}
    title={visible ? "Hide" : "Show"}
  >
    <span class="swatch" style={visible ? `background: ${color};` : ""}></span>
  </button>
  {#if recipe}
    <button
      type="button"
      class="leg-recipe mono"
      onclick={() => router.go("recipes", recipe)}
      title={`All runs of ${recipe}`}
    >{recipe}</button>
  {/if}
  <button
    type="button"
    class="leg-id mono"
    onclick={() => router.go("runs", runId)}
    title={runId}
  >{shortId(runId, idLen)}</button>
  <span class="leg-val mono">{fmtValue(latestValue)}</span>
  {#if delta != null}
    <span class="leg-delta mono" data-sign={delta >= 0 ? "pos" : "neg"}>
      {fmtDelta(delta)}
    </span>
  {:else}
    <span class="leg-delta mono dim">—</span>
  {/if}
  <span class="leg-step mono">step {fmtStep(latestStep)}</span>
</div>

<style>
  .leg-row {
    display: grid;
    grid-template-columns: var(--legend-cols, 22px minmax(0, 1.5fr) 96px 72px 56px 56px);
    column-gap: 12px;
    align-items: center;
    padding: 5px 4px;
    border-bottom: 1px solid var(--line-0);
    transition: color var(--dur-micro) var(--ease);
  }
  .leg-row:last-child { border-bottom: none; }
  .leg-row.is-dim {
    color: var(--fg-3);
  }
  .leg-row.is-dim .leg-recipe,
  .leg-row.is-dim .leg-id,
  .leg-row.is-dim .leg-val,
  .leg-row.is-dim .leg-step,
  .leg-row.is-dim .leg-delta {
    color: var(--fg-3);
  }
  .leg-row.is-hidden .swatch {
    background: transparent;
    border-color: var(--line-2);
  }
  .leg-row.is-hidden .leg-recipe,
  .leg-row.is-hidden .leg-id,
  .leg-row.is-hidden .leg-val,
  .leg-row.is-hidden .leg-step,
  .leg-row.is-hidden .leg-delta {
    color: var(--fg-3);
    text-decoration: line-through;
  }
  .leg-toggle {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    border: none;
    background: transparent;
    cursor: pointer;
    padding: 0;
  }
  .swatch {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 999px;
    border: 2px solid transparent;
  }
  .leg-recipe,
  .leg-id {
    background: transparent;
    border: none;
    padding: 0;
    cursor: pointer;
    font-size: 12px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    text-align: left;
  }
  .leg-recipe { color: var(--fg-0); }
  .leg-recipe:hover { color: var(--accent-dim); }
  .leg-id { color: var(--fg-2); font-size: 11px; }
  .leg-id:hover { color: var(--fg-0); }
  .leg-val {
    font-size: 13px;
    color: var(--fg-0);
    font-variant-numeric: tabular-nums;
    text-align: right;
  }
  .leg-delta {
    font-size: 11px;
    font-variant-numeric: tabular-nums;
    text-align: right;
  }
  .leg-delta[data-sign="pos"] { color: var(--status-succeeded-fg); }
  .leg-delta[data-sign="neg"] { color: var(--status-failed-fg); }
  .leg-step { font-size: 11px; color: var(--fg-3); text-align: right; }
</style>
