<script lang="ts">
  import { api } from "../lib/api";
  import type { RolloutData, RolloutStep } from "../lib/types";

  interface Props {
    artifactId: string;
  }
  let { artifactId }: Props = $props();

  let data = $state<RolloutData | null>(null);
  let error = $state<string | null>(null);
  let current = $state(0);
  let expanded = $state<Set<number>>(new Set());
  let maximized = $state(false);

  $effect(() => {
    if (!artifactId) return;
    data = null;
    error = null;
    current = 0;
    expanded = new Set();
    maximized = false;
    api.rollout(artifactId).then((d) => {
      data = d;
      if (d.frame_count > 1) current = 1;
    }).catch((e: unknown) => {
      error = e instanceof Error ? e.message : String(e);
    });
  });

  function prev() { if (data && current > 0) current--; }
  function next() { if (data && current < data.frame_count - 1) current++; }
  function goTo(n: number) { if (data && n >= 0 && n < data.frame_count) current = n; }
  function toggleExpand(n: number) {
    const s = new Set(expanded);
    if (s.has(n)) s.delete(n); else s.add(n);
    expanded = s;
  }

  function handleKey(e: KeyboardEvent) {
    if (e.key === "Escape") { maximized = false; return; }
    if (e.key === "ArrowLeft") { e.preventDefault(); prev(); }
    if (e.key === "ArrowRight") { e.preventDefault(); next(); }
  }

  function stepForFrame(frame: number): RolloutStep | null {
    return data?.steps[frame] ?? null;
  }

  function truncate(s: string, n = 120) {
    return s.length > n ? s.slice(0, n) + "…" : s;
  }

  function rewardClass(r: number) {
    if (r >= 1) return "reward-full";
    if (r > 0) return "reward-partial";
    return "reward-zero";
  }
</script>

<svelte:window onkeydown={handleKey} />

<!-- ── Inline (compact) view ───────────────────────────────────────── -->
<div class="rollout">
  {#if error}
    <p class="err">Rollout unavailable: {error}</p>
  {:else if !data}
    <div class="skel" style="height: 280px; border-radius: 6px;"></div>
  {:else}
    <div class="viewer">
      <div class="frame-wrap">
        <img class="frame" src={api.frameUrl(artifactId, current)} alt="step {current}" />
        <span class="badge">frame {current} / {data.frame_count - 1}</span>
        <button class="maximize-btn" onclick={() => maximized = true} aria-label="maximize rollout viewer" title="Maximize">
          <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
            <path d="M1 5V1H5M8 1H12V5M12 8V12H8M5 12H1V8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </button>
      </div>

      <div class="controls">
        <button class="nav-btn" onclick={prev} disabled={current === 0} aria-label="previous">◀</button>
        <div class="scrubber" role="group" aria-label="frame scrubber">
          {#each { length: data.frame_count } as _, i}
            <button class="tick" class:active={i === current} class:done={data.steps[i]?.done}
              onclick={() => goTo(i)} aria-label="frame {i}"></button>
          {/each}
        </div>
        <button class="nav-btn" onclick={next} disabled={current === data.frame_count - 1} aria-label="next">▶</button>
      </div>

      {#if stepForFrame(current)}
        {@const step = stepForFrame(current)!}
        <div class="step-info">
          <div class="step-action">{step.action}</div>
          {#if step.response && step.response !== "<reset>"}
            <button class="expand-btn" onclick={() => toggleExpand(current)}>
              {expanded.has(current) ? "hide response ▲" : "show response ▼"}
            </button>
            {#if expanded.has(current)}
              <pre class="response">{step.response}</pre>
            {/if}
          {/if}
        </div>
      {/if}
    </div>

    <div class="traj">
      <table>
        <thead>
          <tr>
            <th class="c-step">#</th>
            <th class="c-action">action</th>
            <th class="c-resp">response</th>
            <th class="c-r">reward</th>
          </tr>
        </thead>
        <tbody>
          {#each data.steps as step (step.step_num)}
            <tr class:selected={step.step_num === current} onclick={() => goTo(step.step_num)}
              role="button" tabindex="0" onkeydown={(e) => e.key === "Enter" && goTo(step.step_num)}>
              <td class="c-step mono">{step.step_num}</td>
              <td class="c-action mono">{truncate(step.action, 60)}</td>
              <td class="c-resp muted">{truncate(step.response === "<reset>" ? "—" : step.response, 80)}</td>
              <td class="c-r"><span class="reward {rewardClass(step.reward)}">{step.reward}</span></td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>

<!-- ── Maximized overlay ──────────────────────────────────────────── -->
{#if maximized && data}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div class="backdrop" onclick={() => maximized = false}>
    <div class="overlay" role="dialog" aria-label="Rollout viewer" onclick={(e) => e.stopPropagation()}>

      <!-- close button -->
      <button class="close-btn" onclick={() => maximized = false} aria-label="close">
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <path d="M1 1L13 13M13 1L1 13" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
        </svg>
      </button>

      <!-- left: frame + controls -->
      <div class="ov-left">
        <div class="ov-frame-wrap">
          <img class="ov-frame" src={api.frameUrl(artifactId, current)} alt="step {current}" />
          <span class="badge">frame {current} / {data.frame_count - 1}</span>
        </div>

        <div class="controls" style="padding: 0 4px;">
          <button class="nav-btn" onclick={prev} disabled={current === 0} aria-label="previous">◀</button>
          <div class="scrubber" role="group" aria-label="frame scrubber">
            {#each { length: data.frame_count } as _, i}
              <button class="tick" class:active={i === current} class:done={data.steps[i]?.done}
                onclick={() => goTo(i)} aria-label="frame {i}"></button>
            {/each}
          </div>
          <button class="nav-btn" onclick={next} disabled={current === data.frame_count - 1} aria-label="next">▶</button>
        </div>

        {#if stepForFrame(current)}
          {@const step = stepForFrame(current)!}
          <div class="step-info">
            <div class="step-action">{step.action}</div>
            {#if step.response && step.response !== "<reset>"}
              <button class="expand-btn" onclick={() => toggleExpand(current)}>
                {expanded.has(current) ? "hide response ▲" : "show response ▼"}
              </button>
              {#if expanded.has(current)}
                <pre class="response">{step.response}</pre>
              {/if}
            {/if}
          </div>
        {/if}
      </div>

      <!-- right: trajectory table -->
      <div class="ov-right">
        <div class="traj">
          <table>
            <thead>
              <tr>
                <th class="c-step">#</th>
                <th class="c-action">action</th>
                <th class="c-resp">response</th>
                <th class="c-r">reward</th>
              </tr>
            </thead>
            <tbody>
              {#each data.steps as step (step.step_num)}
                <tr class:selected={step.step_num === current} onclick={() => goTo(step.step_num)}
                  role="button" tabindex="0" onkeydown={(e) => e.key === "Enter" && goTo(step.step_num)}>
                  <td class="c-step mono">{step.step_num}</td>
                  <td class="c-action mono">{truncate(step.action, 40)}</td>
                  <td class="c-resp muted">{truncate(step.response === "<reset>" ? "—" : step.response, 120)}</td>
                  <td class="c-r"><span class="reward {rewardClass(step.reward)}">{step.reward}</span></td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </div>

    </div>
  </div>
{/if}

<style>
  /* ── Inline ───────────────────────────────────────────────────── */
  .rollout { display: flex; flex-direction: column; gap: 12px; }
  .err { color: theme("colors.status.failed.DEFAULT"); font-size: 12px; }

  .viewer { display: flex; flex-direction: column; gap: 8px; }

  .frame-wrap {
    position: relative; border-radius: 6px; overflow: hidden;
    background: #000; line-height: 0;
  }
  .frame { width: 100%; height: auto; display: block; }

  .badge {
    position: absolute; top: 8px; right: 36px;
    background: rgba(0,0,0,0.65); color: #fff;
    font-size: 11px; font-family: theme("fontFamily.mono");
    padding: 2px 6px; border-radius: 4px;
  }

  .maximize-btn {
    position: absolute; top: 8px; right: 8px;
    width: 24px; height: 24px;
    display: flex; align-items: center; justify-content: center;
    background: rgba(0,0,0,0.55); border: 1px solid rgba(255,255,255,0.15);
    border-radius: 4px; color: #fff; cursor: pointer; padding: 0;
    transition: background 0.1s;
  }
  .maximize-btn:hover { background: rgba(0,0,0,0.8); }

  .controls { display: flex; align-items: center; gap: 8px; }
  .nav-btn {
    flex-shrink: 0; width: 28px; height: 28px;
    display: flex; align-items: center; justify-content: center;
    border-radius: 4px; border: 1px solid theme("colors.line.0");
    background: theme("colors.bg.1"); color: theme("colors.fg.1");
    cursor: pointer; font-size: 11px;
  }
  .nav-btn:hover:not(:disabled) { background: theme("colors.bg.2"); }
  .nav-btn:disabled { opacity: 0.35; cursor: default; }

  .scrubber { flex: 1; display: flex; align-items: center; gap: 3px; flex-wrap: wrap; }
  .tick {
    width: 14px; height: 14px; border-radius: 3px;
    border: 1px solid theme("colors.line.0");
    background: theme("colors.bg.1"); cursor: pointer; padding: 0;
  }
  .tick:hover { background: theme("colors.bg.2"); }
  .tick.active { background: theme("colors.accent.DEFAULT"); border-color: theme("colors.accent.DEFAULT"); }
  .tick.done { border-color: theme("colors.status.succeeded.DEFAULT"); }

  .step-info {
    background: theme("colors.bg.1"); border-radius: 6px;
    padding: 8px 10px; display: flex; flex-direction: column; gap: 4px;
  }
  .step-action { font-size: 12px; font-family: theme("fontFamily.mono"); color: theme("colors.fg.0"); word-break: break-all; }
  .expand-btn {
    font-size: 11px; color: theme("colors.fg.2");
    background: none; border: none; cursor: pointer; padding: 0; text-align: left;
  }
  .expand-btn:hover { color: theme("colors.fg.1"); }
  .response {
    font-size: 11px; font-family: theme("fontFamily.mono"); color: theme("colors.fg.1");
    white-space: pre-wrap; word-break: break-word; margin: 0;
    max-height: 200px; overflow-y: auto;
    background: theme("colors.bg.0"); border-radius: 4px; padding: 6px 8px;
  }

  .traj { overflow-x: auto; border-radius: 6px; border: 1px solid theme("colors.line.0"); }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th {
    text-align: left; padding: 5px 8px;
    background: theme("colors.bg.1"); color: theme("colors.fg.2");
    font-weight: 500; border-bottom: 1px solid theme("colors.line.0"); white-space: nowrap;
  }
  td { padding: 5px 8px; border-bottom: 1px solid theme("colors.line.0"); vertical-align: top; color: theme("colors.fg.1"); }
  tr:last-child td { border-bottom: none; }
  tr[role="button"] { cursor: pointer; }
  tr[role="button"]:hover td { background: theme("colors.bg.1"); }
  tr.selected td { background: theme("colors.accent.soft"); }

  .c-step { width: 32px; text-align: right; }
  .c-action { min-width: 120px; max-width: 220px; word-break: break-all; }
  .c-resp { min-width: 160px; word-break: break-word; }
  .c-r { width: 60px; text-align: right; }

  .reward { font-family: theme("fontFamily.mono"); font-size: 11px; }
  .reward-full { color: theme("colors.status.succeeded.DEFAULT"); }
  .reward-partial { color: theme("colors.status.running.DEFAULT"); }
  .reward-zero { color: theme("colors.fg.3"); }
  .muted { color: theme("colors.fg.2"); }
  .mono { font-family: theme("fontFamily.mono"); }

  /* ── Maximized overlay ────────────────────────────────────────── */
  .backdrop {
    position: fixed; inset: 0; z-index: 9999;
    background: rgba(0, 0, 0, 0.72);
    display: flex; align-items: center; justify-content: center;
    padding: 24px;
    animation: fade-in 120ms ease;
  }
  @keyframes fade-in { from { opacity: 0; } to { opacity: 1; } }

  .overlay {
    position: relative;
    width: 100%; max-width: 1400px;
    height: 100%;
    background: theme("colors.bg.1");
    border-radius: 8px;
    border: 1px solid theme("colors.line.1");
    display: flex;
    overflow: hidden;
    box-shadow: 0 24px 64px rgba(0,0,0,0.6);
  }

  .close-btn {
    position: absolute; top: 12px; right: 12px; z-index: 1;
    width: 28px; height: 28px;
    display: flex; align-items: center; justify-content: center;
    background: theme("colors.bg.2"); border: 1px solid theme("colors.line.0");
    border-radius: 4px; color: theme("colors.fg.2"); cursor: pointer; padding: 0;
  }
  .close-btn:hover { background: theme("colors.bg.3"); color: theme("colors.fg.0"); }

  .ov-left {
    flex: 1 1 60%;
    display: flex; flex-direction: column; gap: 10px;
    padding: 16px; overflow-y: auto;
    border-right: 1px solid theme("colors.line.0");
  }

  .ov-frame-wrap {
    position: relative; border-radius: 6px; overflow: hidden;
    background: #000; line-height: 0; flex-shrink: 0;
  }
  .ov-frame { width: 100%; height: auto; display: block; max-height: calc(100vh - 200px); object-fit: contain; }

  .ov-right {
    flex: 0 0 380px;
    overflow-y: auto;
    padding: 0;
  }
  .ov-right .traj {
    border: none; border-radius: 0; height: 100%;
  }
  .ov-right table { font-size: 13px; }
  .ov-right th { position: sticky; top: 0; z-index: 1; }
</style>
