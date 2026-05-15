<script lang="ts">
  // Browse a crowd-cast SFT dataset artifact directly from the artifact
  // panel. Replaces the standalone `data_pipeline/browse_dataset.py` Flask
  // viewer. Inline view shows headline stats + a Maximize button; the
  // maximized overlay is a two-pane layout (left: tab strip with
  // Segments / Contributors / Timeline; right: per-segment frame viewer
  // when a segment is selected).
  //
  // Empty/404 case: `api.dataset()` returns 404 when the artifact isn't a
  // browseable per-segment dataset (e.g. a Stage C/D grain payload). We
  // render a one-line note in that case so the user understands why the
  // section is otherwise empty.

  import { api } from "../lib/api";
  import type {
    DatasetSegment,
    DatasetSummary,
    SegmentDetail,
  } from "../lib/types";

  interface Props {
    artifactId: string;
  }
  let { artifactId }: Props = $props();

  type Tab = "segments" | "contributors" | "timeline";

  let data = $state<DatasetSummary | null>(null);
  let error = $state<string | null>(null);
  let notBrowseable = $state(false);
  let maximized = $state(false);
  let tab = $state<Tab>("segments");

  // Filters shared across tabs.
  let contributorFilter = $state<string | null>(null);
  let dateFrom = $state<string>("");
  let dateTo = $state<string>("");
  let textFilter = $state("");

  // Segment viewer state.
  let selectedSeg = $state<{ split: string; segment_id: string } | null>(null);
  let segDetail = $state<SegmentDetail | null>(null);
  let segError = $state<string | null>(null);
  let frameIdx = $state(0);

  $effect(() => {
    if (!artifactId) return;
    data = null;
    error = null;
    notBrowseable = false;
    maximized = false;
    api
      .dataset(artifactId)
      .then((d) => {
        data = d;
      })
      .catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : String(e);
        // 404 from the endpoint means "not browseable" — that's a normal
        // state for Stage C/D outputs, not an error.
        if (msg.startsWith("404")) {
          notBrowseable = true;
        } else {
          error = msg;
        }
      });
  });

  // Reset segment viewer when artifact or selection changes.
  $effect(() => {
    if (!artifactId) return;
    if (!selectedSeg) {
      segDetail = null;
      segError = null;
      frameIdx = 0;
      return;
    }
    const { split, segment_id } = selectedSeg;
    segDetail = null;
    segError = null;
    frameIdx = 0;
    api
      .datasetSegment(artifactId, split, segment_id)
      .then((d) => {
        segDetail = d;
      })
      .catch((e: unknown) => {
        segError = e instanceof Error ? e.message : String(e);
      });
  });

  // ── filtering ─────────────────────────────────────────────────────
  function segDateISO(s: DatasetSegment): string | null {
    if (!s.creation_time) return null;
    // creation_time examples: "2026-04-03T11:21:26.000000Z" or "...+00:00".
    // We just need YYYY-MM-DD — slice the first 10 chars after a sanity
    // check that they look like a date.
    const d = s.creation_time.slice(0, 10);
    return /^\d{4}-\d{2}-\d{2}$/.test(d) ? d : null;
  }
  function segDurationS(s: DatasetSegment): number {
    return s.target_fps > 0 ? s.n_frames / s.target_fps : 0;
  }
  function inDateRange(s: DatasetSegment): boolean {
    if (!dateFrom && !dateTo) return true;
    const d = segDateISO(s);
    if (d === null) return false;
    if (dateFrom && d < dateFrom) return false;
    if (dateTo && d > dateTo) return false;
    return true;
  }
  let segmentsFiltered = $derived.by(() => {
    const all = data?.segments ?? [];
    const q = textFilter.trim().toLowerCase();
    return all.filter((s) => {
      if (contributorFilter && s.contributor_hash !== contributorFilter) return false;
      if (!inDateRange(s)) return false;
      if (q && !`${s.segment_id} ${s.contributor_hash} ${s.split}`.toLowerCase().includes(q)) {
        return false;
      }
      return true;
    });
  });

  // ── contributors ──────────────────────────────────────────────────
  interface ContributorStats {
    hash: string;
    n_segments: number;
    n_frames: number;
    total_hours: number;
    distinct_days: number;
    mean_hours_per_active_day: number;
    max_hours_in_a_day: number;
    earliest: string | null;
    latest: string | null;
    no_op_pct: number;
  }
  let contributors = $derived.by<ContributorStats[]>(() => {
    const all = (data?.segments ?? []).filter(inDateRange);
    const by = new Map<string, DatasetSegment[]>();
    for (const s of all) {
      const k = s.contributor_hash || "(no hash)";
      if (!by.has(k)) by.set(k, []);
      by.get(k)!.push(s);
    }
    const out: ContributorStats[] = [];
    for (const [hash, segs] of by) {
      const byDay = new Map<string, number>();
      let n_frames = 0;
      let n_no_op = 0;
      let earliest: string | null = null;
      let latest: string | null = null;
      for (const s of segs) {
        n_frames += s.n_frames;
        n_no_op += s.n_no_op;
        const d = segDateISO(s);
        if (d) {
          byDay.set(d, (byDay.get(d) ?? 0) + segDurationS(s) / 3600);
          if (!earliest || d < earliest) earliest = d;
          if (!latest || d > latest) latest = d;
        }
      }
      const total_hours = Array.from(byDay.values()).reduce((a, b) => a + b, 0);
      const distinct_days = byDay.size;
      const max_h = Math.max(0, ...Array.from(byDay.values()));
      out.push({
        hash,
        n_segments: segs.length,
        n_frames,
        total_hours,
        distinct_days,
        mean_hours_per_active_day: distinct_days > 0 ? total_hours / distinct_days : 0,
        max_hours_in_a_day: max_h,
        earliest,
        latest,
        no_op_pct: n_frames > 0 ? (100 * n_no_op) / n_frames : 0,
      });
    }
    out.sort((a, b) => b.n_frames - a.n_frames);
    return out;
  });

  // ── timeline heatmap (date × contributor) ─────────────────────────
  interface HeatmapData {
    dates: string[];           // descending; continuous (no skipped days)
    contribs: string[];        // sorted by total frames desc
    cell: Map<string, number>; // key = `${date}|${contrib}`, value = hours
    totals: Map<string, number>; // key = date
    maxCell: number;
    maxTotal: number;
  }
  function dateRangeContinuous(min: string, max: string): string[] {
    const out: string[] = [];
    const dMin = new Date(min + "T00:00:00Z");
    const dMax = new Date(max + "T00:00:00Z");
    for (let t = dMin.getTime(); t <= dMax.getTime(); t += 86400000) {
      out.push(new Date(t).toISOString().slice(0, 10));
    }
    return out;
  }
  let heatmap = $derived.by<HeatmapData>(() => {
    const all = (data?.segments ?? []).filter(inDateRange);
    const cell = new Map<string, number>();
    const totals = new Map<string, number>();
    const contribFrames = new Map<string, number>();
    let dMin: string | null = null;
    let dMax: string | null = null;
    for (const s of all) {
      const d = segDateISO(s);
      if (!d) continue;
      const c = s.contributor_hash || "(no hash)";
      const k = `${d}|${c}`;
      const hrs = segDurationS(s) / 3600;
      cell.set(k, (cell.get(k) ?? 0) + hrs);
      totals.set(d, (totals.get(d) ?? 0) + hrs);
      contribFrames.set(c, (contribFrames.get(c) ?? 0) + s.n_frames);
      if (!dMin || d < dMin) dMin = d;
      if (!dMax || d > dMax) dMax = d;
    }
    const dates = dMin && dMax ? dateRangeContinuous(dMin, dMax).reverse() : [];
    const contribs = Array.from(contribFrames.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([c]) => c);
    let maxCell = 0;
    for (const v of cell.values()) if (v > maxCell) maxCell = v;
    let maxTotal = 0;
    for (const v of totals.values()) if (v > maxTotal) maxTotal = v;
    return { dates, contribs, cell, totals, maxCell, maxTotal };
  });

  // Map [0,1] intensity → CSS color drawn from the accent ramp. We blend
  // `accent.soft` (low) toward `accent.DEFAULT` (high) via opacity on a
  // single accent layer — keeps the heatmap monochrome-with-accent
  // instead of introducing a new hue.
  function heatColor(intensity: number): string {
    if (intensity <= 0) return "transparent";
    const a = 0.15 + 0.70 * Math.min(1, intensity);
    return `color-mix(in srgb, var(--accent) ${Math.round(a * 100)}%, transparent)`;
  }

  // ── viewer mode (per-segment playback) ────────────────────────────
  let viewerMode = $derived(selectedSeg !== null);
  let playing = $state(false);
  let frameHostEl = $state<HTMLDivElement | null>(null);
  let actionsListEl = $state<HTMLUListElement | null>(null);

  let currentSegIndex = $derived.by(() => {
    if (!selectedSeg) return -1;
    return segmentsFiltered.findIndex(
      (s) => s.split === selectedSeg!.split && s.segment_id === selectedSeg!.segment_id,
    );
  });
  let hasPrevSeg = $derived(currentSegIndex > 0);
  let hasNextSeg = $derived(
    currentSegIndex >= 0 && currentSegIndex < segmentsFiltered.length - 1,
  );
  function goPrevSeg() {
    if (!hasPrevSeg) return;
    const s = segmentsFiltered[currentSegIndex - 1];
    selectedSeg = { split: s.split, segment_id: s.segment_id };
  }
  function goNextSeg() {
    if (!hasNextSeg) return;
    const s = segmentsFiltered[currentSegIndex + 1];
    selectedSeg = { split: s.split, segment_id: s.segment_id };
  }

  // Windowed list of frame indices we render in the side action list.
  // 3000-row segments are common — rendering them all is laggy. A ±100
  // window keeps the list responsive while still giving plenty of
  // context around the current frame.
  let visibleActionRange = $derived.by<number[]>(() => {
    if (!segDetail) return [];
    const n = segDetail.actions.length;
    if (n === 0) return [];
    const half = 100;
    let lo = Math.max(0, frameIdx - half);
    let hi = Math.min(n - 1, frameIdx + half);
    // Expand to a stable ~2*half window when near a boundary.
    if (hi - lo + 1 < 2 * half + 1) {
      if (lo === 0) hi = Math.min(n - 1, 2 * half);
      if (hi === n - 1) lo = Math.max(0, n - 1 - 2 * half);
    }
    const out: number[] = [];
    for (let i = lo; i <= hi; i++) out.push(i);
    return out;
  });

  // Auto-scroll the action list so the current row stays visible.
  $effect(() => {
    void frameIdx;
    if (!actionsListEl) return;
    const cur = actionsListEl.querySelector(".action-row.current");
    if (cur) (cur as HTMLElement).scrollIntoView({ block: "nearest" });
  });

  // Auto-play loop. Tick rate = recording's target_fps, falling back to
  // 10 fps if the meta doesn't carry one.
  $effect(() => {
    if (!playing || !segDetail) return;
    const metaRec = segDetail.meta as Record<string, unknown>;
    const fps = Number(metaRec.target_fps) || 10;
    const interval = window.setInterval(() => {
      if (!segDetail) return;
      const n = segDetail.actions.length;
      if (frameIdx >= n - 1) {
        playing = false;
        return;
      }
      frameIdx += 1;
    }, Math.max(33, Math.round(1000 / fps)));
    return () => window.clearInterval(interval);
  });

  // Stop playback when selection changes or the overlay closes.
  $effect(() => {
    if (!selectedSeg || !maximized) playing = false;
  });

  async function toggleFullscreen() {
    if (!frameHostEl) return;
    if (document.fullscreenElement) {
      await document.exitFullscreen().catch(() => {});
    } else {
      await frameHostEl.requestFullscreen().catch(() => {});
    }
  }

  // ── keyboard ──────────────────────────────────────────────────────
  // Captured at window level in the capture phase so the scrubber's
  // built-in arrow-key handling (which would shift focus + alter value)
  // never sees keys we want for frame navigation.
  $effect(() => {
    if (!maximized) return;
    function handler(e: KeyboardEvent) {
      const t = e.target as HTMLElement | null;
      // Let text/date filters in the table-mode header receive typing.
      // Range inputs (the scrubber) intentionally aren't excepted — we
      // want to override their arrow-key behavior with ours.
      const inTextField =
        t &&
        (t.tagName === "TEXTAREA" ||
          (t.tagName === "INPUT" &&
            (t as HTMLInputElement).type !== "range"));

      if (e.key === "Escape") {
        e.preventDefault();
        if (document.fullscreenElement) return; // browser handles its own exit
        if (selectedSeg) selectedSeg = null;
        else maximized = false;
        return;
      }
      if (inTextField) return;
      if (!selectedSeg || !segDetail) return;
      const n = segDetail.actions.length;
      const max = Math.max(0, n - 1);

      const big = e.shiftKey ? 10 : 1;
      switch (e.key) {
        case "ArrowLeft":
          e.preventDefault();
          frameIdx = Math.max(0, frameIdx - big);
          return;
        case "ArrowRight":
          e.preventDefault();
          frameIdx = Math.min(max, frameIdx + big);
          return;
        case "PageUp":
          e.preventDefault();
          frameIdx = Math.max(0, frameIdx - 60);
          return;
        case "PageDown":
          e.preventDefault();
          frameIdx = Math.min(max, frameIdx + 60);
          return;
        case "Home":
          e.preventDefault();
          frameIdx = 0;
          return;
        case "End":
          e.preventDefault();
          frameIdx = max;
          return;
        case " ":
          e.preventDefault();
          playing = !playing;
          return;
        case "f":
        case "F":
          e.preventDefault();
          toggleFullscreen();
          return;
        case "[":
          e.preventDefault();
          goPrevSeg();
          return;
        case "]":
          e.preventDefault();
          goNextSeg();
          return;
      }
    }
    window.addEventListener("keydown", handler, { capture: true });
    return () => window.removeEventListener("keydown", handler, { capture: true });
  });

  function fmtHours(h: number): string {
    if (h >= 100) return h.toFixed(0);
    if (h >= 10) return h.toFixed(1);
    return h.toFixed(2);
  }
  function fmtDuration(seconds: number): string {
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    const m = seconds / 60;
    if (m < 60) return `${m.toFixed(1)}m`;
    return `${(m / 60).toFixed(2)}h`;
  }
  function shortHash(s: string, n = 12): string {
    return s.length > n ? s.slice(0, n) + "…" : s;
  }
  function pickContributor(hash: string) {
    contributorFilter = hash;
    tab = "segments";
  }
</script>

<!-- ── Inline panel section ───────────────────────────────────────── -->
{#if error}
  <p class="err">Dataset unavailable: {error}</p>
{:else if notBrowseable}
  <p class="muted">Opaque dataset — no per-segment frames to browse.</p>
{:else if !data}
  <div class="skel" style="height: 64px; border-radius: 6px;"></div>
{:else}
  <div class="inline">
    <dl class="stats">
      <div><dt>segments</dt><dd class="mono">{data.n_segments}</dd></div>
      <div><dt>contributors</dt><dd class="mono">{data.n_contributors}</dd></div>
      <div><dt>hours</dt><dd class="mono">{fmtHours(data.total_hours)}</dd></div>
      <div><dt>splits</dt><dd class="mono">{data.splits.join(" · ")}</dd></div>
    </dl>
    {#if data.date_range[0] && data.date_range[1]}
      <p class="range">
        {data.date_range[0].slice(0, 10)} → {data.date_range[1].slice(0, 10)}
      </p>
    {/if}
    <button type="button" class="browse" onclick={() => (maximized = true)}>
      Browse segments
      <span class="arrow">→</span>
    </button>
  </div>
{/if}

<!-- ── Maximized overlay ──────────────────────────────────────────── -->
{#if maximized && data}
  <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
  <div class="backdrop" onclick={() => (maximized = false)}>
    <div class="overlay" role="dialog" aria-label="Dataset explorer" onclick={(e) => e.stopPropagation()}>
      <button class="close-btn" onclick={() => (maximized = false)} aria-label="close">
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <path d="M1 1L13 13M13 1L1 13" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
        </svg>
      </button>

      {#if !viewerMode}
        <!-- ── TABLE MODE (tabbed browse, full width) ───────────────── -->
        <div class="table-mode">
          <header class="ov-head">
            <nav class="tabs" role="tablist">
              <button role="tab" aria-selected={tab === "segments"} class:active={tab === "segments"} onclick={() => (tab = "segments")}>
                Segments <span class="count mono">{segmentsFiltered.length}</span>
              </button>
              <button role="tab" aria-selected={tab === "contributors"} class:active={tab === "contributors"} onclick={() => (tab = "contributors")}>
                Contributors <span class="count mono">{contributors.length}</span>
              </button>
              <button role="tab" aria-selected={tab === "timeline"} class:active={tab === "timeline"} onclick={() => (tab = "timeline")}>
                Timeline
              </button>
            </nav>
            <div class="filters">
              <label>
                <span>from</span>
                <input type="date" bind:value={dateFrom} />
              </label>
              <label>
                <span>to</span>
                <input type="date" bind:value={dateTo} />
              </label>
              {#if tab === "segments"}
                <input class="text-filter" type="text" placeholder="Filter segments…" bind:value={textFilter} />
              {/if}
              {#if contributorFilter}
                <button type="button" class="chip" onclick={() => (contributorFilter = null)}>
                  {shortHash(contributorFilter, 10)} <span aria-hidden="true">×</span>
                </button>
              {/if}
            </div>
          </header>

          <div class="ov-body">
            {#if tab === "segments"}
              <div class="seg-table">
                <table>
                  <thead>
                    <tr>
                      <th>split</th>
                      <th>segment</th>
                      <th>contributor</th>
                      <th>date</th>
                      <th class="num">frames</th>
                      <th class="num">dur</th>
                      <th class="num">no_op</th>
                    </tr>
                  </thead>
                  <tbody>
                    {#each segmentsFiltered as s (s.split + "/" + s.segment_id)}
                      <tr
                        onclick={() => (selectedSeg = { split: s.split, segment_id: s.segment_id })}
                        role="button"
                        tabindex="0"
                        onkeydown={(e) => e.key === "Enter" && (selectedSeg = { split: s.split, segment_id: s.segment_id })}
                      >
                        <td class="split">{s.split}</td>
                        <td class="seg mono">{shortHash(s.segment_id, 40)}</td>
                        <td class="contrib mono">{shortHash(s.contributor_hash, 10)}</td>
                        <td class="mono dim">{segDateISO(s) ?? "—"}</td>
                        <td class="num mono">{s.n_frames}</td>
                        <td class="num mono dim">{fmtDuration(segDurationS(s))}</td>
                        <td class="num mono dim">{s.n_frames ? Math.round((100 * s.n_no_op) / s.n_frames) : 0}%</td>
                      </tr>
                    {:else}
                      <tr><td colspan="7" class="empty">no segments match</td></tr>
                    {/each}
                  </tbody>
                </table>
              </div>
            {:else if tab === "contributors"}
              <div class="seg-table">
                <table>
                  <thead>
                    <tr>
                      <th>contributor</th>
                      <th class="num">segs</th>
                      <th class="num">hours</th>
                      <th class="num">days</th>
                      <th class="num">mean&nbsp;h/day</th>
                      <th class="num">max&nbsp;h/day</th>
                      <th>range</th>
                      <th class="num">no_op</th>
                    </tr>
                  </thead>
                  <tbody>
                    {#each contributors as c (c.hash)}
                      <tr onclick={() => pickContributor(c.hash)} role="button" tabindex="0"
                          onkeydown={(e) => e.key === "Enter" && pickContributor(c.hash)}>
                        <td class="contrib mono">{shortHash(c.hash, 18)}</td>
                        <td class="num mono">{c.n_segments}</td>
                        <td class="num mono">{fmtHours(c.total_hours)}</td>
                        <td class="num mono">{c.distinct_days}</td>
                        <td class="num mono">{fmtHours(c.mean_hours_per_active_day)}</td>
                        <td class="num mono">{fmtHours(c.max_hours_in_a_day)}</td>
                        <td class="mono dim">{c.earliest ?? "—"} → {c.latest ?? "—"}</td>
                        <td class="num mono dim">{Math.round(c.no_op_pct)}%</td>
                      </tr>
                    {:else}
                      <tr><td colspan="8" class="empty">no contributors match</td></tr>
                    {/each}
                  </tbody>
                </table>
              </div>
            {:else if tab === "timeline"}
              <div class="heatmap">
                {#if heatmap.dates.length === 0}
                  <p class="empty">no dated segments in range</p>
                {:else}
                  <table>
                    <thead>
                      <tr>
                        <th>date</th>
                        {#each heatmap.contribs as c}
                          <th class="num mono">{shortHash(c, 8)}</th>
                        {/each}
                        <th class="num">total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {#each heatmap.dates as d}
                        {@const tot = heatmap.totals.get(d) ?? 0}
                        <tr>
                          <th class="dlabel mono">{d}</th>
                          {#each heatmap.contribs as c}
                            {@const v = heatmap.cell.get(`${d}|${c}`) ?? 0}
                            <td class="num mono"
                                style:background={heatColor(v / Math.max(heatmap.maxCell, 0.01))}
                                title={v > 0 ? `${c} · ${fmtHours(v)}h on ${d}` : ""}>
                              {v > 0 ? fmtHours(v) : ""}
                            </td>
                          {/each}
                          <td class="num mono total"
                              style:background={heatColor(tot / Math.max(heatmap.maxTotal, 0.01))}>
                            {fmtHours(tot)}
                          </td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                {/if}
              </div>
            {/if}
          </div>
        </div>
      {:else}
        <!-- ── VIEWER MODE (full-screen segment playback) ────────────── -->
        {#if segError}
          <p class="err" style="margin: 24px;">{segError}</p>
        {:else if !segDetail}
          <div class="skel" style="height: 60vh; border-radius: 6px; margin: 24px;"></div>
        {:else}
          {@const n = segDetail.actions.length}
          {@const meta = segDetail.meta as Record<string, unknown>}
          {@const cur = segDetail.actions[frameIdx] ?? ""}
          {@const fps = Number(meta.target_fps) || 0}
          <div class="viewer-mode">
            <header class="viewer-head">
              <button class="back-btn" onclick={() => (selectedSeg = null)} title="back to segments (Esc)">
                <span aria-hidden="true">←</span> back
              </button>
              <div class="bread">
                <span class="split mono">{segDetail.split}</span>
                <span class="seg-id mono" title={segDetail.segment_id}>{segDetail.segment_id}</span>
              </div>
              <div class="seg-nav">
                <button class="nav-btn" onclick={goPrevSeg} disabled={!hasPrevSeg} title="prev segment ([)">‹</button>
                <span class="dim mono">{currentSegIndex + 1} / {segmentsFiltered.length}</span>
                <button class="nav-btn" onclick={goNextSeg} disabled={!hasNextSeg} title="next segment (])">›</button>
              </div>
            </header>

            <div class="viewer-body">
              <section class="viewer-main">
                <div bind:this={frameHostEl} class="frame-host">
                  <img class="frame" src={api.datasetFrameUrl(artifactId, segDetail.split, segDetail.segment_id, frameIdx)}
                       alt="frame {frameIdx}" />
                  <span class="badge mono">frame {frameIdx} / {Math.max(0, n - 1)}</span>
                  <button class="fs-btn" onclick={toggleFullscreen} title="fullscreen frame (F)" aria-label="fullscreen">
                    <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
                      <path d="M1 5V1H5M8 1H12V5M12 8V12H8M5 12H1V8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
                    </svg>
                  </button>
                </div>

                <div class="controls">
                  <button class="nav-btn play" onclick={() => (playing = !playing)}
                          title={playing ? "pause (Space)" : "play (Space)"} aria-label="play / pause">
                    {playing ? "⏸" : "▶"}
                  </button>
                  <button class="nav-btn" onclick={() => (frameIdx = Math.max(0, frameIdx - 1))}
                          disabled={frameIdx === 0} title="prev frame (←)" aria-label="prev frame">◀</button>
                  <input type="range" class="scrubber" tabindex="-1"
                         min="0" max={Math.max(0, n - 1)} bind:value={frameIdx} />
                  <button class="nav-btn" onclick={() => (frameIdx = Math.min(n - 1, frameIdx + 1))}
                          disabled={frameIdx >= n - 1} title="next frame (→)" aria-label="next frame">▶</button>
                  <span class="time mono">
                    {fps > 0 ? `${(frameIdx / fps).toFixed(1)}s / ${(n / fps).toFixed(1)}s` : `${frameIdx} / ${Math.max(0, n - 1)}`}
                  </span>
                </div>

                <pre class="action" class:no-op={cur === "NO_OP"}>{cur || " "}</pre>

                <p class="kbd-hint mono dim">
                  ← → frame · shift ±10 · PgUp/Dn ±60 · Home/End · space play · F fullscreen · [ ] segment · esc back
                </p>
              </section>

              <aside class="viewer-side">
                <dl class="seg-meta">
                  <div><dt>contributor</dt><dd class="mono">{shortHash(String(meta.contributor_hash ?? ""), 16)}</dd></div>
                  <div><dt>recorded</dt><dd class="mono">{String(meta.creation_time ?? "—")}</dd></div>
                  <div><dt>frame size</dt><dd class="mono">{String(meta.frame_width ?? "?")}x{String(meta.frame_height ?? "?")}</dd></div>
                  <div><dt>fps</dt><dd class="mono">{String(meta.target_fps ?? "?")}</dd></div>
                  <div><dt>frames</dt><dd class="mono">{n}</dd></div>
                  <div><dt>no_op</dt>
                    <dd class="mono">{String(meta.n_no_op ?? 0)} ({n ? Math.round((100 * Number(meta.n_no_op ?? 0)) / n) : 0}%)</dd>
                  </div>
                </dl>

                <header class="side-h">actions</header>
                <ul class="actions-list" bind:this={actionsListEl}>
                  {#each visibleActionRange as i (i)}
                    {@const a = segDetail.actions[i]}
                    <li>
                      <button class="action-row" class:current={i === frameIdx} class:no-op={a === "NO_OP"}
                              onclick={() => (frameIdx = i)}>
                        <span class="frame-no mono">{i}</span>
                        <span class="action-text mono">{a || " "}</span>
                      </button>
                    </li>
                  {/each}
                </ul>
              </aside>
            </div>
          </div>
        {/if}
      {/if}
    </div>
  </div>
{/if}

<style>
  /* ── Inline ─────────────────────────────────────────────────────── */
  .err { color: theme("colors.status.failed.fg"); font-size: 12px; margin: 0; }
  .muted { color: theme("colors.fg.2"); font-size: 12px; margin: 0; }

  .inline { display: flex; flex-direction: column; gap: 10px; }
  .stats {
    display: grid; grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 6px 16px; margin: 0;
  }
  .stats > div { display: flex; flex-direction: column; gap: 1px; }
  .stats dt { font-size: 11px; color: theme("colors.fg.2"); margin: 0; }
  .stats dd { font-size: 13px; color: theme("colors.fg.0"); margin: 0; }
  .range { font-size: 11px; color: theme("colors.fg.2"); margin: 0; font-family: theme("fontFamily.mono"); }
  .browse {
    display: inline-flex; align-items: center; gap: 6px;
    align-self: flex-start;
    padding: 6px 10px;
    background: theme("colors.bg.2");
    border: 1px solid theme("colors.line.0");
    border-radius: 4px;
    color: theme("colors.fg.1");
    font-size: 12px; cursor: pointer;
  }
  .browse:hover { background: theme("colors.bg.3"); color: theme("colors.fg.0"); }
  .browse .arrow { color: theme("colors.fg.2"); }
  .browse:hover .arrow { color: theme("colors.fg.0"); }

  /* ── Maximized overlay ──────────────────────────────────────────── */
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
    width: 100%; max-width: 1600px; height: 100%;
    background: theme("colors.bg.1");
    border-radius: 8px;
    border: 1px solid theme("colors.line.1");
    display: flex;
    overflow: hidden;
    box-shadow: 0 24px 64px rgba(0, 0, 0, 0.6);
  }
  .close-btn {
    position: absolute; top: 12px; right: 12px; z-index: 2;
    width: 28px; height: 28px;
    display: flex; align-items: center; justify-content: center;
    background: theme("colors.bg.2"); border: 1px solid theme("colors.line.0");
    border-radius: 4px; color: theme("colors.fg.2"); cursor: pointer; padding: 0;
  }
  .close-btn:hover { background: theme("colors.bg.3"); color: theme("colors.fg.0"); }

  /* Table-mode and viewer-mode each fill the whole overlay; the overlay
     itself stays `display: flex` so a single child stretches naturally. */
  .table-mode, .viewer-mode {
    flex: 1 1 100%;
    display: flex; flex-direction: column;
    min-width: 0; min-height: 0;
  }
  .ov-head {
    padding: 14px 16px 10px 16px;
    border-bottom: 1px solid theme("colors.line.0");
    display: flex; flex-direction: column; gap: 10px;
  }

  .tabs { display: flex; gap: 16px; }
  .tabs button {
    background: none; border: 0; padding: 4px 0 8px 0;
    color: theme("colors.fg.2"); font-size: 13px; cursor: pointer;
    border-bottom: 2px solid transparent;
    display: inline-flex; align-items: center; gap: 6px;
  }
  .tabs button:hover { color: theme("colors.fg.0"); }
  .tabs button.active {
    color: theme("colors.fg.0");
    border-bottom-color: theme("colors.accent.DEFAULT");
  }
  .tabs .count { font-size: 11px; color: theme("colors.fg.2"); }

  .filters { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  .filters label { display: inline-flex; align-items: center; gap: 4px; color: theme("colors.fg.2"); font-size: 11px; }
  .filters input[type="date"], .filters input[type="text"] {
    background: theme("colors.bg.0"); border: 1px solid theme("colors.line.0");
    border-radius: 3px; color: theme("colors.fg.0");
    padding: 3px 6px; font-size: 11px; font-family: theme("fontFamily.mono");
  }
  .filters .text-filter { min-width: 200px; }
  .filters input:focus {
    outline: none; border-color: theme("colors.accent.DEFAULT");
  }
  .chip {
    display: inline-flex; align-items: center; gap: 6px;
    background: theme("colors.accent.soft"); color: theme("colors.accent.dim");
    border: 0; border-radius: 3px;
    padding: 2px 8px; font-size: 11px; font-family: theme("fontFamily.mono");
    cursor: pointer;
  }

  .ov-body { flex: 1; overflow-y: auto; min-height: 0; }

  .seg-table { padding: 0; }
  .seg-table table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .seg-table th {
    text-align: left; padding: 6px 10px;
    background: theme("colors.bg.1"); color: theme("colors.fg.2");
    font-weight: 500; border-bottom: 1px solid theme("colors.line.0");
    position: sticky; top: 0; z-index: 1;
  }
  .seg-table td {
    padding: 5px 10px; border-bottom: 1px solid theme("colors.line.0");
    vertical-align: top; color: theme("colors.fg.1");
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .seg-table tr[role="button"] { cursor: pointer; }
  .seg-table tr[role="button"]:hover td { background: theme("colors.bg.2"); }
  .seg-table tr.active td { background: theme("colors.accent.soft"); }
  .seg-table .num { text-align: right; font-variant-numeric: tabular-nums; }
  .seg-table .dim { color: theme("colors.fg.2"); }
  .seg-table .split {
    font-family: theme("fontFamily.mono"); font-size: 11px;
    color: theme("colors.accent.dim"); background: theme("colors.accent.soft");
    padding: 1px 5px; border-radius: 3px; display: inline-block;
  }
  .seg-table .seg { color: theme("colors.fg.0"); max-width: 280px; }
  .seg-table .contrib { color: theme("colors.fg.1"); }
  .seg-table .empty { color: theme("colors.fg.2"); text-align: center; padding: 32px; }

  /* ── Heatmap ────────────────────────────────────────────────────── */
  .heatmap { padding: 4px 0; overflow-x: auto; }
  .heatmap table { border-collapse: collapse; font-size: 11px; }
  .heatmap th {
    text-align: right; padding: 4px 6px;
    background: theme("colors.bg.1"); color: theme("colors.fg.2");
    font-weight: 500; border-bottom: 1px solid theme("colors.line.0");
    position: sticky; top: 0; z-index: 1;
    white-space: nowrap;
  }
  .heatmap th.dlabel {
    text-align: left; position: sticky; left: 0; z-index: 2;
    background: theme("colors.bg.1");
  }
  .heatmap td {
    padding: 3px 6px; border-bottom: 1px solid theme("colors.line.0");
    text-align: right; font-variant-numeric: tabular-nums;
    color: theme("colors.fg.1");
    min-width: 36px;
  }
  .heatmap td.total { font-weight: 500; color: theme("colors.fg.0"); border-left: 1px solid theme("colors.line.0"); }
  .heatmap .empty { padding: 32px; color: theme("colors.fg.2"); text-align: center; }

  /* ── Viewer mode (full-overlay segment playback) ────────────────── */
  .viewer-head {
    display: flex; align-items: center; gap: 12px;
    padding: 10px 16px;
    border-bottom: 1px solid theme("colors.line.0");
  }
  .viewer-head .back-btn {
    background: theme("colors.bg.2"); border: 1px solid theme("colors.line.0");
    border-radius: 4px; color: theme("colors.fg.1");
    padding: 4px 10px; font-size: 12px; cursor: pointer;
    display: inline-flex; align-items: center; gap: 4px;
  }
  .viewer-head .back-btn:hover { background: theme("colors.bg.3"); color: theme("colors.fg.0"); }
  .viewer-head .bread {
    flex: 1; display: flex; align-items: center; gap: 8px;
    overflow: hidden;
  }
  .viewer-head .bread .split {
    color: theme("colors.accent.dim"); background: theme("colors.accent.soft");
    padding: 1px 6px; border-radius: 3px; font-size: 11px;
  }
  .viewer-head .bread .seg-id {
    color: theme("colors.fg.0"); font-size: 12px;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }
  .viewer-head .seg-nav {
    display: inline-flex; align-items: center; gap: 8px; margin-right: 40px;
  }

  .viewer-body {
    flex: 1; display: flex; min-height: 0; overflow: hidden;
  }

  /* Main pane: frame fills available height/width; controls + action
     sit below it; the frame container itself enters browser
     fullscreen when the user hits F. */
  .viewer-main {
    flex: 1; display: flex; flex-direction: column;
    padding: 16px; gap: 10px; min-width: 0; min-height: 0;
    overflow: hidden;
  }
  .frame-host {
    flex: 1; min-height: 0;
    position: relative; border-radius: 6px; overflow: hidden;
    background: #000; line-height: 0;
    display: flex; align-items: center; justify-content: center;
  }
  .frame {
    max-width: 100%; max-height: 100%; width: auto; height: auto;
    display: block; image-rendering: pixelated;
  }
  /* Browser-fullscreen state: frame fills the viewport, controls hidden. */
  .frame-host:fullscreen { background: #000; padding: 0; border-radius: 0; }
  .frame-host:fullscreen .frame {
    max-width: 100vw; max-height: 100vh; object-fit: contain;
  }

  .badge {
    position: absolute; top: 8px; right: 38px;
    background: rgba(0, 0, 0, 0.65); color: #fff;
    font-size: 11px; padding: 2px 6px; border-radius: 4px;
    pointer-events: none;
  }
  .fs-btn {
    position: absolute; top: 8px; right: 8px;
    width: 24px; height: 24px;
    display: flex; align-items: center; justify-content: center;
    background: rgba(0, 0, 0, 0.55); border: 1px solid rgba(255, 255, 255, 0.15);
    border-radius: 4px; color: #fff; cursor: pointer; padding: 0;
  }
  .fs-btn:hover { background: rgba(0, 0, 0, 0.8); }

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
  .nav-btn.play { font-size: 12px; }
  .scrubber {
    flex: 1; appearance: none; height: 4px; background: theme("colors.bg.2");
    border-radius: 2px; outline: none;
  }
  .scrubber::-webkit-slider-thumb {
    appearance: none; width: 12px; height: 12px; border-radius: 50%;
    background: theme("colors.accent.DEFAULT"); cursor: pointer;
  }
  .scrubber::-moz-range-thumb {
    width: 12px; height: 12px; border-radius: 50%;
    background: theme("colors.accent.DEFAULT"); cursor: pointer; border: 0;
  }
  .time {
    flex-shrink: 0;
    color: theme("colors.fg.2"); font-size: 11px;
    min-width: 84px; text-align: right;
  }

  .action {
    margin: 0; padding: 8px 10px;
    background: theme("colors.bg.0"); border-radius: 4px;
    color: theme("colors.fg.0");
    font-family: theme("fontFamily.mono"); font-size: 12px;
    white-space: pre-wrap; word-break: break-all;
    min-height: 28px;
  }
  .action.no-op { color: theme("colors.fg.3"); }

  .kbd-hint { font-size: 10px; margin: 0; }

  /* Right rail: meta + windowed action list. */
  .viewer-side {
    flex: 0 0 320px;
    display: flex; flex-direction: column; min-height: 0;
    border-left: 1px solid theme("colors.line.0");
    overflow: hidden;
  }
  .seg-meta { display: flex; flex-direction: column; gap: 4px; padding: 14px 16px; margin: 0;
              border-bottom: 1px solid theme("colors.line.0"); }
  .seg-meta > div { display: grid; grid-template-columns: 90px 1fr; gap: 8px; font-size: 11px; }
  .seg-meta dt { color: theme("colors.fg.2"); margin: 0; }
  .seg-meta dd { color: theme("colors.fg.0"); margin: 0; word-break: break-all; }

  .side-h {
    padding: 8px 16px;
    font-size: 11px; color: theme("colors.fg.2");
    border-bottom: 1px solid theme("colors.line.0");
    text-transform: uppercase; letter-spacing: 0.04em;
  }
  .actions-list {
    list-style: none; margin: 0; padding: 4px 0;
    flex: 1; overflow-y: auto; min-height: 0;
  }
  .actions-list li { margin: 0; padding: 0; }
  .action-row {
    width: 100%; display: grid; grid-template-columns: 42px 1fr;
    gap: 8px; align-items: baseline;
    padding: 3px 14px;
    background: none; border: 0; cursor: pointer;
    text-align: left; color: theme("colors.fg.1");
    font-size: 11px;
  }
  .action-row:hover { background: theme("colors.bg.2"); }
  .action-row.current { background: theme("colors.accent.soft"); color: theme("colors.fg.0"); }
  .action-row.no-op .action-text { color: theme("colors.fg.3"); }
  .action-row .frame-no {
    color: theme("colors.fg.2"); text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .action-row.current .frame-no { color: theme("colors.accent.dim"); }
  .action-row .action-text {
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }

  .mono { font-family: theme("fontFamily.mono"); }
  .dim { color: theme("colors.fg.2"); }
</style>
