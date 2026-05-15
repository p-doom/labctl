<script lang="ts">
  // Tiny inline multi-line sparkline. Renders one polyline per training
  // run, sharing y-scale so the trajectories are visually comparable.
  // The whole component fits inside a row of the policies list and is
  // deliberately decoration-free — no axes, no labels.

  import type { MetricSeriesRun } from "../lib/types";

  interface Props {
    runs: MetricSeriesRun[];
    colorFor: (idx: number) => string;
    width?: number;
    height?: number;
  }
  let { runs, colorFor, width = 160, height = 24 }: Props = $props();

  // Flatten all points to find the global y range and the global step
  // range. Skip runs with <2 points — a single-point trajectory has
  // no visible trend.
  let bounds = $derived.by(() => {
    let yMin = Infinity;
    let yMax = -Infinity;
    let xMin = Infinity;
    let xMax = -Infinity;
    let anyMulti = false;
    for (const r of runs) {
      if (r.points.length < 2) continue;
      anyMulti = true;
      for (const p of r.points) {
        if (p.value == null || p.step == null) continue;
        if (p.value < yMin) yMin = p.value;
        if (p.value > yMax) yMax = p.value;
        if (p.step < xMin) xMin = p.step;
        if (p.step > xMax) xMax = p.step;
      }
    }
    if (!anyMulti) return null;
    if (yMin === yMax) {
      // All points identical — splay symmetrically so the line lands mid-row.
      yMin -= 0.5;
      yMax += 0.5;
    }
    if (xMin === xMax) {
      xMin -= 1;
      xMax += 1;
    }
    return { yMin, yMax, xMin, xMax };
  });

  function pathFor(run: MetricSeriesRun): string {
    if (!bounds) return "";
    if (run.points.length < 2) return "";
    const { xMin, xMax, yMin, yMax } = bounds;
    const xs = xMax - xMin || 1;
    const ys = yMax - yMin || 1;
    const inset = 1.5; // keep line off the edge
    const w = width - inset * 2;
    const h = height - inset * 2;
    const parts: string[] = [];
    for (let i = 0; i < run.points.length; i++) {
      const p = run.points[i]!;
      if (p.step == null || p.value == null) continue;
      const x = inset + ((p.step - xMin) / xs) * w;
      const y = inset + h - ((p.value - yMin) / ys) * h;
      parts.push(`${parts.length === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`);
    }
    return parts.join(" ");
  }

  // When there's only one point per run we still want a visual cue.
  // Plot the single dots instead of leaving the cell blank.
  let dots = $derived.by(() => {
    if (bounds) return [];
    const collected: { x: number; y: number; color: string }[] = [];
    let i = 0;
    for (const r of runs) {
      const p = r.points[r.points.length - 1];
      if (p?.value != null) {
        collected.push({
          x: width / 2,
          y: height / 2 + (i - runs.length / 2) * 4,
          color: colorFor(i),
        });
      }
      i++;
    }
    return collected;
  });
</script>

<svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
  {#if bounds}
    {#each runs as r, i (r.run_id)}
      {@const d = pathFor(r)}
      {#if d}
        <path d={d} stroke={colorFor(i)} stroke-width="1.2" fill="none" stroke-linecap="round" stroke-linejoin="round" />
      {/if}
    {/each}
  {:else}
    {#each dots as dot, i (i)}
      <circle cx={dot.x} cy={dot.y} r="1.6" fill={dot.color} />
    {/each}
  {/if}
</svg>
