// Performance tests for the runs filter pipeline.
//
// Not just a pass/fail — these print timing distributions to stdout so a
// human can compare runs. They also fail if the median exceeds a hard
// budget, so regressions show up in CI.
//
// Run: `npm run test`.
//
// What's measured:
//   1. Haystack precomputation cost (runs once per allRuns change)
//   2. Filter pass with haystack lookup (runs on every keystroke)
//   3. Filter pass with the old per-row toLowerCase logic (baseline)
//   4. visibleRows = filtered.slice(start, end) (virtualization step)
//   5. End-to-end keystroke chain: trim + lowercase query + filter +
//      slice — what the user actually waits on.

import { describe, test, expect } from "vitest";
import { statusGroup } from "./format";
import type { RunSummary } from "./types";

const ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789_-";
const STATUSES = ["running", "succeeded", "failed", "pending", "submitted"] as const;
const REPOS = ["omegalax", "world-model", "rlhf-sandbox", "control-plane", "evals"];
const USERS = ["alice", "bob", "carol", "dave", "eve"];

function rng(seed: number) {
  // xorshift32 — deterministic so test runs are comparable
  let s = seed | 0;
  return () => {
    s ^= s << 13;
    s ^= s >>> 17;
    s ^= s << 5;
    return ((s >>> 0) / 0xffffffff);
  };
}

function genRuns(n: number, seed = 42): RunSummary[] {
  const r = rng(seed);
  const runs: RunSummary[] = [];
  for (let i = 0; i < n; i++) {
    const idLen = 12;
    let id = "run_";
    for (let k = 0; k < idLen; k++) id += ALPHABET[Math.floor(r() * ALPHABET.length)];
    const recipe = `recipe_${Math.floor(r() * 200).toString(36)}_${["train", "eval", "ablate", "sweep"][Math.floor(r() * 4)]}`;
    runs.push({
      id,
      recipe_name: recipe,
      recipe_hash: id.slice(4, 12),
      status: STATUSES[Math.floor(r() * STATUSES.length)] as RunSummary["status"],
      job_id: `${Math.floor(r() * 1000000)}`,
      run_dir: `/fast/labctl_runs/${id}`,
      repo: REPOS[Math.floor(r() * REPOS.length)]!,
      created_at: Date.now() / 1000 - Math.floor(r() * 86400 * 30),
      finished_at: null,
      duration_secs: null,
      pipeline_id: null,
      stage_name: r() < 0.3 ? `stage_${Math.floor(r() * 10)}` : null,
      submitted_by: USERS[Math.floor(r() * USERS.length)]!,
      is_terminal: false,
    });
  }
  return runs;
}

interface Stats {
  median: number;
  p95: number;
  p99: number;
  min: number;
  max: number;
  mean: number;
}

function measure(label: string, iterations: number, fn: () => unknown): Stats {
  // Warmup — JIT, cache fill.
  for (let i = 0; i < Math.min(5, iterations); i++) fn();
  const samples: number[] = new Array(iterations);
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    fn();
    samples[i] = performance.now() - t0;
  }
  samples.sort((a, b) => a - b);
  const pct = (p: number) => samples[Math.min(samples.length - 1, Math.floor(samples.length * p))]!;
  const median = pct(0.5);
  const p95 = pct(0.95);
  const p99 = pct(0.99);
  const min = samples[0]!;
  const max = samples[samples.length - 1]!;
  const mean = samples.reduce((a, b) => a + b, 0) / samples.length;
  // Column-aligned so multiple cases line up in CI output.
  // eslint-disable-next-line no-console
  console.log(
    `[perf] ${label.padEnd(54)} ` +
      `med=${median.toFixed(3).padStart(7)}ms  ` +
      `p95=${p95.toFixed(3).padStart(7)}ms  ` +
      `p99=${p99.toFixed(3).padStart(7)}ms  ` +
      `mean=${mean.toFixed(3).padStart(7)}ms  ` +
      `min=${min.toFixed(3).padStart(7)}ms  ` +
      `max=${max.toFixed(3).padStart(7)}ms  ` +
      `n=${iterations}`,
  );
  return { median, p95, p99, min, max, mean };
}

function buildHaystacks(runs: RunSummary[]): Map<string, string> {
  const m = new Map<string, string>();
  for (const r of runs) {
    m.set(
      r.id,
      `${r.recipe_name}\n${r.id}\n${r.stage_name ?? ""}\n${r.repo ?? ""}\n${r.submitted_by ?? ""}`.toLowerCase(),
    );
  }
  return m;
}

function filterWithHaystack(
  runs: RunSummary[],
  haystacks: Map<string, string>,
  q: string,
  statusFilter: string | null,
): RunSummary[] {
  const useText = q.length > 0;
  const useStatus = statusFilter != null;
  if (!useText && !useStatus) return runs;
  return runs.filter((r) => {
    if (useStatus && statusGroup(r.status) !== statusFilter) return false;
    if (useText && !haystacks.get(r.id)!.includes(q)) return false;
    return true;
  });
}

function filterNaive(runs: RunSummary[], q: string): RunSummary[] {
  if (!q) return runs;
  return runs.filter(
    (r) =>
      r.recipe_name.toLowerCase().includes(q) ||
      r.id.toLowerCase().includes(q) ||
      (r.stage_name?.toLowerCase().includes(q) ?? false) ||
      (r.repo?.toLowerCase().includes(q) ?? false) ||
      (r.submitted_by?.toLowerCase().includes(q) ?? false),
  );
}

// Budgets are wall-time at the median for a typical dev laptop. CI on
// shared runners is slower, so we set them generously — the goal is to
// catch order-of-magnitude regressions, not to hold a sub-millisecond
// line. Tighten if you start running these on dedicated hardware.
const BUDGET_PER_KEYSTROKE_MS = 5; // human-perceptible threshold ~50ms; aim 10× under
const BUDGET_HAYSTACK_BUILD_MS = 25; // runs once per SSE push, not per keystroke

describe("filter pipeline perf", () => {
  for (const N of [1000, 10000] as const) {
    describe(`@ ${N} runs`, () => {
      const runs = genRuns(N);
      const haystacks = buildHaystacks(runs);

      test("haystack precomputation", () => {
        const s = measure(`haystack build (n=${N})`, 50, () => buildHaystacks(runs));
        if (N === 1000) expect(s.median).toBeLessThan(BUDGET_HAYSTACK_BUILD_MS);
      });

      test("filter: empty query (early-return path)", () => {
        const s = measure(`filter empty query (n=${N})`, 5000, () =>
          filterWithHaystack(runs, haystacks, "", null),
        );
        expect(s.median).toBeLessThan(0.05);
      });

      test("filter: text query, haystack lookup", () => {
        const s = measure(`filter text='train' haystack (n=${N})`, 2000, () =>
          filterWithHaystack(runs, haystacks, "train", null),
        );
        if (N === 1000) expect(s.median).toBeLessThan(BUDGET_PER_KEYSTROKE_MS);
      });

      test("filter: text query, naive per-row toLowerCase (baseline)", () => {
        measure(`filter text='train' NAIVE (n=${N})`, 500, () => filterNaive(runs, "train"));
      });

      test("filter: text query + status filter", () => {
        const s = measure(`filter text+status (n=${N})`, 2000, () =>
          filterWithHaystack(runs, haystacks, "train", "running"),
        );
        if (N === 1000) expect(s.median).toBeLessThan(BUDGET_PER_KEYSTROKE_MS);
      });

      test("visible slice (virtualization step)", () => {
        const filtered = filterWithHaystack(runs, haystacks, "train", null);
        measure(`slice 0..30 (n=${N}, filtered=${filtered.length})`, 50000, () =>
          filtered.slice(0, 30),
        );
      });

      test("end-to-end keystroke chain", () => {
        const s = measure(`E2E keystroke (n=${N})`, 2000, () => {
          // What happens in the runtime on every keystroke:
          //   filterText -> textQuery -> filtered -> visibleRows
          // Haystacks are already cached; the per-keystroke loop is the
          // critical path.
          const q = "train".trim().toLowerCase();
          const f = filterWithHaystack(runs, haystacks, q, null);
          f.slice(0, 30);
        });
        if (N === 1000) expect(s.median).toBeLessThan(BUDGET_PER_KEYSTROKE_MS);
      });
    });
  }
});
