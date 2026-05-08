// Smart metric extraction. Mirrors the server's `first_metric` /
// `first_flat_metric` logic so client and server agree on what counts as
// a metric. No framework knowledge — pure structural pattern-matching:
//
//   - {tasks: {k: v|{value}}, primary?}     ← the original convention
//   - {scores: {k: number}}                 ← inspect-ai / lm-eval-harness style
//   - {metrics: {k: number}}                ← MLflow style
//   - {results: {k: number}}                ← BIG-bench style
//   - {k: number, ...}                      ← bare top-level dict
//
// Anything else falls through to the JSON tree view.

export interface MetricEntry {
  value: number;
  stderr?: number;
  n?: number;
  [key: string]: unknown;
}

export interface ExtractedMetrics {
  tasks: Record<string, MetricEntry>;
  primary: string | null;
}

export function extractMetrics(result: unknown): ExtractedMetrics | null {
  if (!result || typeof result !== "object" || Array.isArray(result)) return null;
  const r = result as Record<string, unknown>;

  // Honor an explicit primary pin when paired with a tasks dict.
  if (typeof r.primary === "string" && r.tasks) {
    const tasks = parseFlatMetricDict(r.tasks);
    if (tasks && tasks[r.primary]) {
      return { tasks, primary: r.primary };
    }
  }

  for (const key of ["tasks", "scores", "metrics", "results"]) {
    if (key in r) {
      const tasks = parseFlatMetricDict(r[key]);
      if (tasks) {
        return { tasks, primary: pickPrimary(tasks, r) };
      }
    }
  }

  // Top level is itself a metric dict.
  const top = parseFlatMetricDict(result);
  if (top) return { tasks: top, primary: pickPrimary(top, r) };

  return null;
}

function parseFlatMetricDict(obj: unknown): Record<string, MetricEntry> | null {
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return null;
  const o = obj as Record<string, unknown>;
  const out: Record<string, MetricEntry> = {};
  for (const [k, v] of Object.entries(o)) {
    if (typeof v === "number" && Number.isFinite(v)) {
      out[k] = { value: v };
    } else if (
      v &&
      typeof v === "object" &&
      !Array.isArray(v) &&
      typeof (v as { value?: unknown }).value === "number"
    ) {
      out[k] = v as MetricEntry;
    } else {
      // Mixed types — bail; let JSON tree handle it.
      return null;
    }
  }
  return Object.keys(out).length > 0 ? out : null;
}

function pickPrimary(tasks: Record<string, MetricEntry>, root: Record<string, unknown>): string | null {
  // Explicit pin wins.
  if (typeof root.primary === "string" && tasks[root.primary]) {
    return root.primary;
  }
  // Heuristic: prefer keys ending in /accuracy, /strict_accuracy, /acc;
  // otherwise the first one. This makes the inline "headline" pick
  // something meaningful for inspect-ai-style outputs without coupling.
  const keys = Object.keys(tasks);
  const preferred = keys.find(
    (k) => /\b(strict_accuracy|accuracy|acc|pass@1|exact_match|score)$/i.test(k),
  );
  return preferred ?? keys[0] ?? null;
}
