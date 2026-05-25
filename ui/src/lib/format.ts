// Status & time formatting. Tight, no third-party date library — the
// vocabulary is small enough to write by hand and avoid 30kb of moment/dayjs.

import type { RunStatus } from "./types";

export function statusGroup(s: RunStatus | string): "running" | "succeeded" | "failed" | "pending" | "neutral" {
  switch (s) {
    case "running":
      return "running";
    case "succeeded":
      return "succeeded";
    case "failed":
    case "oom":
    case "timeout":
    case "unknown_terminal":
      return "failed";
    case "submitted":
    case "created":
    case "pending":
      return "pending";
    case "cancelled":
    default:
      return "neutral";
  }
}

const SHORT_STATUS: Record<string, string> = {
  created: "created",
  submitted: "queued",
  running: "running",
  succeeded: "ok",
  failed: "failed",
  cancelled: "cancelled",
  timeout: "timeout",
  oom: "oom",
  unknown_terminal: "unknown",
};

export function shortStatus(s: string): string {
  return SHORT_STATUS[s] ?? s;
}

/** Concise relative time. Updates well — see the `relativeTime` rune-using
 *  reactive helper. */
export function formatRelative(ts: number, now = Date.now() / 1000): string {
  const delta = Math.max(0, now - ts);
  if (delta < 5) return "just now";
  if (delta < 60) return `${Math.floor(delta)}s ago`;
  if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
  if (delta < 86_400) return `${Math.floor(delta / 3600)}h ago`;
  if (delta < 86_400 * 30) return `${Math.floor(delta / 86_400)}d ago`;
  if (delta < 86_400 * 365) return `${Math.floor(delta / (86_400 * 30))}mo ago`;
  return `${Math.floor(delta / (86_400 * 365))}y ago`;
}

export function formatAbsolute(ts: number): string {
  const d = new Date(ts * 1000);
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

export function formatDuration(secs: number | null | undefined): string {
  if (secs == null) return "—";
  if (secs < 1) return "<1s";
  if (secs < 60) return `${Math.floor(secs)}s`;
  const m = Math.floor(secs / 60);
  if (secs < 3600) return `${m}m ${Math.floor(secs % 60)}s`;
  const h = Math.floor(secs / 3600);
  return `${h}h ${m % 60}m`;
}

/** "live" duration — for runs without a finished_at, ticks from created_at. */
export function liveDuration(run: { created_at: number; finished_at: number | null; is_terminal: boolean }, nowSecs: number): number | null {
  if (run.finished_at != null) return run.finished_at - run.created_at;
  if (run.is_terminal) return null;
  return Math.max(0, nowSecs - run.created_at);
}

/** Always show first 8 of a hash; click-to-copy elsewhere. Fixed-width
 *  truncation prevents row jitter. */
export function shortHash(h: string, n = 8): string {
  return h.slice(0, n);
}

export function shortId(id: string, n = 12): string {
  // run_<uuid> — the prefix is uniform, so we keep "run_" then truncate.
  if (id.length <= n + 4) return id;
  if (id.startsWith("run_")) return `run_${id.slice(4, 4 + n)}`;
  if (id.startsWith("artifact_")) return id.slice(0, 9 + n);
  return id.slice(0, n);
}

export function copy(text: string): void {
  navigator.clipboard?.writeText(text).catch(() => {});
}

// ---------- Stanza grammar helpers ----------
// The lab's editorial framing — runs are *editions*, dates are masthead-
// styled, status is a single glyph. Used by mastheads and edition headers
// across the UI.

/**
 * The edition number for an id. Each entity carries an opaque id (run_…,
 * artifact_…); for editorial display we want a stable short tag. We take
 * the leading 6 hex characters after the prefix and uppercase them. The
 * result is monospace-ready and stable: same id → same edition number.
 */
export function editionNumber(id: string): string {
  const body = id.includes("_") ? id.slice(id.indexOf("_") + 1) : id;
  return body.slice(0, 6).toUpperCase();
}

/** "23 MAY 2026" — masthead date, day-month-year in small caps. */
export function formatEditionDate(ts: number): string {
  const d = new Date(ts * 1000);
  const day = d.getDate();
  const mon = d
    .toLocaleString("en-GB", { month: "short" })
    .toUpperCase();
  const year = d.getFullYear();
  return `${day} ${mon} ${year}`;
}

/** "14:32" — masthead time component. */
export function formatEditionTime(ts: number): string {
  const d = new Date(ts * 1000);
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  return `${hh}:${mm}`;
}

/** Single-glyph status. The dot color carries hue; this carries shape. */
export function statusGlyph(s: string): string {
  switch (statusGroup(s)) {
    case "running":
      return "●";
    case "succeeded":
      return "✓";
    case "failed":
      return "×";
    case "pending":
      return "○";
    case "neutral":
    default:
      return "—";
  }
}

// A methods paragraph is a sequence of text/code segments. The component
// renders code segments inside <code> for proper monospace styling,
// while plain text flows as body type. The shape mirrors a methods
// section in a paper: terse, factual, indicative.
export type MethodSegment =
  | { kind: "text"; value: string }
  | { kind: "code"; value: string };

/**
 * Build a methods-paragraph for a run, given its detail response. The
 * voice is editorial: complete sentences, no exclamations, no "we",
 * past-tense for terminal runs, present-tense for live runs. Inputs and
 * outputs are framed as "specimens" — the Stanza vocabulary.
 */
export function methodsParagraph(d: {
  run: {
    recipe_name: string;
    recipe_hash: string;
    repo: string;
    created_at: number;
    submitted_by: string | null;
    pipeline_id: string | null;
    stage_name: string | null;
    is_terminal: boolean;
    status: string;
  };
  inputs: { artifact_id: string | null }[];
  outputs: { id: string }[];
}): MethodSegment[] {
  const r = d.run;
  const date = formatEditionDate(r.created_at);
  const time = formatEditionTime(r.created_at);
  const repoShort = r.repo.split("/").pop() ?? r.repo;
  const hashShort = r.recipe_hash.slice(0, 7);
  const live = !r.is_terminal;
  const segs: MethodSegment[] = [];

  // Sentence 1: provenance.
  segs.push({ kind: "text", value: live ? "Submitted from " : "Submitted from " });
  segs.push({ kind: "code", value: `${repoShort}@${hashShort}` });
  segs.push({ kind: "text", value: ` on ${date} at ${time}` });
  if (r.submitted_by) {
    segs.push({ kind: "text", value: " by " });
    segs.push({ kind: "code", value: r.submitted_by });
  }
  segs.push({ kind: "text", value: ". " });

  // Sentence 2: pipeline / stage context.
  if (r.stage_name && r.pipeline_id) {
    segs.push({ kind: "text", value: live ? "Executes stage " : "Executed stage " });
    segs.push({ kind: "code", value: r.stage_name });
    segs.push({ kind: "text", value: " of pipeline " });
    segs.push({ kind: "code", value: r.pipeline_id.slice(0, 12) });
    segs.push({ kind: "text", value: ". " });
  } else if (r.stage_name) {
    segs.push({ kind: "text", value: live ? "Executes stage " : "Executed stage " });
    segs.push({ kind: "code", value: r.stage_name });
    segs.push({ kind: "text", value: ". " });
  } else {
    segs.push({ kind: "text", value: live ? "Executes recipe " : "Executed recipe " });
    segs.push({ kind: "code", value: r.recipe_name });
    segs.push({ kind: "text", value: ". " });
  }

  // Sentence 3: inputs / outputs.
  const ins = d.inputs.length;
  const outs = d.outputs.length;
  if (ins || outs) {
    const verb = live ? "Consumes" : "Consumed";
    const prod = live ? "produces" : "produced";
    const insStr = ins ? `${verb} ${ins} input ${ins === 1 ? "specimen" : "specimens"}` : "";
    const outsStr = outs ? `${prod} ${outs} output ${outs === 1 ? "specimen" : "specimens"}` : "";
    let s = "";
    if (insStr && outsStr) s = `${insStr}; ${outsStr}.`;
    else if (insStr) s = `${insStr}.`;
    else s = `${outsStr.charAt(0).toUpperCase() + outsStr.slice(1)}.`;
    segs.push({ kind: "text", value: s });
  }

  return segs;
}
