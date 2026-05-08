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
