// Pure helpers for the environment browser: list filtering, the file
// tree, per-file highlighting language, and the rubric arithmetic the
// Grading section explains. No DOM, no fetches — see envs.test.ts.

import type { Lang } from "./highlight";
import type { EnvFileEntry, EnvRubrics, EnvSummary } from "./types";

/** Lowercased text a list query is matched against. */
export function haystack(t: EnvSummary): string {
  return [t.key, t.title ?? "", t.tags.join(" "), t.authors.join(" "), t.prompt]
    .join("\n")
    .toLowerCase();
}

/**
 * Tasks matching every whitespace-separated word of `query` (anywhere in
 * key, title, tags, authors or prompt) and carrying every selected tag.
 */
export function filterTasks(
  tasks: EnvSummary[],
  query: string,
  tags: ReadonlySet<string>,
  hay: (t: EnvSummary) => string = haystack,
): EnvSummary[] {
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  return tasks.filter((t) => {
    for (const tag of tags) if (!t.tags.includes(tag)) return false;
    if (words.length === 0) return true;
    const h = hay(t);
    return words.every((w) => h.includes(w));
  });
}

/** Tags by how many tasks carry them, most common first, then by name. */
export function tagCounts(tasks: EnvSummary[]): [string, number][] {
  const counts = new Map<string, number>();
  for (const t of tasks) for (const tag of t.tags) counts.set(tag, (counts.get(tag) ?? 0) + 1);
  return [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

export interface TreeNode {
  name: string;
  /** Full path relative to the task directory. */
  path: string;
  dir: boolean;
  children: TreeNode[];
  /** Files only. */
  file?: EnvFileEntry;
  /** Directories: total files beneath. */
  count: number;
}

/**
 * Nest the files under `prefix` (e.g. "workspace/") into a tree.
 * Directories sort before files, each group alphabetically.
 */
export function buildTree(files: EnvFileEntry[], prefix: string): TreeNode[] {
  const root: TreeNode = { name: "", path: prefix.replace(/\/$/, ""), dir: true, children: [], count: 0 };
  for (const f of files) {
    if (!f.path.startsWith(prefix)) continue;
    const parts = f.path.slice(prefix.length).split("/");
    let node = root;
    node.count++;
    for (let i = 0; i < parts.length - 1; i++) {
      let child = node.children.find((c) => c.dir && c.name === parts[i]);
      if (!child) {
        child = {
          name: parts[i],
          path: prefix + parts.slice(0, i + 1).join("/"),
          dir: true,
          children: [],
          count: 0,
        };
        node.children.push(child);
      }
      child.count++;
      node = child;
    }
    const name = parts[parts.length - 1];
    node.children.push({ name, path: f.path, dir: false, children: [], file: f, count: 1 });
  }
  const sort = (n: TreeNode) => {
    n.children.sort((a, b) => (a.dir === b.dir ? a.name.localeCompare(b.name) : a.dir ? -1 : 1));
    n.children.forEach(sort);
  };
  sort(root);
  return root.children;
}

const BY_EXT: Record<string, Lang> = {
  py: "python",
  md: "markdown",
  ts: "typescript",
  tsx: "tsx",
  js: "javascript",
  mjs: "javascript",
  json: "json",
  toml: "toml",
  yaml: "yaml",
  yml: "yaml",
  sh: "bash",
  bash: "bash",
  patch: "diff",
  diff: "diff",
  go: "go",
  tex: "latex",
  cfg: "ini",
  ini: "ini",
};

/** Highlighting language for a path, or null for plain text. */
export function langForPath(path: string): Lang | null {
  const name = path.split("/").pop() ?? "";
  const dot = name.lastIndexOf(".");
  if (dot <= 0) return null;
  return BY_EXT[name.slice(dot + 1).toLowerCase()] ?? null;
}

export function isMarkdown(path: string): boolean {
  return /\.md$/i.test(path);
}

export interface RubricSummary {
  outcomePoints: number;
  outcomes: number;
  invariants: number;
  vetoes: number;
  judged: number;
  programmatic: number;
}

/**
 * The counts behind the crowd-envs reward: passed outcome points over
 * total outcome points, zeroed if any invariant fails, zeroed if any veto
 * trips (scaffold/rubric.py `Rubric.score`).
 */
export function rubricSummary(r: EnvRubrics | null | undefined): RubricSummary {
  const s: RubricSummary = { outcomePoints: 0, outcomes: 0, invariants: 0, vetoes: 0, judged: 0, programmatic: 0 };
  for (const g of r?.rubrics ?? []) {
    for (const c of g.criteria) {
      if (g.role === "outcome") {
        s.outcomes++;
        s.outcomePoints += c.points ?? 0;
      } else if (g.role === "invariant") s.invariants++;
      else if (g.role === "veto") s.vetoes++;
      if (g.scoring.type === "judge") s.judged++;
      else s.programmatic++;
    }
  }
  return s;
}

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(n < 10 * 1024 ? 1 : 0)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

export function formatSecs(s: number | null | undefined): string {
  if (s == null) return "—";
  if (s < 60) return `${s}s`;
  if (s % 60 === 0) return `${s / 60} min`;
  return `${Math.floor(s / 60)}m ${s % 60}s`;
}

/** "2026-09-28T15:35:27+02:00" → "28 Sep 2026". */
export function formatCommitDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
}

/** A full sha reads as its first seven characters; names stay as typed. */
export function shortRev(rev: string): string {
  return /^[0-9a-f]{40}$/.test(rev) ? rev.slice(0, 7) : rev;
}

/** The server's `{"error": …}` text out of an api.ts error, or the
 *  message as it came. */
export function errorMessage(e: unknown): string {
  const msg = e instanceof Error ? e.message : String(e);
  const m = /^\d{3} [^:]*: (\{.*\})$/s.exec(msg);
  if (m) {
    try {
      const body = JSON.parse(m[1]);
      if (typeof body?.error === "string") return body.error;
    } catch {
      // not JSON after all
    }
  }
  return msg;
}
