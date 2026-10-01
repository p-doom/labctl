import { describe, it, expect } from "vitest";
import {
  errorMessage,
  shortRev,
  buildTree,
  filterTasks,
  formatBytes,
  formatSecs,
  langForPath,
  rubricSummary,
  tagCounts,
} from "./envs";
import { renderMarkdown } from "./markdown";
import type { EnvFileEntry, EnvRubrics, EnvSummary } from "./types";

function task(key: string, over: Partial<EnvSummary> = {}): EnvSummary {
  return {
    key,
    title: null,
    tags: [],
    difficulty: "hard",
    authors: [],
    runtime: "terminal-standard",
    agent_timeout_secs: 900,
    workspace_files: 1,
    workspace_bytes: 10,
    prompt: "",
    ...over,
  };
}

const TASKS = [
  task("idm-degenerate-actions", { title: "Make the offline inverse-dynamics run useful", tags: ["vision", "training"], prompt: "degenerate behavior" }),
  task("park-physics", { title: "Make the Cinder Coast ride inspectable", tags: ["3d", "rendering"], authors: ["Sacha Martinelle"] }),
  task("rl-without-sft", { title: "Make the mocked CUA-Gym overfit run usable", tags: ["training", "rollouts"] }),
];

describe("filterTasks", () => {
  it("returns everything for an empty query and no tags", () => {
    expect(filterTasks(TASKS, "", new Set())).toHaveLength(3);
  });

  it("matches every word, across title, key, tags, authors and prompt", () => {
    expect(filterTasks(TASKS, "degenerate", new Set()).map((t) => t.key)).toEqual(["idm-degenerate-actions"]);
    expect(filterTasks(TASKS, "sacha cinder", new Set()).map((t) => t.key)).toEqual(["park-physics"]);
    expect(filterTasks(TASKS, "make usable", new Set()).map((t) => t.key)).toEqual(["rl-without-sft"]);
    expect(filterTasks(TASKS, "PARK", new Set()).map((t) => t.key)).toEqual(["park-physics"]);
  });

  it("requires every selected tag", () => {
    expect(filterTasks(TASKS, "", new Set(["training"])).map((t) => t.key)).toEqual(["idm-degenerate-actions", "rl-without-sft"]);
    expect(filterTasks(TASKS, "", new Set(["training", "vision"])).map((t) => t.key)).toEqual(["idm-degenerate-actions"]);
    expect(filterTasks(TASKS, "", new Set(["nope"]))).toEqual([]);
  });
});

describe("tagCounts", () => {
  it("orders by count, then name", () => {
    expect(tagCounts(TASKS)).toEqual([
      ["training", 2],
      ["3d", 1],
      ["rendering", 1],
      ["rollouts", 1],
      ["vision", 1],
    ]);
  });
});

describe("buildTree", () => {
  const f = (path: string): EnvFileEntry => ({ path, size: 1, area: "workspace" });
  const files = [
    f("workspace/README.md"),
    f("workspace/src/b.py"),
    f("workspace/src/a.py"),
    f("workspace/src/deep/c.py"),
    f("prompt.md"),
    f("reference/NOTES.md"),
  ];

  it("nests files under the prefix, directories first", () => {
    const tree = buildTree(files, "workspace/");
    expect(tree.map((n) => n.name)).toEqual(["src", "README.md"]);
    const src = tree[0];
    expect(src.dir).toBe(true);
    expect(src.path).toBe("workspace/src");
    expect(src.count).toBe(3);
    expect(src.children.map((n) => n.name)).toEqual(["deep", "a.py", "b.py"]);
    expect(src.children[1].path).toBe("workspace/src/a.py");
  });

  it("ignores files outside the prefix", () => {
    expect(buildTree(files, "reference/").map((n) => n.path)).toEqual(["reference/NOTES.md"]);
    expect(buildTree(files, "scoring/")).toEqual([]);
  });
});

describe("langForPath", () => {
  it("maps extensions it knows and nothing else", () => {
    expect(langForPath("workspace/a/b.py")).toBe("python");
    expect(langForPath("README.MD")).toBe("markdown");
    expect(langForPath("reference/solution/solve.sh")).toBe("bash");
    expect(langForPath("data/train.jsonl")).toBeNull();
    expect(langForPath("Makefile")).toBeNull();
    expect(langForPath(".gitignore")).toBeNull();
  });
});

describe("rubricSummary", () => {
  const rubrics: EnvRubrics = {
    rubrics: [
      { name: "o", role: "outcome", scoring: { type: "python" }, criteria: [{ name: "a", criterion: "", points: 3 }, { name: "b", criterion: "", points: 2 }] },
      { name: "j", role: "outcome", scoring: { type: "judge", evidence: "both" }, criteria: [{ name: "c", criterion: "", points: 5 }] },
      { name: "i", role: "invariant", scoring: { type: "python" }, criteria: [{ name: "d", criterion: "" }] },
      { name: "v", role: "veto", scoring: { type: "judge" }, criteria: [{ name: "e", criterion: "" }] },
    ],
  };

  it("counts outcome points, gates and vetoes", () => {
    expect(rubricSummary(rubrics)).toEqual({ outcomePoints: 10, outcomes: 3, invariants: 1, vetoes: 1, judged: 2, programmatic: 3 });
  });

  it("is all zeros without rubrics", () => {
    expect(rubricSummary(null).outcomes).toBe(0);
  });
});

describe("shortRev and errorMessage", () => {
  it("shortens full shas only", () => {
    expect(shortRev("c3c1382b21afdde38e4de291e585a25d074fac94")).toBe("c3c1382");
    expect(shortRev("origin/main")).toBe("origin/main");
    expect(shortRev("c3c1382")).toBe("c3c1382");
  });
  it("pulls the server's error text out of an api error", () => {
    const e = new Error('404 Not Found: {"error":"no task \\"x\\" at abc1234"}');
    expect(errorMessage(e)).toBe('no task "x" at abc1234');
    expect(errorMessage(new Error("502 Bad Gateway: <html>"))).toBe("502 Bad Gateway: <html>");
    expect(errorMessage("plain")).toBe("plain");
  });
});

describe("formatting", () => {
  it("formats bytes and seconds", () => {
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(2048)).toBe("2.0 KB");
    expect(formatBytes(70537)).toBe("69 KB");
    expect(formatBytes(5 * 1024 * 1024)).toBe("5.0 MB");
    expect(formatSecs(900)).toBe("15 min");
    expect(formatSecs(90)).toBe("1m 30s");
    expect(formatSecs(null)).toBe("—");
  });
});

describe("renderMarkdown", () => {
  it("renders markdown", () => {
    expect(renderMarkdown("# Title\n\nSome *text*.")).toContain("<em>text</em>");
  });

  it("strips scripts and event handlers", () => {
    const html = renderMarkdown('<script>alert(1)</script><img src="https://x/y.png" onerror="alert(2)">');
    expect(html).not.toContain("<script");
    expect(html).not.toContain("onerror");
  });

  it("opens external links in a new tab and neutralises relative ones", () => {
    const html = renderMarkdown("[ext](https://example.com) [rel](scripts/run.sh)");
    expect(html).toContain('href="https://example.com"');
    expect(html).toContain('target="_blank"');
    expect(html).not.toContain('href="scripts/run.sh"');
    expect(html).toContain('title="scripts/run.sh"');
  });
});
