// Minimal hash-based router. Single page app, three top-level surfaces +
// optional selected entity in a side panel. URL is the source of truth.
//
// Examples:
//   #/runs                         → runs list
//   #/runs?status=running          → filtered runs list
//   #/runs/run_abc123              → runs list with that run open in panel
//   #/pipelines                    → pipelines list
//   #/pipelines/pipe_xyz           → pipeline detail
//   #/artifacts                    → artifact list
//   #/artifacts/artifact_abc       → artifact detail panel

export type View =
  | "runs"
  | "pipelines"
  | "artifacts"
  | "policies"
  | "lineage"
  | "recipes"
  | "compare";

export interface Route {
  view: View;
  selected: string | null;
  query: URLSearchParams;
}

function parse(hash: string): Route {
  const raw = hash.replace(/^#\/?/, "");
  const [pathPart, queryPart] = raw.split("?", 2) as [string, string | undefined];
  const segments = pathPart.split("/").filter(Boolean);
  const view = (segments[0] as View) || "runs";
  const selected = segments[1] ?? null;
  const query = new URLSearchParams(queryPart ?? "");
  if (!["runs", "pipelines", "artifacts", "policies", "lineage", "recipes", "compare"].includes(view)) {
    return { view: "runs", selected: null, query };
  }
  return { view, selected, query };
}

let route = $state<Route>(parse(typeof window !== "undefined" ? window.location.hash : ""));

if (typeof window !== "undefined") {
  window.addEventListener("hashchange", () => {
    route = parse(window.location.hash);
  });
}

export const router = {
  get view() {
    return route.view;
  },
  get selected() {
    return route.selected;
  },
  get query() {
    return route.query;
  },
  go(view: View, selected: string | null = null, query: URLSearchParams | null = null) {
    const q = query?.toString();
    let hash = `#/${view}`;
    if (selected) hash += `/${selected}`;
    if (q) hash += `?${q}`;
    if (window.location.hash !== hash) window.location.hash = hash;
  },
  select(view: View, selected: string | null) {
    const q = route.query.toString();
    let hash = `#/${view}`;
    if (selected) hash += `/${selected}`;
    if (q) hash += `?${q}`;
    if (window.location.hash !== hash) window.location.hash = hash;
  },
  setQuery(updates: Record<string, string | null>) {
    const q = new URLSearchParams(route.query);
    for (const [k, v] of Object.entries(updates)) {
      if (v == null || v === "") q.delete(k);
      else q.set(k, v);
    }
    let hash = `#/${route.view}`;
    if (route.selected) hash += `/${route.selected}`;
    const qs = q.toString();
    if (qs) hash += `?${qs}`;
    if (window.location.hash !== hash) window.location.hash = hash;
  },
};
