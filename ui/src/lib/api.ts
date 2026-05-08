import type {
  ArtifactDetail,
  ArtifactSummary,
  ClusterInfo,
  CompareView,
  LineageNode,
  LogResponse,
  PipelineDetail,
  PipelineSummary,
  RecipeHistory,
  RecipeView,
  RunDetailResponse,
  RunEvent,
  RunSummary,
  EvalRequest,
} from "./types";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`/api${path}`, { headers: { accept: "application/json" } });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  cluster: () => get<ClusterInfo>("/cluster"),
  runs: () => get<{ runs: RunSummary[] }>("/runs").then((d) => d.runs),
  run: (id: string) => get<RunDetailResponse>(`/runs/${encodeURIComponent(id)}`),
  runLog: (id: string, tail = 200) =>
    get<LogResponse>(`/runs/${encodeURIComponent(id)}/log?tail=${tail}`),
  runEvents: (id: string) =>
    get<{ events: RunEvent[] }>(`/runs/${encodeURIComponent(id)}/events`).then((d) => d.events),
  recipeHistory: (name: string, limit = 20) =>
    get<RecipeHistory>(`/recipes/${encodeURIComponent(name)}/history?limit=${limit}`),
  recipe: (name: string) => get<RecipeView>(`/recipes/${encodeURIComponent(name)}`),
  compare: (ids: string[]) =>
    get<CompareView>(`/compare?ids=${ids.map(encodeURIComponent).join(",")}`),
  pipelines: () =>
    get<{ pipelines: PipelineSummary[] }>("/pipelines").then((d) => d.pipelines),
  pipeline: (id: string) => get<PipelineDetail>(`/pipelines/${encodeURIComponent(id)}`),
  artifacts: () =>
    get<{ artifacts: ArtifactSummary[] }>("/artifacts").then((d) => d.artifacts),
  artifact: (id: string) =>
    get<{ artifact: ArtifactDetail; producer: RunSummary | null; consumers: RunSummary[] }>(
      `/artifacts/${encodeURIComponent(id)}`,
    ),
  lineage: (id: string) =>
    get<LineageNode>(`/artifacts/${encodeURIComponent(id)}/lineage`),
  evals: () => get<{ evals: EvalRequest[] }>("/evals").then((d) => d.evals),
};
