import type {
  ArtifactDetail,
  ArtifactSummary,
  ClusterInfo,
  CompareView,
  DatasetSummary,
  EnvFileContent,
  EnvRevisions,
  EnvsList,
  EnvTaskResponse,
  LineageNode,
  LogResponse,
  PipelineDetail,
  PipelineSummary,
  PolicyCard,
  PolicyDetail,
  RecipeHistory,
  RecipeView,
  RolloutData,
  RolloutSeriesResponse,
  RunDetailResponse,
  RunEvent,
  RunSummary,
  SegmentDetail,
} from "./types";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`/api${path}`, { headers: { accept: "application/json" } });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json() as Promise<T>;
}

/** `?a=1&b=2` from the defined, non-empty entries; "" when there are none. */
function qs(params: Record<string, string | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v) q.set(k, v);
  const s = q.toString();
  return s ? `?${s}` : "";
}

export const api = {
  cluster: () => get<ClusterInfo>("/cluster"),
  runs: () => get<{ runs: RunSummary[] }>("/runs").then((d) => d.runs),
  run: (id: string) => get<RunDetailResponse>(`/runs/${encodeURIComponent(id)}`),
  // Rollout browser: a run's evals aggregated one export hop down, grouped
  // by policy + step-sorted. See get_run_rollouts in src/server.rs.
  runRollouts: (id: string) =>
    get<RolloutSeriesResponse>(`/runs/${encodeURIComponent(id)}/rollouts`),
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
  policies: () =>
    get<{ policies: PolicyCard[] }>("/policies").then((d) => d.policies),
  policy: (name: string) => get<PolicyDetail>(`/policies/${encodeURIComponent(name)}`),
  // `task` selects an instruction in a multi-instruction eval rollout;
  // omit it for single-rollout (legacy) artifacts.
  rollout: (id: string, task?: number) =>
    get<RolloutData>(
      `/artifacts/${encodeURIComponent(id)}/rollout${task === undefined ? "" : `?task=${task}`}`,
    ),
  frameUrl: (id: string, n: number, task?: number) =>
    `/api/artifacts/${encodeURIComponent(id)}/frames/${n}${task === undefined ? "" : `?task=${task}`}`,
  // Dataset explorer. `dataset()` 404s when the artifact isn't a browseable
  // per-segment dataset (e.g. Stage C/D grain shards) — callers treat that
  // as "no Browse section" rather than an error.
  dataset: (id: string) => get<DatasetSummary>(`/artifacts/${encodeURIComponent(id)}/dataset`),
  datasetSegment: (id: string, split: string, seg: string) =>
    get<SegmentDetail>(
      `/artifacts/${encodeURIComponent(id)}/dataset/segments/${encodeURIComponent(split)}/${encodeURIComponent(seg)}`,
    ),
  datasetFrameUrl: (id: string, split: string, seg: string, n: number) =>
    `/api/artifacts/${encodeURIComponent(id)}/dataset/frames/${encodeURIComponent(split)}/${encodeURIComponent(seg)}/${n}`,
  // Environment browser. `rev` defaults server-side to [envs].rev;
  // `grader` adds scoring/ and reference/ (see src/envs.rs).
  envs: (rev?: string) => get<EnvsList>(`/envs${qs({ rev })}`),
  envRevisions: () => get<EnvRevisions>("/envs/revisions"),
  envTask: (key: string, rev?: string, grader = false) =>
    get<EnvTaskResponse>(
      `/envs/tasks/${encodeURIComponent(key)}${qs({ rev, grader: grader ? "1" : undefined })}`,
    ),
  envFile: (key: string, path: string, rev?: string, grader = false) =>
    get<EnvFileContent>(
      `/envs/tasks/${encodeURIComponent(key)}/file${qs({ path, rev, grader: grader ? "1" : undefined })}`,
    ),
};
