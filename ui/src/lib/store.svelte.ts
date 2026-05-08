// Single global datastore. All views read synchronously from here; the
// store fetches lazily and is patched in real time by SSE pushes.
//
// Lifecycle:
//   - On app boot, `connectStream()` opens an EventSource to /api/stream.
//   - Views call `loadX()` once on mount; the store dedupes inflight calls
//     and short-circuits if a fresh-enough cached value exists (SWR).
//   - SSE pushes invalidate cache entries; if a view is currently observing
//     them, a background refetch fires. Views never see a flicker.
//
// Why this beats the previous setInterval-poller:
//   - No 2s round-trip cap on freshness — pushes arrive in <500ms server-side
//     plus negligible network.
//   - No per-view fetch logic; cache reads are synchronous and instant.
//   - Hover prefetch is just `loadRunDetail(id)` early.

import { api } from "./api";
import type {
  ArtifactSummary,
  CompareView,
  EvalRequest,
  PipelineSummary,
  RecipeHistory,
  RecipeView,
  RunDetailResponse,
  RunSummary,
  PipelineDetail,
  ArtifactDetail,
  LineageNode,
  LogResponse,
  RunEvent,
  ClusterInfo,
} from "./types";

interface Collection<T> {
  data: T | null;
  loadedAt: number | null;
  loading: boolean;
  error: string | null;
}

interface Detail<T> {
  data: T | null;
  loadedAt: number | null;
}

const FRESH_MS = 1000; // SWR threshold — within this we don't refetch on read

// ---------- runes-backed state ----------

let _runs = $state<Collection<RunSummary[]>>({ data: null, loadedAt: null, loading: false, error: null });
let _pipelines = $state<Collection<PipelineSummary[]>>({ data: null, loadedAt: null, loading: false, error: null });
let _artifacts = $state<Collection<ArtifactSummary[]>>({ data: null, loadedAt: null, loading: false, error: null });
let _evals = $state<Collection<EvalRequest[]>>({ data: null, loadedAt: null, loading: false, error: null });
let _cluster = $state<ClusterInfo | null>(null);

// ---------- equality short-circuits ----------
// SSE pushes refresh the affected list, but the response is often
// byte-identical (e.g. a status flip on an inactive run leaves everything
// else unchanged). We compare only the fields that actually mutate at
// runtime — id/recipe_name/repo are immutable post-creation. If equal,
// keep the old reference so downstream $derived chains don't re-run.

function sameRuns(a: RunSummary[] | null, b: RunSummary[]): boolean {
  if (a == null || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const x = a[i]!;
    const y = b[i]!;
    if (
      x.id !== y.id ||
      x.status !== y.status ||
      x.finished_at !== y.finished_at ||
      x.duration_secs !== y.duration_secs ||
      x.is_terminal !== y.is_terminal ||
      x.job_id !== y.job_id
    ) {
      return false;
    }
  }
  return true;
}

function samePipelines(a: PipelineSummary[] | null, b: PipelineSummary[]): boolean {
  if (a == null || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const x = a[i]!;
    const y = b[i]!;
    if (x.id !== y.id || x.status !== y.status || x.stage_count !== y.stage_count) return false;
  }
  return true;
}

function sameArtifacts(a: ArtifactSummary[] | null, b: ArtifactSummary[]): boolean {
  if (a == null || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i]!.id !== b[i]!.id) return false;
  }
  return true; // Artifacts are immutable once written; identity by id is enough.
}

function sameEvals(a: EvalRequest[] | null, b: EvalRequest[]): boolean {
  if (a == null || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const x = a[i]!;
    const y = b[i]!;
    if (
      x.eval_key !== y.eval_key ||
      x.state !== y.state ||
      x.eval_run_id !== y.eval_run_id ||
      x.updated_at !== y.updated_at
    ) {
      return false;
    }
  }
  return true;
}

// Detail caches. Replace whole entries on update so Svelte's deep proxy
// reactivity picks up only what changed.
const _runDetails = $state<Record<string, Detail<RunDetailResponse>>>({});
const _runLogs = $state<Record<string, Detail<LogResponse>>>({});
const _runEvents = $state<Record<string, Detail<RunEvent[]>>>({});
const _pipelineDetails = $state<Record<string, Detail<PipelineDetail>>>({});
const _artifactDetails = $state<Record<string, Detail<{ artifact: ArtifactDetail; producer: RunSummary | null; consumers: RunSummary[] }>>>({});
const _lineage = $state<Record<string, Detail<LineageNode>>>({});
const _recipeHistory = $state<Record<string, Detail<RecipeHistory>>>({});
const _recipeViews = $state<Record<string, Detail<RecipeView>>>({});
const _compareViews = $state<Record<string, Detail<CompareView>>>({});

// In-flight tracking — dedupe concurrent calls for the same key.
const inflight = new Map<string, Promise<unknown>>();

function dedupe<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const existing = inflight.get(key);
  if (existing) return existing as Promise<T>;
  const p = fn().finally(() => inflight.delete(key));
  inflight.set(key, p);
  return p;
}

function isFresh(at: number | null): boolean {
  return at != null && Date.now() - at < FRESH_MS;
}

// ---------- collection loaders ----------

export async function loadRuns(force = false): Promise<RunSummary[]> {
  if (!force && _runs.data && isFresh(_runs.loadedAt)) return _runs.data;
  return dedupe("runs", async () => {
    if (!_runs.data) _runs.loading = true;
    try {
      const next = await api.runs();
      // Skip the assignment when the response matches what we already
      // hold. Otherwise every SSE-triggered refetch invalidates the whole
      // table even when nothing relevant changed — kills hover snappiness.
      if (!sameRuns(_runs.data, next)) {
        _runs.data = next;
      }
      _runs.loadedAt = Date.now();
      _runs.error = null;
      return _runs.data ?? next;
    } catch (e) {
      _runs.error = e instanceof Error ? e.message : String(e);
      throw e;
    } finally {
      _runs.loading = false;
    }
  });
}

export async function loadPipelines(force = false): Promise<PipelineSummary[]> {
  if (!force && _pipelines.data && isFresh(_pipelines.loadedAt)) return _pipelines.data;
  return dedupe("pipelines", async () => {
    if (!_pipelines.data) _pipelines.loading = true;
    try {
      const next = await api.pipelines();
      if (!samePipelines(_pipelines.data, next)) _pipelines.data = next;
      _pipelines.loadedAt = Date.now();
      _pipelines.error = null;
      return _pipelines.data ?? next;
    } catch (e) {
      _pipelines.error = e instanceof Error ? e.message : String(e);
      throw e;
    } finally {
      _pipelines.loading = false;
    }
  });
}

export async function loadArtifacts(force = false): Promise<ArtifactSummary[]> {
  if (!force && _artifacts.data && isFresh(_artifacts.loadedAt)) return _artifacts.data;
  return dedupe("artifacts", async () => {
    if (!_artifacts.data) _artifacts.loading = true;
    try {
      const next = await api.artifacts();
      if (!sameArtifacts(_artifacts.data, next)) _artifacts.data = next;
      _artifacts.loadedAt = Date.now();
      _artifacts.error = null;
      return _artifacts.data ?? next;
    } catch (e) {
      _artifacts.error = e instanceof Error ? e.message : String(e);
      throw e;
    } finally {
      _artifacts.loading = false;
    }
  });
}

export async function loadEvals(force = false): Promise<EvalRequest[]> {
  if (!force && _evals.data && isFresh(_evals.loadedAt)) return _evals.data;
  return dedupe("evals", async () => {
    if (!_evals.data) _evals.loading = true;
    try {
      const next = await api.evals();
      if (!sameEvals(_evals.data, next)) _evals.data = next;
      _evals.loadedAt = Date.now();
      _evals.error = null;
      return _evals.data ?? next;
    } catch (e) {
      _evals.error = e instanceof Error ? e.message : String(e);
      throw e;
    } finally {
      _evals.loading = false;
    }
  });
}

export async function loadCluster(): Promise<ClusterInfo> {
  if (_cluster) return _cluster;
  return dedupe("cluster", async () => {
    _cluster = await api.cluster();
    return _cluster;
  });
}

// ---------- detail loaders (SWR) ----------

export async function loadRunDetail(id: string, force = false): Promise<RunDetailResponse> {
  const entry = _runDetails[id];
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`run:${id}`, async () => {
    const next = await api.run(id);
    _runDetails[id] = { data: next, loadedAt: Date.now() };
    return next;
  });
}

export async function loadRunLog(id: string, force = false): Promise<LogResponse> {
  const entry = _runLogs[id];
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`log:${id}`, async () => {
    const next = await api.runLog(id, 200);
    _runLogs[id] = { data: next, loadedAt: Date.now() };
    return next;
  });
}

export async function loadRunEvents(id: string, force = false): Promise<RunEvent[]> {
  const entry = _runEvents[id];
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`events:${id}`, async () => {
    const next = await api.runEvents(id);
    _runEvents[id] = { data: next, loadedAt: Date.now() };
    return next;
  });
}

export async function loadPipelineDetail(id: string, force = false): Promise<PipelineDetail> {
  const entry = _pipelineDetails[id];
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`pipeline:${id}`, async () => {
    const next = await api.pipeline(id);
    _pipelineDetails[id] = { data: next, loadedAt: Date.now() };
    return next;
  });
}

export async function loadArtifactDetail(id: string, force = false) {
  const entry = _artifactDetails[id];
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`artifact:${id}`, async () => {
    const next = await api.artifact(id);
    _artifactDetails[id] = { data: next, loadedAt: Date.now() };
    return next;
  });
}

export async function loadLineage(id: string, force = false): Promise<LineageNode> {
  const entry = _lineage[id];
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`lineage:${id}`, async () => {
    const next = await api.lineage(id);
    _lineage[id] = { data: next, loadedAt: Date.now() };
    return next;
  });
}

export async function loadCompareView(ids: string[], force = false): Promise<CompareView> {
  const key = ids.slice().sort().join(",");
  const entry = _compareViews[key];
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`compare:${key}`, async () => {
    const next = await api.compare(ids);
    _compareViews[key] = { data: next, loadedAt: Date.now() };
    return next;
  });
}

export async function loadRecipeView(name: string, force = false): Promise<RecipeView> {
  const entry = _recipeViews[name];
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`recipe:${name}`, async () => {
    const next = await api.recipe(name);
    _recipeViews[name] = { data: next, loadedAt: Date.now() };
    return next;
  });
}

export async function loadRecipeHistory(name: string): Promise<RecipeHistory> {
  const entry = _recipeHistory[name];
  if (entry?.data) return entry.data;
  return dedupe(`recipe:${name}`, async () => {
    const next = await api.recipeHistory(name, 16);
    _recipeHistory[name] = { data: next, loadedAt: Date.now() };
    return next;
  });
}

// ---------- synchronous reads (the snappy bit) ----------

export const store = {
  get runs() {
    return _runs;
  },
  get pipelines() {
    return _pipelines;
  },
  get artifacts() {
    return _artifacts;
  },
  get evals() {
    return _evals;
  },
  get cluster() {
    return _cluster;
  },
  runDetail(id: string) {
    return _runDetails[id]?.data ?? null;
  },
  runLog(id: string) {
    return _runLogs[id]?.data ?? null;
  },
  runEvents(id: string) {
    return _runEvents[id]?.data ?? null;
  },
  pipelineDetail(id: string) {
    return _pipelineDetails[id]?.data ?? null;
  },
  artifactDetail(id: string) {
    return _artifactDetails[id]?.data ?? null;
  },
  lineage(id: string) {
    return _lineage[id]?.data ?? null;
  },
  recipeHistory(name: string) {
    return _recipeHistory[name]?.data ?? null;
  },
  recipeView(name: string) {
    return _recipeViews[name]?.data ?? null;
  },
  compareView(ids: string[]) {
    const key = ids.slice().sort().join(",");
    return _compareViews[key]?.data ?? null;
  },
};

// ---------- SSE stream ----------

let _streamConnected = $state(false);

export const stream = {
  get connected() {
    return _streamConnected;
  },
};

let eventSource: EventSource | null = null;

export function connectStream() {
  if (typeof window === "undefined" || eventSource) return;
  const es = new EventSource("/api/stream");
  eventSource = es;

  es.onopen = () => { _streamConnected = true; };

  es.addEventListener("run.created", (e: MessageEvent) => {
    // Whole list invalidated; refetch in background.
    parseId(e);
    if (_runs.data) loadRuns(true).catch(() => {});
  });
  es.addEventListener("run.updated", (e: MessageEvent) => {
    const id = parseId(e);
    if (!id) return;
    // Refresh list (status/duration may have changed) and the detail if cached.
    if (_runs.data) loadRuns(true).catch(() => {});
    if (_runDetails[id]?.data) loadRunDetail(id, true).catch(() => {});
    if (_runLogs[id]?.data) loadRunLog(id, true).catch(() => {});
    // Recipe history sparkline likely needs a bump.
    invalidateRecipeHistoryFor(id);
  });
  es.addEventListener("artifact.created", () => {
    if (_artifacts.data) loadArtifacts(true).catch(() => {});
  });

  es.onerror = () => {
    _streamConnected = false;
    if (import.meta.env.DEV) console.warn("SSE stream error; will reconnect");
  };
}

function parseId(e: MessageEvent): string | null {
  try {
    const data = JSON.parse(e.data);
    return typeof data?.id === "string" ? data.id : null;
  } catch {
    return null;
  }
}

function invalidateRecipeHistoryFor(runId: string) {
  // We don't track recipe-name → run-id reverse; cheapest is to bump every
  // history entry's loadedAt forward 0 (force on next read). The histories
  // are tiny (16 entries) so a refetch is fine.
  const summary = _runs.data?.find((r) => r.id === runId);
  if (!summary) return;
  const name = summary.recipe_name;
  if (_recipeHistory[name]) {
    delete _recipeHistory[name];
  }
}
