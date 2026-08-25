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

import { SvelteMap } from "svelte/reactivity";
import { api } from "./api";
import type {
  ArtifactSummary,
  CompareView,
  PipelineSummary,
  PolicyCard,
  PolicyDetail,
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
const DETAIL_CACHE_LIMIT = 32;

// ---------- runes-backed state ----------

let _runs = $state<Collection<RunSummary[]>>({ data: null, loadedAt: null, loading: false, error: null });
let _pipelines = $state<Collection<PipelineSummary[]>>({ data: null, loadedAt: null, loading: false, error: null });
let _artifacts = $state<Collection<ArtifactSummary[]>>({ data: null, loadedAt: null, loading: false, error: null });
let _policies = $state<Collection<PolicyCard[]>>({ data: null, loadedAt: null, loading: false, error: null });
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

class DetailCache<T> {
  private readonly entries = new SvelteMap<string, Detail<T>>();
  private readonly recency = new Map<string, true>();

  get(key: string): Detail<T> | undefined {
    const entry = this.entries.get(key);
    if (entry) this.touch(key);
    return entry;
  }

  has(key: string): boolean {
    return this.entries.has(key);
  }

  set(key: string, entry: Detail<T>) {
    this.entries.set(key, entry);
    this.touch(key);
    if (this.recency.size <= DETAIL_CACHE_LIMIT) return;
    const oldest = this.recency.keys().next().value;
    if (oldest === undefined) return;
    this.recency.delete(oldest);
    this.entries.delete(oldest);
  }

  delete(key: string) {
    this.recency.delete(key);
    this.entries.delete(key);
  }

  private touch(key: string) {
    this.recency.delete(key);
    this.recency.set(key, true);
  }
}

const _runDetails = new DetailCache<RunDetailResponse>();
const _runLogs = new DetailCache<LogResponse>();
const _runEvents = new DetailCache<RunEvent[]>();
const _pipelineDetails = new DetailCache<PipelineDetail>();
const _artifactDetails = new DetailCache<{ artifact: ArtifactDetail; producer: RunSummary | null; consumers: RunSummary[] }>();
const _lineage = new DetailCache<LineageNode>();
const _recipeHistory = new DetailCache<RecipeHistory>();
const _recipeViews = new DetailCache<RecipeView>();
const _compareViews = new DetailCache<CompareView>();
const _policyDetails = new DetailCache<PolicyDetail>();

// In-flight tracking — dedupe concurrent calls for the same key.
const inflight = new Map<string, Promise<unknown>>();

function dedupe<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const existing = inflight.get(key);
  if (existing) return existing as Promise<T>;
  const p = fn().finally(() => inflight.delete(key));
  inflight.set(key, p);
  return p;
}

class StreamRefresh {
  private requestedGeneration = 0;
  private running = false;

  constructor(
    private readonly inflightKey: string,
    private readonly refresh: () => Promise<unknown>,
    private readonly onIdle: () => void,
  ) {}

  request() {
    this.requestedGeneration += 1;
    if (this.running) return;
    this.running = true;
    queueMicrotask(() => void this.drain());
  }

  private async drain() {
    try {
      await inflight.get(this.inflightKey);
    } catch {}
    for (;;) {
      const generation = this.requestedGeneration;
      try {
        await this.refresh();
      } catch {}
      if (generation === this.requestedGeneration) {
        this.running = false;
        this.onIdle();
        return;
      }
    }
  }
}

const streamRefreshes = new Map<string, StreamRefresh>();

function requestStreamRefresh(inflightKey: string, refresh: () => Promise<unknown>) {
  let scheduler = streamRefreshes.get(inflightKey);
  if (!scheduler) {
    let created: StreamRefresh;
    created = new StreamRefresh(inflightKey, refresh, () => {
      if (streamRefreshes.get(inflightKey) === created) {
        streamRefreshes.delete(inflightKey);
      }
    });
    scheduler = created;
    streamRefreshes.set(inflightKey, scheduler);
  }
  scheduler.request();
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

export async function loadPolicies(force = false): Promise<PolicyCard[]> {
  if (!force && _policies.data && isFresh(_policies.loadedAt)) return _policies.data;
  return dedupe("policies", async () => {
    if (!_policies.data) _policies.loading = true;
    try {
      const next = await api.policies();
      _policies.data = next;
      _policies.loadedAt = Date.now();
      _policies.error = null;
      return next;
    } catch (e) {
      _policies.error = e instanceof Error ? e.message : String(e);
      throw e;
    } finally {
      _policies.loading = false;
    }
  });
}

export async function loadPolicyDetail(name: string, force = false): Promise<PolicyDetail> {
  const entry = _policyDetails.get(name);
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`policy:${name}`, async () => {
    const next = await api.policy(name);
    _policyDetails.set(name, { data: next, loadedAt: Date.now() });
    return next;
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
  const entry = _runDetails.get(id);
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`run:${id}`, async () => {
    const next = await api.run(id);
    _runDetails.set(id, { data: next, loadedAt: Date.now() });
    return next;
  });
}

export async function loadRunLog(id: string, force = false): Promise<LogResponse> {
  const entry = _runLogs.get(id);
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`log:${id}`, async () => {
    const next = await api.runLog(id, 200);
    _runLogs.set(id, { data: next, loadedAt: Date.now() });
    return next;
  });
}

export async function loadRunEvents(id: string, force = false): Promise<RunEvent[]> {
  const entry = _runEvents.get(id);
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`events:${id}`, async () => {
    const next = await api.runEvents(id);
    _runEvents.set(id, { data: next, loadedAt: Date.now() });
    return next;
  });
}

export async function loadPipelineDetail(id: string, force = false): Promise<PipelineDetail> {
  const entry = _pipelineDetails.get(id);
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`pipeline:${id}`, async () => {
    const next = await api.pipeline(id);
    _pipelineDetails.set(id, { data: next, loadedAt: Date.now() });
    return next;
  });
}

export async function loadArtifactDetail(id: string, force = false) {
  const entry = _artifactDetails.get(id);
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`artifact:${id}`, async () => {
    const next = await api.artifact(id);
    _artifactDetails.set(id, { data: next, loadedAt: Date.now() });
    return next;
  });
}

export async function loadLineage(id: string, force = false): Promise<LineageNode> {
  const entry = _lineage.get(id);
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`lineage:${id}`, async () => {
    const next = await api.lineage(id);
    _lineage.set(id, { data: next, loadedAt: Date.now() });
    return next;
  });
}

export async function loadCompareView(ids: string[], force = false): Promise<CompareView> {
  const key = ids.slice().sort().join(",");
  const entry = _compareViews.get(key);
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`compare:${key}`, async () => {
    const next = await api.compare(ids);
    _compareViews.set(key, { data: next, loadedAt: Date.now() });
    return next;
  });
}

export async function loadRecipeView(name: string, force = false): Promise<RecipeView> {
  const entry = _recipeViews.get(name);
  if (!force && entry?.data && isFresh(entry.loadedAt)) return entry.data;
  return dedupe(`recipe:${name}`, async () => {
    const next = await api.recipe(name);
    _recipeViews.set(name, { data: next, loadedAt: Date.now() });
    return next;
  });
}

export async function loadRecipeHistory(name: string): Promise<RecipeHistory> {
  const entry = _recipeHistory.get(name);
  if (entry?.data) return entry.data;
  return dedupe(`recipe-history:${name}`, async () => {
    const next = await api.recipeHistory(name, 16);
    _recipeHistory.set(name, { data: next, loadedAt: Date.now() });
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
  get policies() {
    return _policies;
  },
  policyDetail(name: string) {
    return _policyDetails.get(name)?.data ?? null;
  },
  get cluster() {
    return _cluster;
  },
  runDetail(id: string) {
    return _runDetails.get(id)?.data ?? null;
  },
  runLog(id: string) {
    return _runLogs.get(id)?.data ?? null;
  },
  runEvents(id: string) {
    return _runEvents.get(id)?.data ?? null;
  },
  pipelineDetail(id: string) {
    return _pipelineDetails.get(id)?.data ?? null;
  },
  artifactDetail(id: string) {
    return _artifactDetails.get(id)?.data ?? null;
  },
  lineage(id: string) {
    return _lineage.get(id)?.data ?? null;
  },
  recipeHistory(name: string) {
    return _recipeHistory.get(name)?.data ?? null;
  },
  recipeView(name: string) {
    return _recipeViews.get(name)?.data ?? null;
  },
  compareView(ids: string[]) {
    const key = ids.slice().sort().join(",");
    return _compareViews.get(key)?.data ?? null;
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
    const id = parseId(e);
    if (!id) return;
    if (_runs.data) requestStreamRefresh("runs", () => loadRuns(true));
  });
  es.addEventListener("run.updated", (e: MessageEvent) => {
    const id = parseId(e);
    if (!id) return;
    if (_runs.data) requestStreamRefresh("runs", () => loadRuns(true));
    if (_runDetails.has(id)) {
      requestStreamRefresh(`run:${id}`, () => loadRunDetail(id, true));
    }
    if (_runLogs.has(id)) {
      requestStreamRefresh(`log:${id}`, () => loadRunLog(id, true));
    }
    if (_runEvents.has(id)) {
      requestStreamRefresh(`events:${id}`, () => loadRunEvents(id, true));
    }
    invalidateRecipeHistoryFor(id);
  });
  es.addEventListener("artifact.created", () => {
    if (_artifacts.data) requestStreamRefresh("artifacts", () => loadArtifacts(true));
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
  _recipeHistory.delete(name);
}
