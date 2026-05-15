// Mirrors src/server.rs response shapes. Hand-written rather than generated
// because the API surface is small and stable.

export type RunStatus =
  | "created"
  | "submitted"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "timeout"
  | "oom"
  | "unknown_terminal"
  | "unknown";

export interface RunSummary {
  id: string;
  recipe_name: string;
  recipe_hash: string;
  status: RunStatus;
  job_id: string | null;
  run_dir: string;
  repo: string;
  created_at: number;
  finished_at: number | null;
  duration_secs: number | null;
  pipeline_id: string | null;
  stage_name: string | null;
  submitted_by: string | null;
  is_terminal: boolean;
}

export interface RunFull extends RunSummary {
  recipe: Record<string, unknown>;
  context: Record<string, unknown>;
  dependency_on: unknown;
  source_path: string;
}

export interface ArtifactSummary {
  id: string;
  kind: string;
  path: string;
  content_hash: string;
  producer_run_id: string | null;
  created_at: number;
  aliases?: string[];
}

export interface ArtifactDetail extends ArtifactSummary {
  metadata: Record<string, unknown>;
}

export interface InputResolution {
  role: string;
  artifact_id: string | null;
  resolved_path: string;
}

export interface RunDetailResponse {
  run: RunFull;
  inputs: InputResolution[];
  outputs: ArtifactSummary[];
  eval_series: EvalSeries[];
  tracking: Tracking;
}

export interface EvalSeries {
  policy_id: string;
  metric_name: string | null;
  latest_value: number | null;
  latest_step: number | null;
  previous_value: number | null;
  count: number;
  points: EvalSeriesPoint[];
}

export interface EvalSeriesPoint {
  step: number | null;
  value: number | null;
  metric_name: string | null;
  eval_run_id: string | null;
  checkpoint_artifact_id: string;
  state: string;
}

export interface Tracking {
  wandb: WandbTracking | null;
}

export interface WandbTracking {
  entity: string;
  project: string;
  group?: string;
  url: string;
}

export interface EvalRequest {
  eval_key: string;
  checkpoint_artifact_id: string;
  eval_recipe_hash: string;
  policy_id: string;
  eval_run_id: string | null;
  state: string;
  created_at?: number;
  updated_at?: number;
  /** Server-extracted headline metric. Present when the eval run produced
   *  a conforming `{tasks, primary}` result. */
  primary_metric?: string;
  primary_value?: number;
}

/**
 * Row shape for the Policies list view. `series` is the metric pivot
 * for the policy's primary metric, restricted to the few most recent
 * training runs that ran under this policy — enough to draw a sparkline
 * per row without paying for the full detail.
 */
export interface PolicyCard {
  name: string;
  primary_metric: string | null;
  total_count: number;
  failed_count: number;
  running_count: number;
  last_fired_at: number;
  series: MetricSeries | null;
}

/**
 * Full policy detail. `requests` is the raw eval-request log for the
 * activity drawer at the bottom of the page. The chart and leaderboard
 * read from `series_by_metric` (same shape as recipe/compare views).
 */
export interface PolicyDetail extends MetricPivotView {
  policy_name: string;
  requests: EvalRequest[];
}

export interface PipelineSummary {
  id: string;
  name: string;
  pipeline_path: string | null;
  created_at: number;
  stage_count: number;
  status: "running" | "succeeded" | "failed" | "mixed" | "unknown";
}

export interface PipelineDetail {
  pipeline: PipelineSummary;
  stages: (RunSummary & { dependency_on: unknown })[];
}

export interface LineageNode {
  artifacts: (ArtifactSummary & { is_root: boolean })[];
  runs: RunSummary[];
  edges: { from: string; to: string; kind: "produces" | "consumed_by"; role?: string }[];
  root_id: string;
}

export interface LogResponse {
  run_id: string;
  lines: string[];
  path: string | null;
  truncated: boolean;
}

export interface RecipeHistory {
  recipe_name: string;
  history: { status: RunStatus; created_at: number }[];
}

/**
 * Shared shape for Compare and Recipe views: one chart's worth of data
 * per metric, with one trajectory per run inside each metric. The
 * `metrics` array is the chip-selector source, sorted by descending
 * coverage (most-measured metric first).
 */
export interface MetricPivotView {
  /** Recipe name when the response is for /api/recipes/:name; absent
   *  for /api/compare. Both share the rest of the shape. */
  recipe_name?: string;
  runs: RunSummary[];
  metrics: string[];
  series_by_metric: MetricSeries[];
}

export type RecipeView = MetricPivotView;
export type CompareView = MetricPivotView;

export interface MetricSeries {
  metric_name: string;
  run_count: number;
  runs: MetricSeriesRun[];
}

export interface MetricSeriesRun {
  run_id: string;
  run_recipe_name: string;
  run_status: string;
  run_created_at: number;
  count: number;
  latest_value: number | null;
  latest_step: number | null;
  previous_value: number | null;
  points: MetricSeriesPoint[];
}

export interface MetricSeriesPoint {
  step: number | null;
  value: number;
  eval_run_id: string | null;
  state: string;
  checkpoint_artifact_id: string;
}

export interface ClusterInfo {
  name: string;
  registry_db: string;
  runs_base: string;
}

export interface RunEvent {
  event_type: string;
  payload: Record<string, unknown>;
  created_at: number;
}

export interface RolloutStep {
  step_num: number;
  action: string;
  response: string;
  reward: number;
  done: boolean;
  info: Record<string, unknown>;
}

export interface RolloutData {
  steps: RolloutStep[];
  frame_count: number;
}

// ---------- Dataset explorer (crowd-cast SFT per-segment datasets) ----------

export interface DatasetSegmentStats {
  n_keypress: number;
  n_keyrelease: number;
  n_mousepress: number;
  n_mouserelease: number;
  n_mousemove: number;
  n_scroll: number;
  n_context_changed: number;
  n_dangling_release: number;
  n_held_at_end: number;
  max_simultaneous_keys: number;
}

export interface DatasetSegment {
  split: string;
  segment_id: string;
  contributor_hash: string;
  n_frames: number;
  n_no_op: number;
  frame_width: number;
  frame_height: number;
  target_fps: number;
  /** ISO8601 UTC, or empty string when meta.json had no creation_time. */
  creation_time: string;
  stats: DatasetSegmentStats;
}

export interface DatasetSummary {
  splits: string[];
  n_segments: number;
  n_contributors: number;
  total_hours: number;
  /** (earliest, latest) creation_time, empty strings if unknown. */
  date_range: [string, string];
  segments: DatasetSegment[];
}

export interface SegmentDetail {
  split: string;
  segment_id: string;
  meta: Record<string, unknown>;
  /** One action string per frame, in source order. */
  actions: string[];
}
