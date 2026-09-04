-- Supports deterministic newest-first run listings.
CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs (created_at DESC, id DESC);
