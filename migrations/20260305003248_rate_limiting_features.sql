-- Add max_concurrency directly to task_queue to avoid a separate table and triggers
ALTER TABLE underway.task_queue ADD COLUMN IF NOT EXISTS max_concurrency INTEGER;

-- Create the Global Rate Limiter table with DOUBLE PRECISION for TokenBucket fractional logic
CREATE TABLE IF NOT EXISTS underway.global_rate_limit (
    id TEXT PRIMARY KEY,
    tokens_used DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    window_start TIMESTAMPTZ NOT NULL DEFAULT now()
);
