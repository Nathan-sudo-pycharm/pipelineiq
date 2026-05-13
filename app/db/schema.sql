-- Pipeline configurations
-- Stores user-defined filter and transform rules per pipeline
CREATE TABLE IF NOT EXISTS pipeline_config (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    description TEXT,
    rules       JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Raw events
-- Every event that passes schema validation lands here
CREATE TABLE IF NOT EXISTS raw_events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL REFERENCES pipeline_config(id),
    source      TEXT NOT NULL,          -- 'webhook' | 'csv' | 'stream'
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Pre-computed rolling window aggregates
-- Written by workers at processing time, not computed at query time
CREATE TABLE IF NOT EXISTS aggregates (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id  UUID NOT NULL REFERENCES pipeline_config(id),
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    metric       TEXT NOT NULL,         -- e.g. 'temperature', 'request_count'
    count        INTEGER NOT NULL DEFAULT 0,
    avg          NUMERIC,
    min          NUMERIC,
    max          NUMERIC,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregates_unique_window
    ON aggregates(pipeline_id, window_start, metric);

-- Dead-letter log
-- Events that fail schema validation land here instead of being silently dropped
CREATE TABLE IF NOT EXISTS dead_letter (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID,                   -- nullable — may fail before pipeline is resolved
    source      TEXT NOT NULL,
    raw_payload TEXT NOT NULL,          -- stored as raw text since it may be malformed JSON
    error       TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_raw_events_pipeline_id   ON raw_events(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_received_at   ON raw_events(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_aggregates_pipeline_id   ON aggregates(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_aggregates_window_start  ON aggregates(window_start DESC);
CREATE INDEX IF NOT EXISTS idx_dead_letter_pipeline_id  ON dead_letter(pipeline_id);