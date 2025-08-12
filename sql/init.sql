CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS entities (
  id SERIAL PRIMARY KEY,
  type TEXT NOT NULL,            -- person | project | event
  category TEXT,                 -- inferred category separate from type
  name TEXT NOT NULL UNIQUE,
  aliases TEXT[] DEFAULT '{}',
  wiki_id TEXT,
  imdb_id TEXT,
  studio TEXT,
  network TEXT
);

CREATE TABLE IF NOT EXISTS signals (
  entity_id INT REFERENCES entities(id),
  source TEXT NOT NULL,          -- youtube | reddit | wiki | trends | x | tiktok | rss
  ts TIMESTAMPTZ NOT NULL,
  metric TEXT NOT NULL,          -- views | mentions | comments | etc
  value DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (entity_id, ts, source, metric)
);

CREATE TABLE IF NOT EXISTS scores (
  entity_id INT REFERENCES entities(id),
  ts TIMESTAMPTZ NOT NULL,
  velocity_z DOUBLE PRECISION,
  accel DOUBLE PRECISION,
  xplat DOUBLE PRECISION,
  affect DOUBLE PRECISION,
  novelty DOUBLE PRECISION,
  et_fit DOUBLE PRECISION,
  tentpole DOUBLE PRECISION,
  decay DOUBLE PRECISION,
  risk DOUBLE PRECISION,
  heat DOUBLE PRECISION,
  reasons TEXT,
  PRIMARY KEY (entity_id, ts)
);

SELECT create_hypertable('signals', 'ts', if_not_exists => TRUE);
SELECT create_hypertable('scores', 'ts', if_not_exists => TRUE);

-- Helpful index for latest-per-entity queries
CREATE INDEX IF NOT EXISTS idx_scores_latest ON scores (entity_id, ts DESC);

-- Additional helpful indexes
CREATE INDEX IF NOT EXISTS idx_signals_entity_ts ON signals (entity_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_signals_source_metric_ts ON signals (source, metric, ts DESC);
-- Explicit index to match acceptance
CREATE INDEX IF NOT EXISTS idx_signals_entity_source_ts ON signals (entity_id, source, ts DESC);

-- Track discovery outcomes (optional analytics)
CREATE TABLE IF NOT EXISTS discovery_outcomes (
  id SERIAL PRIMARY KEY,
  entity TEXT NOT NULL,
  outcome TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  peak_score DOUBLE PRECISION,
  duration_days DOUBLE PRECISION,
  confidence DOUBLE PRECISION,
  velocity DOUBLE PRECISION
);

-- =========================
-- TimescaleDB policies
-- =========================
-- Enable compression and set policies. Adjust intervals via env in migrations later if needed.
-- Signals: compress after 7 days; retain 30 days
ALTER TABLE IF EXISTS signals SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'ts DESC',
  timescaledb.compress_segmentby = 'entity_id, source, metric'
);
SELECT add_compression_policy('signals', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('signals', INTERVAL '30 days', if_not_exists => TRUE);

-- Scores: compress after 14 days; retain 60 days (tunable)
ALTER TABLE IF EXISTS scores SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'ts DESC',
  timescaledb.compress_segmentby = 'entity_id'
);
SELECT add_compression_policy('scores', INTERVAL '14 days', if_not_exists => TRUE);
SELECT add_retention_policy('scores', INTERVAL '60 days', if_not_exists => TRUE);

-- Optional: continuous aggregate for daily average heat per entity (last 30 days window typical)
-- Note: Uses time_bucket for 1 day; join to entities lazily when querying
CREATE MATERIALIZED VIEW IF NOT EXISTS cagg_scores_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', ts) AS bucket,
  entity_id,
  AVG(heat) AS avg_heat,
  MAX(heat) AS max_heat
FROM scores
GROUP BY bucket, entity_id;

-- Refresh policy: keep last 30 days refreshed every hour
SELECT add_continuous_aggregate_policy(
  'cagg_scores_daily',
  start_offset => INTERVAL '30 days',
  end_offset   => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour',
  if_not_exists => TRUE
);

-- =========================
-- Audit logs and source health
-- =========================
CREATE TABLE IF NOT EXISTS audit_logs (
  id SERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  event TEXT NOT NULL,
  level TEXT DEFAULT 'info',
  status INT,
  extra JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS source_health (
  source TEXT PRIMARY KEY,
  last_ok TIMESTAMPTZ,
  last_error TIMESTAMPTZ,
  consecutive_errors INT DEFAULT 0,
  circuit_open_until TIMESTAMPTZ
);

-- =========================
-- Trend state for alert gating persistence
-- =========================
CREATE TABLE IF NOT EXISTS trend_state (
  entity_id INT PRIMARY KEY REFERENCES entities(id),
  last_gate_pass_ts TIMESTAMPTZ,
  consecutive_passes INT NOT NULL DEFAULT 0,
  last_alert_ts TIMESTAMPTZ,
  last_alert_heat DOUBLE PRECISION,
  prior_peak_heat DOUBLE PRECISION DEFAULT 0
);

-- =========================
-- Trade mentions (Variety, THR) first-seen tracking
-- =========================
CREATE TABLE IF NOT EXISTS trade_mentions (
  entity_id INT REFERENCES entities(id),
  source TEXT NOT NULL,              -- variety | thr | other
  first_seen_ts TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (entity_id, source)
);

-- Helpful indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_scores_entity_ts ON scores (entity_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_trade_mentions_entity_first_seen ON trade_mentions (entity_id, first_seen_ts);

-- =========================
-- Alerts log and KPI snapshots
-- =========================
CREATE TABLE IF NOT EXISTS alerts (
  id SERIAL PRIMARY KEY,
  entity_id INT REFERENCES entities(id),
  alert_ts TIMESTAMPTZ NOT NULL,
  heat DOUBLE PRECISION,
  reasons TEXT,
  pre_trade BOOLEAN DEFAULT FALSE,
  lead_time_minutes INT,
  usefulness BOOLEAN,
  feedback TEXT,
  alert_uuid UUID DEFAULT gen_random_uuid() UNIQUE,
  UNIQUE (entity_id, alert_ts)
);

CREATE INDEX IF NOT EXISTS idx_alerts_entity_ts ON alerts (entity_id, alert_ts DESC);

CREATE TABLE IF NOT EXISTS kpi_snapshots (
  ts TIMESTAMPTZ PRIMARY KEY,
  median_lead_time_seconds DOUBLE PRECISION,
  alert_usefulness DOUBLE PRECISION,
  false_positive_rate DOUBLE PRECISION,
  top_wins JSONB DEFAULT '{}'::jsonb
);

-- Votes for alerts usefulness metric
CREATE TABLE IF NOT EXISTS alert_votes (
  alert_uuid UUID NOT NULL,
  useful BOOLEAN,
  voter TEXT,
  ts TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (alert_uuid, voter, ts)
);
