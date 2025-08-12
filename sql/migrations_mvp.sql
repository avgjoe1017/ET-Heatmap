-- MVP schema additions and fixes (idempotent)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Audit logs
CREATE TABLE IF NOT EXISTS audit_logs (
  id SERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  event TEXT NOT NULL,
  level TEXT DEFAULT 'info',
  status INT,
  extra JSONB DEFAULT '{}'::jsonb
);

-- Source health
CREATE TABLE IF NOT EXISTS source_health (
  source TEXT PRIMARY KEY,
  last_ok TIMESTAMPTZ,
  last_error TIMESTAMPTZ,
  consecutive_errors INT DEFAULT 0,
  circuit_open_until TIMESTAMPTZ
);

-- Trend state
CREATE TABLE IF NOT EXISTS trend_state (
  entity_id INT PRIMARY KEY REFERENCES entities(id),
  last_gate_pass_ts TIMESTAMPTZ,
  consecutive_passes INT NOT NULL DEFAULT 0,
  last_alert_ts TIMESTAMPTZ,
  last_alert_heat DOUBLE PRECISION,
  prior_peak_heat DOUBLE PRECISION DEFAULT 0
);

-- Trade mentions
CREATE TABLE IF NOT EXISTS trade_mentions (
  entity_id INT REFERENCES entities(id),
  source TEXT NOT NULL,
  first_seen_ts TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (entity_id, source)
);
CREATE INDEX IF NOT EXISTS idx_trade_mentions_entity_first_seen ON trade_mentions (entity_id, first_seen_ts);

-- Alerts
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

-- KPI snapshots
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
