CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS entities (
  id SERIAL PRIMARY KEY,
  type TEXT NOT NULL,            -- person | project | event
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
  novelty DOUBLE PRECISION,
  et_fit DOUBLE PRECISION,
  tentpole DOUBLE PRECISION,
  decay DOUBLE PRECISION,
  risk DOUBLE PRECISION,
  heat DOUBLE PRECISION,
  PRIMARY KEY (entity_id, ts)
);

SELECT create_hypertable('signals', 'ts', if_not_exists => TRUE);
SELECT create_hypertable('scores', 'ts', if_not_exists => TRUE);

-- Helpful index for latest-per-entity queries
CREATE INDEX IF NOT EXISTS idx_scores_latest ON scores (entity_id, ts DESC);
