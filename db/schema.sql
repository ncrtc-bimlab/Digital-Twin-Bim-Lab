-- Schema (safe to run many times).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Every reading ever received. TimescaleDB splits this by time automatically,
-- so it stays fast even with billions of rows.
CREATE TABLE IF NOT EXISTS readings (
  time       TIMESTAMPTZ      NOT NULL,
  sensor_id  TEXT             NOT NULL,
  station    TEXT,
  zone       TEXT,
  metric     TEXT             NOT NULL,
  value      DOUBLE PRECISION NOT NULL,
  unit       TEXT,
  protocol   TEXT,
  battery    REAL,
  rssi       INTEGER
);

SELECT create_hypertable('readings', 'time', if_not_exists => TRUE);

-- Fast lookup: "last 24 h of sensor AV070"
CREATE INDEX IF NOT EXISTS readings_sensor_time_idx
  ON readings (sensor_id, time DESC);

-- Day 3: the same reading can never be stored twice (needed because Kafka is "at least once").
-- On a hypertable a unique index must include the time column.
CREATE UNIQUE INDEX IF NOT EXISTS readings_unique_idx
  ON readings (sensor_id, metric, time);
