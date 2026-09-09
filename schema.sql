-- Shared schema for Turso vs D1 benchmark. Identical tables on both sides.
CREATE TABLE IF NOT EXISTS kv (
  id INTEGER PRIMARY KEY,
  payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bench_writes (
  id INTEGER PRIMARY KEY,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
