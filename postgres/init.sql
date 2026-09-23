CREATE TABLE IF NOT EXISTS alerts (
  alert_id    TEXT PRIMARY KEY,          -- e.g. R1:A-000118 ; makes re-inserts harmless
  rule        TEXT NOT NULL,
  card_id     INT  NOT NULL,
  txn_id      TEXT NOT NULL,
  event_time  TEXT NOT NULL,
  details     TEXT,
  detected_at TIMESTAMPTZ DEFAULT now()
);
