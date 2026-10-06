-- Outbox of Meilisearch Lab billing events (spec §5.3). The worker inserts one row per
-- finished Lab job; the control plane's sender delivers them. Rows are never deleted
-- before delivery; delivered rows are purged after 7 days.
CREATE TABLE IF NOT EXISTS lab_events (
    id            UUID PRIMARY KEY,               -- the event id: redelivery-safe
    body          JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_attempt  TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS lab_events_due ON lab_events (next_attempt)
    WHERE delivered_at IS NULL;
