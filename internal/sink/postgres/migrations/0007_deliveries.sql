-- Durable outbox for listener taps (notify bot DMs, large-transfer alerts).
-- Rows are inserted before the block's cursor is saved (ON CONFLICT DO
-- NOTHING makes block replays idempotent) and finalised after sending, so a
-- restart neither drops nor duplicates messages. scope is the cursor name
-- (several deployments may share one database).
CREATE TABLE IF NOT EXISTS deliveries (
    scope      TEXT        NOT NULL,
    kind       TEXT        NOT NULL,                 -- 'notify' | 'alert'
    key        TEXT        NOT NULL,                 -- e.g. <chat>|<transfer id>|<dir>
    chat_id    BIGINT      NOT NULL DEFAULT 0,
    payload    JSONB       NOT NULL,
    attempts   INT         NOT NULL DEFAULT 0,
    status     TEXT        NOT NULL DEFAULT 'pending', -- pending | sent | failed | dropped
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at    TIMESTAMPTZ,
    PRIMARY KEY (scope, kind, key)
);
CREATE INDEX IF NOT EXISTS deliveries_pending_idx ON deliveries (scope, kind, created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS deliveries_done_idx ON deliveries (done_at) WHERE status <> 'pending';
