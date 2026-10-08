-- Runtime watch list managed through the HTTP API (/v1/addresses).
-- Enabled rows are merged with the static filter.watch_addresses config.
CREATE TABLE IF NOT EXISTS watch_addresses (
    address    TEXT        PRIMARY KEY,               -- canonical base58 ("T…")
    label      TEXT,
    direction  TEXT        NOT NULL DEFAULT 'both',
    enabled    BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT watch_addresses_direction_chk CHECK (direction IN ('both', 'incoming', 'outgoing')),
    CONSTRAINT watch_addresses_address_chk   CHECK (address ~ '^T[1-9A-HJ-NP-Za-km-z]{33}$')
);

CREATE INDEX IF NOT EXISTS watch_addresses_enabled_idx ON watch_addresses (created_at) WHERE enabled;
