CREATE TABLE IF NOT EXISTS trc20_transfers (
    id           BIGSERIAL PRIMARY KEY,
    tx_id        TEXT        NOT NULL,
    log_index    INTEGER     NOT NULL,
    block_number BIGINT      NOT NULL,
    block_time   TIMESTAMPTZ NOT NULL,
    contract     TEXT        NOT NULL,
    symbol       TEXT        NOT NULL,
    from_address TEXT        NOT NULL,
    to_address   TEXT        NOT NULL,
    amount_raw   NUMERIC(78, 0) NOT NULL,
    amount       NUMERIC(78, 18) NOT NULL,
    reasons      TEXT[]      NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT trc20_transfers_tx_log_uniq UNIQUE (tx_id, log_index)
);

CREATE INDEX IF NOT EXISTS trc20_transfers_block_idx ON trc20_transfers (block_number);
CREATE INDEX IF NOT EXISTS trc20_transfers_from_idx  ON trc20_transfers (from_address, block_number DESC);
CREATE INDEX IF NOT EXISTS trc20_transfers_to_idx    ON trc20_transfers (to_address, block_number DESC);

CREATE TABLE IF NOT EXISTS listener_cursor (
    name       TEXT PRIMARY KEY,
    last_block BIGINT      NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
