-- Per-address statements (/v1/export.csv): time-ordered lookups by chain + address.
CREATE INDEX IF NOT EXISTS trc20_transfers_chain_from_time_idx ON trc20_transfers (chain, from_address, block_time);
CREATE INDEX IF NOT EXISTS trc20_transfers_chain_to_time_idx   ON trc20_transfers (chain, to_address, block_time);
