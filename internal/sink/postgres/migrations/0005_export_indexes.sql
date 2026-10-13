-- migrate:no-transaction
-- Built with CREATE INDEX CONCURRENTLY: trc20_transfers can be large and a
-- plain CREATE INDEX would block inserts for the whole build. Statements run
-- one by one outside a transaction; an index left INVALID by an interrupted
-- build is dropped and rebuilt on the next start.
-- (chain, block_number) moved here from 0003.
CREATE INDEX CONCURRENTLY IF NOT EXISTS trc20_transfers_chain_block_idx ON trc20_transfers (chain, block_number);
-- Per-address statements (/v1/export.csv): time-ordered lookups by chain + address.
CREATE INDEX CONCURRENTLY IF NOT EXISTS trc20_transfers_chain_from_time_idx ON trc20_transfers (chain, from_address, block_time);
CREATE INDEX CONCURRENTLY IF NOT EXISTS trc20_transfers_chain_to_time_idx   ON trc20_transfers (chain, to_address, block_time);
