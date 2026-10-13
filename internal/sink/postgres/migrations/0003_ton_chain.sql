-- Multi-chain support (TRON + TON). Backward compatible: existing rows and
-- callers that do not set a chain default to 'tron'. The table keeps its
-- historical name trc20_transfers; TON rows have chain = 'ton'.
ALTER TABLE trc20_transfers ADD COLUMN IF NOT EXISTS chain TEXT NOT NULL DEFAULT 'tron';
ALTER TABLE trc20_transfers ADD COLUMN IF NOT EXISTS memo  TEXT;
-- trc20_transfers_chain_block_idx is built CONCURRENTLY in 0005 (no write lock).

-- Watch list: TON addresses are stored in canonical raw form ("0:<64 hex>").
ALTER TABLE watch_addresses ADD COLUMN IF NOT EXISTS chain TEXT NOT NULL DEFAULT 'tron';
ALTER TABLE watch_addresses DROP CONSTRAINT IF EXISTS watch_addresses_address_chk;
ALTER TABLE watch_addresses ADD CONSTRAINT watch_addresses_address_chk CHECK (
    (chain = 'tron' AND address ~ '^T[1-9A-HJ-NP-Za-km-z]{33}$')
 OR (chain = 'ton'  AND address ~ '^-?[0-9]+:[0-9a-f]{64}$'));
CREATE INDEX IF NOT EXISTS watch_addresses_chain_idx ON watch_addresses (chain) WHERE enabled;
