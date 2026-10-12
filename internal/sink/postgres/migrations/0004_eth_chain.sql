-- Ethereum (ERC-20 USDT) support. Transfers need no schema change (chain is
-- free text; ETH rows have chain = 'eth', tx_id = 0x-prefixed lowercase hash).
-- Watch list: Ethereum addresses are stored in canonical lowercase "0x…" form.
ALTER TABLE watch_addresses DROP CONSTRAINT IF EXISTS watch_addresses_address_chk;
ALTER TABLE watch_addresses ADD CONSTRAINT watch_addresses_address_chk CHECK (
    (chain = 'tron' AND address ~ '^T[1-9A-HJ-NP-Za-km-z]{33}$')
 OR (chain = 'ton'  AND address ~ '^-?[0-9]+:[0-9a-f]{64}$')
 OR (chain = 'eth'  AND address ~ '^0x[0-9a-f]{40}$'));
