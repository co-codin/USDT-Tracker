-- Self-service Telegram notify bot: users (one per private chat) and their
-- subscribed addresses. plan is the hook for paid tiers ('free' | 'premium' | …);
-- the per-plan address limit is configured in the app (notify_bot.*_limit).
CREATE TABLE IF NOT EXISTS notify_users (
    chat_id    BIGINT      PRIMARY KEY,
    username   TEXT,
    plan       TEXT        NOT NULL DEFAULT 'free',
    blocked    BOOLEAN     NOT NULL DEFAULT FALSE,   -- user blocked the bot (403): skip deliveries
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notify_subscriptions (
    chat_id    BIGINT      NOT NULL REFERENCES notify_users (chat_id) ON DELETE CASCADE,
    chain      TEXT        NOT NULL,
    address    TEXT        NOT NULL,                 -- canonical form (see chainaddr)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, chain, address),
    CONSTRAINT notify_subscriptions_chain_chk CHECK (chain IN ('tron', 'ton', 'eth'))
);
CREATE INDEX IF NOT EXISTS notify_subscriptions_addr_idx ON notify_subscriptions (chain, address);

-- getUpdates offset so a restart does not replay or lose commands.
CREATE TABLE IF NOT EXISTS notify_bot_state (
    name        TEXT PRIMARY KEY,
    next_offset BIGINT NOT NULL
);
