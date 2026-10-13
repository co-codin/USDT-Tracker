# USDT-Tracker (tron-usdt-listener)

[![Go](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A **read-only** listener for **USDT transfers on TRON (TRC-20), TON (USD₮ jetton) and Ethereum (ERC-20)**, written in Go.
It follows each chain block by block, decodes every `Transfer` event of the USDT contract
[`TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t`](https://tronscan.org/#/token20/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t)
on TRON and every transfer of the official USD₮ jetton master
[`EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs`](https://tonviewer.com/EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs)
on TON and every ERC-20 `Transfer` of Tether USD
[`0xdAC17F958D2ee523a2206206994597C13D831ec7`](https://etherscan.io/token/0xdAC17F958D2ee523a2206206994597C13D831ec7)
on Ethereum, filters them (watch list, large-transfer threshold) and fans them out to pluggable sinks:
**stdout JSON**, **signed webhooks**, **Telegram alerts** and **PostgreSQL**.

Typical uses: deposit detection for a payment flow, treasury / whale alerts, feeding an analytics
pipeline, compliance monitoring of a set of addresses.

> ℹ️ **Naming:** the project is called **USDT-Tracker**; the Go module, repository folder and binary
> keep the historical name `tron-usdt-listener` (it started as a TRON-only listener).

> 🔒 **Read-only by design.** The listener only calls public query endpoints. It never asks for,
> stores or handles private keys and has no code path that signs or broadcasts transactions.

---

## Features

- **Block-range processing with a persisted cursor** – resumes exactly where it stopped, no gaps
  (file cursor by default, PostgreSQL when the Postgres sink is enabled).
- **Confirmations per chain** – each chain stays `N` blocks behind its head: TRON 20 (≈ 60 s, solidified),
  TON 3 masterchain blocks (indexer lag margin; TON blocks are final), Ethereum 12
  (`LISTENER_CONFIRMATIONS`, `TON_CONFIRMATIONS`, `ETH_CONFIRMATIONS`).
- **At-least-once delivery, deduplicated** – the cursor only advances after every required sink
  accepted a block; per-sink dedupe by `tx_id + log_index` so a retried block never double-sends to
  sinks that already succeeded. Postgres has a unique key; webhooks carry stable idempotency ids.
- **Resilient read-only clients for every chain** – client-side rate limiting, retries with exponential
  backoff + jitter, honours `429` / `Retry-After`. TRON (TronGrid HTTP) also handles the 403 "frequency limit"
  and verifies ambiguous empty blocks; TON (toncenter v3) retries blocks the indexer does not have yet;
  Ethereum (JSON-RPC `eth_getLogs`) halves the log range when a node rejects it.
- **Filters** – watch list (incoming / outgoing / both), minimum amount for large-transfer alerts,
  `any` (watched OR large) / `all` (watched AND large) modes, or firehose mode (everything).
- **Runtime watch list + admin API** – add/remove deposit addresses over HTTP
  (`/v1/addresses`, bearer token) without a restart; stored in Postgres and merged with the static
  config list. See [Runtime watch list (admin API)](#runtime-watch-list-admin-api).
- **Sinks** – stdout (structured JSON via `slog`), webhook (HMAC-SHA256 signed, retries),
  Telegram bot, PostgreSQL (`pgx`, embedded migrations). Required vs best-effort per sink.
- **Ops** – Prometheus `/metrics`, `/healthz` (503 when stalled), graceful shutdown on
  SIGINT/SIGTERM (finishes the in-flight block), distroless Docker image (~19 MB), docker-compose.
- **TRON, TON and Ethereum, separately or together** – `chains: [tron]` (default), `[ton]`, `[eth]` or any combination such as `[tron, ton, eth]`.
  Each chain runs its own loop and cursor; sinks, filter rules, metrics (label `chain`) and the admin
  API are shared. See [TON](#ton-usd-jetton) and [Ethereum](#ethereum-erc-20-usdt).
- **Large-transfer channel** – posts every transfer ≥ 100 000 USDT (configurable) on any enabled chain
  to a Telegram channel, paced and batched. See [Large-transfer channel](#large-transfer-channel-whale-alerts).
- **CSV / accounting export** – per-address statement via `/v1/export.csv` or the `export` subcommand.
  See [CSV / accounting export](#csv--accounting-export).
- **Telegram notify bot** – users `/add` their TRON / TON / Ethereum address and get a DM on every transfer;
  free tier limit + `premium` plan hook. See [Telegram notify bot](#telegram-notify-bot-self-service).
- **Backfill mode** – `-from/-to` (plus `-chain` when several chains are enabled) re-processes any
  historical range without touching the cursor.
- Works **without an API key** at a low request rate; set `TRON_PRO_API_KEY` for more headroom.
- Any TRC-20 token can be watched by changing `token.contract/decimals/symbol`.

## Architecture

```mermaid
flowchart LR
    TRON["TRON HTTP API (TronGrid)"]
    TONAPI["toncenter v3 / TON Index"]
    ETHRPC["Ethereum JSON-RPC"]

    subgraph L["tron-usdt-listener"]
        S1["chain/tron/trc20.Source"]
        S2["chain/ton/jetton.Source"]
        S3["chain/eth/erc20.Source"]
        F["filter<br/>static + runtime watch list · min amount"]
        P["listener loop (one per chain)<br/>head − confirmations<br/>ordered delivery"]
        X["sink.Dispatcher<br/>per-sink dedupe (tx_id:log_index)"]
        K[("cursor<br/>file | Postgres")]
        M["/metrics · /healthz · /v1/addresses · /v1/export.csv"]
        A["alert<br/>large-transfer channel"]
        N["notify bot<br/>(tgbot long polling)"]
    end

    TRON --> S1 --> P
    TONAPI --> S2 --> P
    ETHRPC --> S3 --> P
    P --> A --> TC["Telegram channel"]
    P --> N --> TU["Telegram users (DM)"]
    P --> F --> X
    P <--> K
    X --> O["stdout JSON"]
    X --> W["Webhook<br/>HMAC-SHA256"]
    X --> T["Telegram bot"]
    X --> G[("PostgreSQL")]
    P -.-> M
    M -. "add / remove address" .-> G
    G -. "enabled rows (reload ≤ 5s)" .-> F
```

Processing loop (shown for TRON; TON and Ethereum follow the same loop with their own sources):

1. Poll head (`/wallet/getblock`, header only), compute `safe = head − confirmations`.
2. Fetch blocks `cursor+1 … min(safe, cursor+batch_size)` concurrently (one
   `gettransactioninfobyblocknum` call per block), but **deliver strictly in order**.
3. Decode `Transfer(address,address,uint256)` logs emitted by the token contract in **successful**
   transactions (reverted/failed ones are skipped; transfers made inside contract calls – DEX swaps,
   routers, multisends – are included).
4. Filter → dispatch to all sinks → persist cursor. If a required sink fails, the same block is
   retried with backoff; nothing moves forward until it succeeds.

Repository layout (chain-specific code lives under `internal/chain/<chain>`, so another backend,
e.g. Solana, can be added as `internal/chain/sol` implementing `chain.Source`):

```
cmd/listener/                 main: flags, logging, signals, -healthcheck
internal/
  app/                        composition root (wires everything; used by main and e2e tests)
  chain/                      chain-agnostic Source interface + Block
    tron/                     TRON: base58check addresses, read-only HTTP client
      trc20/                  TRC-20 Transfer decoding + chain.Source implementation
      trontest/               in-process mock TRON node for integration tests
    ton/                      TON: raw/user-friendly addresses, shards, read-only toncenter v3 client
      jetton/                 USD₮ jetton transfers per masterchain block (chain.Source)
      tontest/                in-process mock toncenter index for integration tests
    eth/                      Ethereum: EIP-55 addresses, read-only JSON-RPC client (eth_blockNumber/eth_getLogs)
      erc20/                  ERC-20 Transfer logs over chunked block ranges (chain.Source + RangeSource)
      ethtest/                in-process mock JSON-RPC node for integration tests
  chainaddr/                  per-chain address validation / canonical form
  alert/                      large-transfer Telegram channel (paced, batched, any chain)
  export/                     CSV statement per address (/v1/export.csv, `export` subcommand)
  explorer/                   tronscan / tonviewer / etherscan links, display forms of addresses
  notify/                     self-service Telegram notify bot (commands, subscriptions, dispatch)
  tgbot/                      minimal Bot API client (sendMessage, getUpdates); tgtest/ mock Bot API
  config/                     YAML + env overrides + validation
  cursor/                     cursor stores (file, memory) + dedupe set
  filter/                     watch list / threshold matching, Dynamic (static + DB) filter
  listener/                   block-range loop, confirmations, retries, status
  metrics/                    Prometheus collectors
  model/                      chain-neutral Transfer + amount helpers
  retry/                      backoff
  server/                     /metrics, /healthz; mounts the admin API
  sink/                       Sink interface, dispatcher, stdout, webhook, telegram
    postgres/                 pgx store: sink + cursor + watch_addresses, embedded migrations
  watch/                      runtime watch list: entries, validation, Store, admin API (/v1/addresses)
pkg/webhooksig/               sign/verify helpers you can import in webhook consumers
deploy/helm/usdt-tracker/     Helm chart (Kubernetes)
Dockerfile, docker-compose.yml
```

## Quick start

### Local (Go 1.26+)

```bash
git clone https://github.com/co-codin/USDT-Tracker && cd USDT-Tracker
make build
./bin/tron-usdt-listener            # firehose: every USDT transfer as JSON on stdout
```

With a config file and filters:

```bash
cp config.example.yaml config.yaml   # edit filter / sinks
cp .env.example .env                 # optional: API key, Telegram, webhook secrets
make run
```

Only watch two addresses and large transfers ≥ 100k USDT, pretty-printed:

```bash
FILTER_WATCH_ADDRESSES=TXXXX...,TYYYY... FILTER_MIN_AMOUNT=100000 \
  ./bin/tron-usdt-listener 2>/dev/null | jq .
```

Re-process a historical range (cursor untouched):

```bash
./bin/tron-usdt-listener -from 86883900 -to 86883910
```

Application logs go to **stderr**; the stdout sink writes one JSON object per transfer to **stdout**.

### Docker Compose (listener + PostgreSQL)

```bash
cp .env.example .env      # optional
docker compose up -d --build
docker compose logs -f listener
curl -s localhost:9090/healthz
docker compose exec postgres psql -U listener -d tron \
  -c "select block_number, from_address, to_address, amount from trc20_transfers order by id desc limit 5"
```

In compose the Postgres sink is enabled and also stores the cursor (`listener_cursor` table).
For TON add `CHAINS=tron,ton` (or `ton`) and `TONCENTER_API_KEY` to `.env`.

### Kubernetes (Helm)

The chart lives in [`deploy/helm/usdt-tracker`](deploy/helm/usdt-tracker). It deploys the app image
(single replica, `Recreate` strategy – the listener is a single writer), a Service on port `9090`
(`/metrics`, `/healthz`, `/v1/addresses`), optional Ingress (only `/v1` by default) and
ServiceMonitor, liveness/readiness probes on `/healthz`, resource requests/limits, and a non-root,
read-only-root-filesystem pod. By default it also installs PostgreSQL via the Bitnami subchart.

```bash
# 1. Build and push the image (the chart's default ghcr.io/co-codin/usdt-tracker:latest is a placeholder)
docker build -t ghcr.io/<you>/usdt-tracker:v0.2.0 . && docker push ghcr.io/<you>/usdt-tracker:v0.2.0

# 2. Install with the bundled Postgres and the admin API enabled
helm dependency build deploy/helm/usdt-tracker
helm upgrade --install usdt deploy/helm/usdt-tracker -n usdt --create-namespace \
  --set image.repository=ghcr.io/<you>/usdt-tracker --set image.tag=v0.2.0 \
  --set secrets.apiToken="$(openssl rand -hex 32)" \
  --set secrets.tronApiKey="$TRON_PRO_API_KEY"

# 3. Use it
kubectl -n usdt port-forward svc/usdt-usdt-tracker 9090:9090 &
TOKEN=$(kubectl -n usdt get secret usdt-usdt-tracker -o jsonpath='{.data.API_TOKEN}' | base64 -d)
curl -s localhost:9090/v1/addresses -H "Authorization: Bearer $TOKEN"
```

**API token and other secrets.** Either pass them as values (`secrets.apiToken`, `secrets.tronApiKey`,
`secrets.telegramBotToken`, `secrets.webhookSecret`; the chart creates a Secret) or create the Secret
yourself and reference it – recommended with GitOps / sealed-secrets / external-secrets:

```bash
kubectl -n usdt create secret generic usdt-secrets \
  --from-literal=API_TOKEN="$(openssl rand -hex 32)" --from-literal=TRON_PRO_API_KEY=...
helm upgrade --install usdt deploy/helm/usdt-tracker -n usdt --set secrets.existingSecret=usdt-secrets
# if usdt-secrets also holds DATABASE_URL (external DB):
#   --set postgresql.enabled=false --set secrets.existingSecretHasDatabaseUrl=true
```

Recognised keys: `API_TOKEN`, `TRON_PRO_API_KEY`, `TONCENTER_API_KEY`, `TELEGRAM_BOT_TOKEN`,
`WEBHOOK_SECRET`, `DATABASE_URL`.

**TON in Kubernetes:** `--set 'config.chains={tron,ton}' --set secrets.toncenterApiKey=… --set config.ton.rps=9`
(plus `config.ton.watch_addresses` for static TON addresses). The chart rejects unknown chains at
render time and warns in NOTES when TON runs without an API key. No token = admin API disabled. `values.yaml` only contains empty placeholders.

**External Postgres (recommended for production).** Disable the subchart and give a DSN, either inline
(stored in the chart's Secret) or from an existing Secret:

```bash
helm upgrade --install usdt deploy/helm/usdt-tracker -n usdt \
  --set postgresql.enabled=false \
  --set externalDatabase.existingSecret=usdt-db --set externalDatabase.existingSecretKey=DATABASE_URL
# or: --set postgresql.enabled=false --set externalDatabase.url='postgres://user:pass@db:5432/tron?sslmode=require'
```

Non-secret settings (filters, sinks, confirmations…) go under `config:` in values (same schema as
`config.example.yaml`, rendered into a ConfigMap). Notes: free Bitnami PostgreSQL images are only
published as `latest` and meant for non-production use, so treat the bundled database as a demo
default; without any database the cursor is kept in an `emptyDir` (enable `persistence.enabled` or,
better, use Postgres) and the admin API answers `503`.

## Configuration

Configuration is read from `config.yaml` (or `-config path` / `CONFIG_FILE`) and then overridden by
environment variables. Empty env values are ignored. Unknown YAML keys are rejected (typo-safe).
See [`config.example.yaml`](config.example.yaml) for every option with comments.

| YAML key | Env var | Default | Description |
|---|---|---|---|
| `tron.api_url` | `TRON_API_URL` | `https://api.trongrid.io` | Any TRON full-node HTTP API |
| `tron.api_key` | `TRON_PRO_API_KEY` | – | Optional TronGrid key (higher limits) |
| `tron.rps` | `TRON_RPS` | `3` | Client-side request rate limit |
| `tron.timeout` / `tron.max_retries` | – | `15s` / `8` | Per request |
| `token.contract` | `TOKEN_CONTRACT` | USDT | Any TRC-20 contract (base58 or hex) |
| `token.symbol` / `token.decimals` | – | `USDT` / `6` | |
| `listener.confirmations` | `LISTENER_CONFIRMATIONS` | `20` | Blocks behind head |
| `listener.start_block` | `LISTENER_START_BLOCK` | `0` | First run only: `0` = safe head, `N` = block N, `-N` = N blocks back |
| `listener.poll_interval` | `LISTENER_POLL_INTERVAL` | `3s` | Head polling when caught up |
| `listener.batch_size` | `LISTENER_BATCH_SIZE` | `20` | Max blocks per iteration |
| `listener.concurrency` | `LISTENER_CONCURRENCY` | `2` | Parallel block fetches |
| `listener.dedupe_cache_size` | – | `100000` | Remembered keys per sink |
| `cursor.file` | `CURSOR_FILE` | `./data/cursor.json` | File cursor (unused with Postgres) |
| `filter.watch_addresses` | `FILTER_WATCH_ADDRESSES` | – | Comma-separated in env |
| `filter.direction` | `FILTER_DIRECTION` | `both` | `both` \| `incoming` \| `outgoing` |
| `filter.min_amount` | `FILTER_MIN_AMOUNT` | `0` | Large-transfer threshold in USDT |
| `filter.mode` | `FILTER_MODE` | `any` | `any` = watched OR large, `all` = AND |
| `filter.reload_interval` | `FILTER_RELOAD_INTERVAL` | `5s` | How often enabled `watch_addresses` rows are re-read (API writes apply immediately) |
| `http.addr` | `HTTP_ADDR` | `:9090` | `/metrics`, `/healthz`, admin API; empty disables |
| `api.token` | `API_TOKEN` | – | Bearer token for `/v1/*` (≥ 16 chars); **empty disables the admin API** |
| `http.health_stale_after` | – | `2m` | `/healthz` → 503 without progress |
| `sinks.stdout.enabled` | `SINK_STDOUT_ENABLED` | `true` | |
| `sinks.webhook.enabled` | `SINK_WEBHOOK_ENABLED` | `false` | |
| `sinks.webhook.url` / `.secret` | `WEBHOOK_URL` / `WEBHOOK_SECRET` | – | |
| `sinks.telegram.enabled` | `SINK_TELEGRAM_ENABLED` | `false` | |
| `sinks.telegram.bot_token` / `.chat_id` | `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | – | |
| `sinks.telegram.min_amount` | `TELEGRAM_MIN_AMOUNT` | – | Extra alert threshold (watched-address hits always alert) |
| `sinks.postgres.enabled` | `SINK_POSTGRES_ENABLED` | `false` | Also switches the cursor to Postgres |
| `sinks.postgres.dsn` | `DATABASE_URL` | – | `postgres://user:pass@host:5432/db` |
| `log.level` / `log.format` | `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json` | `text` for humans |
| `chains` | `CHAINS` | `tron` | `tron`, `ton` or `tron,ton` |
| `ton.api_url` | `TON_API_URL` | `https://toncenter.com/api/v3` | toncenter v3 (TON Index) compatible API |
| `ton.api_key` | `TONCENTER_API_KEY` | – | toncenter key (`X-API-Key`); **strongly recommended** |
| `ton.rps` | `TON_RPS` | `1` | Client-side rate limit (keyless toncenter ≈ 1 rps; ~9 with a free key) |
| `ton.jetton_master` | `TON_JETTON_MASTER` | USD₮ `EQCxE6…_sDs` | Any jetton master |
| `ton.symbol` / `ton.decimals` | – | `USDT` / `6` | |
| `ton.watch_addresses` | `TON_WATCH_ADDRESSES` | – | TON addresses (`EQ…`/`UQ…`/`0:<hex>`), comma-separated in env |
| `ton.confirmations` | `TON_CONFIRMATIONS` | `3` | Masterchain blocks behind the indexed head |
| `ton.start_block` | `TON_START_BLOCK` | `0` | Like `listener.start_block`, in masterchain seqnos |
| `ton.poll_interval` | `TON_POLL_INTERVAL` | `1s` | |
| `ton.batch_size` / `ton.concurrency` | – | `20` / `2` | |
| `ton.cursor_file` | `TON_CURSOR_FILE` | `./data/cursor-ton.json` | With Postgres: cursor row `<cursor_name>:ton` |
| `eth.rpc_url` | `ETH_RPC_URL` | `https://ethereum-rpc.publicnode.com` | Any Ethereum JSON-RPC endpoint. Treated as a secret (provider keys live in the URL): only `scheme://host` is logged |
| `eth.rps` | `ETH_RPS` | `5` | Client-side rate limit |
| `eth.contract` | `ETH_CONTRACT` | USDT `0xdAC17F…1ec7` | Any ERC-20 (`eth.symbol` / `eth.decimals`, default `USDT` / `6`) |
| `eth.watch_addresses` | `ETH_WATCH_ADDRESSES` | – | Ethereum addresses (`0x…`, lowercase or EIP-55), comma-separated in env |
| `eth.confirmations` | `ETH_CONFIRMATIONS` | `12` | Blocks behind head (reorg margin) |
| `eth.start_block` | `ETH_START_BLOCK` | `0` | Like `listener.start_block` |
| `eth.poll_interval` | `ETH_POLL_INTERVAL` | `6s` | |
| `eth.batch_size` | `ETH_BATCH_SIZE` | `50` | Blocks per iteration |
| `eth.log_chunk` | `ETH_LOG_CHUNK` | `50` | Max blocks per `eth_getLogs` call; halved automatically when the node refuses a range |
| `eth.head_tag` | `ETH_HEAD_TAG` | `latest` | Head block: `latest` (minus `ETH_CONFIRMATIONS`), `safe` or `finalized` (reorg-proof, ~13 min behind; use with `ETH_CONFIRMATIONS=0`). Logs and block headers are fetched in one JSON-RPC batch and hash-checked, so a lagging RPC backend cannot make blocks look empty; a reorg deeper than the confirmations is logged and counted in `reorgs_detected_total` |
| `eth.cursor_file` | `ETH_CURSOR_FILE` | `./data/cursor-eth.json` | With Postgres: cursor row `<cursor_name>:eth` |

`filter.direction`, `filter.min_amount` and `filter.mode` apply to both chains; `filter.watch_addresses`
holds TRON addresses, `ton.watch_addresses` TON addresses and `eth.watch_addresses` Ethereum addresses (putting a TON address in the TRON list is
rejected with a hint).

Every sink has `best_effort` (Telegram defaults to `true`, others to `false`). A **required** sink
blocks progress until it succeeds (no data loss); a **best-effort** sink only logs and counts failures.

## Runtime watch list (admin API)

Payment gateways usually create a fresh deposit address per order. Instead of editing
`filter.watch_addresses` and restarting, add and remove addresses over HTTP while the listener runs.

**Requirements:** the Postgres sink (addresses live in the `watch_addresses` table) and an API token.
The API is served on the existing ops server (`http.addr`, default `:9090`) under `/v1/`.

```bash
export API_TOKEN=$(openssl rand -hex 32)       # or api.token in config.yaml
SINK_POSTGRES_ENABLED=true DATABASE_URL=postgres://... API_TOKEN=$API_TOKEN ./bin/tron-usdt-listener
```

| Request | Success | Errors |
|---|---|---|
| `GET /v1/addresses[?chain=tron\|ton]` | `200 {"addresses":[…],"static_addresses":[…]}` | `400` unknown chain |
| `POST /v1/addresses` body `{"address":"T…","chain":"tron","label":"order 42","direction":"incoming"}` (`chain`, `label`, `direction` optional; chain is detected from the address when omitted; direction `both` \| `incoming` \| `outgoing`, default `both`) | `201` + the stored row | `400` invalid address/JSON or chain not enabled, `409` already watched |
| `DELETE /v1/addresses/{address}` | `204` | `400` invalid address, `404` not in the runtime list |

Every request needs `Authorization: Bearer <token>` (`401` otherwise). With an empty token the API is
**disabled** (`404` for every `/v1` path – it is never left open). With a token but the Postgres sink
disabled the API answers `503` with an explanation; the static config list keeps working as before.
TRON addresses must be base58 `T…` addresses with a valid checksum (hex is rejected by the API).
TON addresses may be raw (`0:<hex>`) or user-friendly (`EQ…`/`UQ…`, use the URL-safe form in
`DELETE` paths); they are stored and returned in raw form, and every row has a `"chain"` field.
Adding a TON address to a listener that does not run TON is rejected with `400`.

```bash
H="Authorization: Bearer $API_TOKEN"

# add a deposit address (incoming transfers only)
curl -s -X POST localhost:9090/v1/addresses -H "$H" -H 'Content-Type: application/json' \
  -d '{"address":"TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv","label":"order 1001","direction":"incoming"}'
# {"chain":"tron","address":"TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv","label":"order 1001","direction":"incoming",
#  "enabled":true,"created_at":"2026-10-08T20:41:07.512Z"}

# list (runtime rows + read-only static config addresses)
curl -s localhost:9090/v1/addresses -H "$H" | jq .

# a TON deposit address (chain detected from the address; stored as raw 0:<hex>)
curl -s -X POST localhost:9090/v1/addresses -H "$H" -H 'Content-Type: application/json' \
  -d '{"address":"UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg","label":"ton order 7","direction":"incoming"}'
# {"chain":"ton","address":"0:9db2cae853cf2db870ea06d1e8286f202cd9ad689dfc74b98195520d06495859",...}

# remove it once the order is paid
curl -s -X DELETE localhost:9090/v1/addresses/TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv -H "$H" -w '%{http_code}\n'
# 204
```

How it works:

- The filter merges the static `filter.watch_addresses` (with `filter.direction`) with the **enabled**
  rows of `watch_addresses` (each with its own `direction`). If an address is in both with different
  directions, the union (`both`) applies.
- An API write invalidates the cached list, so the change applies from the **next processed block**.
  Rows changed directly in SQL (or by another instance) are picked up within `filter.reload_interval`
  (default `5s`). Since blocks are processed `confirmations` (default 20 ≈ 60 s) behind head, an
  address added right before a customer pays is in place long before that block is processed.
- The list is refreshed before each block; if Postgres cannot be read the block is retried (cursor
  does not move), so no block is filtered with a list that failed to load.
- With the API enabled, an **empty** watch list matches nothing (only the `min_amount` rule, if set) –
  removing the last deposit address never turns the listener into a firehose. Without a token, the
  legacy rule applies: no static addresses, no DB rows and no `min_amount` → every transfer.
- To pause an address without deleting it, set `enabled = false` in SQL (no API endpoint for that yet).

**Security:** the token is compared in constant time; request bodies are capped at 4 KB. The API
has no TLS of its own – keep `:9090` on localhost/private network (docker-compose binds
`127.0.0.1:9090`) or put it behind a TLS reverse proxy. `/metrics` and `/healthz` stay unauthenticated.

## Telegram notify bot (self-service)

A public bot where anyone can watch their own wallets – the free entry point of a hosted offering:

```
/add TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv     ✅ Watching TRON address …
/add UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg
/list                                        📋 Your addresses (2/3, free plan)
/remove 1                                    🗑 Stopped watching …

📥 +1,250 USDT received on TRON
To: TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv
From: TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx
View transaction · 2026-10-12 21:25 UTC
```

```bash
NOTIFY_BOT_ENABLED=true TELEGRAM_NOTIFY_BOT_TOKEN=<token from @BotFather> \
SINK_POSTGRES_ENABLED=true DATABASE_URL=postgres://… CHAINS=tron,ton,eth ./bin/tron-usdt-listener
```

- Commands: `/start`, `/help`, `/add <address>` (chain auto-detected: TRON `T…`, TON `UQ…`/`EQ…`/`0:…`,
  Ethereum `0x…` with EIP-55 check; a bare address also works), `/list`, `/remove <address|number>`, `/plan`.
  Only chains enabled in `CHAINS` are accepted.
- Runs **inside the listener** (same process, long polling via `getUpdates` – no public URL / webhook).
  Users, subscriptions and the update offset live in Postgres (migration `0006`: `notify_users`,
  `notify_subscriptions`, `notify_bot_state`).
- Fed with every decoded transfer (independent of the watch list and filter); incoming, outgoing and
  self transfers; transfers below `min_amount` (default 0.01) are skipped to ignore zero-value
  address-poisoning spam. Global send rate 25 msg/s, several notifications for one chat are merged into
  one message, a user who blocked the bot (`403`) is paused until they `/start` again.
- **Plans / premium hook:** `notify_users.plan` (`free` by default) → limit `free_limit` (3) or
  `premium_limit` (50) addresses. There is no payment code: upgrade a user with
  `UPDATE notify_users SET plan = 'premium' WHERE chat_id = …` (or wire your billing to it).
- Use a separate bot token from the alert bots (one `getUpdates` consumer per token).
- **Abuse limits:** private chats only (in groups any member could change the group's
  subscriptions, so the bot only answers with a hint there); each chat may run
  `commands_per_minute` commands (default 20, burst 5 – extras are dropped silently); command replies
  share the global send rate with notifications; a user row is created only on the first `/add`;
  `/add` is refused with a "capacity" message once `max_users` (10000) users or
  `max_total_subs` (50000) subscriptions exist (`0` = unlimited). Env: `NOTIFY_MAX_USERS`,
  `NOTIFY_MAX_TOTAL_SUBS`, `NOTIFY_COMMANDS_PER_MINUTE`.

## CSV / accounting export

A per-address statement for bookkeeping (freelancers, small teams, accountants), streamed straight
from the Postgres `trc20_transfers` table – constant memory, any size:

```bash
# HTTP (same bearer token as the admin API; needs the postgres sink)
curl -H "Authorization: Bearer $API_TOKEN" -o october.csv \
  "http://localhost:9090/v1/export.csv?address=TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv&from=2026-10-01&to=2026-10-31"

# CLI (reads DATABASE_URL / config; no running listener needed)
./bin/tron-usdt-listener export -address 0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed \
  -from 2026-10-01 -to 2026-10-31 -o october.csv
```

| Parameter | |
|---|---|
| `address` | required; TRON `T…`, TON `EQ…`/`UQ…`/`0:<hex>` or Ethereum `0x…` |
| `chain` | `tron` / `ton` / `eth`; detected from the address when omitted |
| `from` | inclusive, `YYYY-MM-DD` or RFC 3339 (UTC) |
| `to` | `YYYY-MM-DD` includes that whole day; an RFC 3339 timestamp is exclusive |

```csv
time_utc,chain,tx_id,log_index,from,to,direction,amount,symbol,memo,explorer_url
2026-10-01T12:00:00Z,eth,0x3971…500e,5,0xfB69…d359,0x5aAe…eAed,in,1250.5,USDT,,https://etherscan.io/tx/0x3971…500e
```

- `direction` is relative to `address`: `in`, `out` or `self`. Amounts are exact decimals (no float
  rounding). Addresses use the explorer form (TON `UQ…`, Ethereum EIP-55).
- RFC 4180 quoting (commas, quotes, newlines in memos). Memos starting with `= + - @` are prefixed
  with `'` to prevent spreadsheet formula injection.
- **Only stored transfers are exported**: the Postgres sink stores what the filter matched, so add the
  address to the watch list (static config or `/v1/addresses`) before you need its statement, and use
  `-from/-to` backfill for history. Migration `0005` adds `(chain, address, block_time)` indexes.
- `401` without/with a wrong token, `400` for bad parameters, `503` when the postgres sink is off,
  `404` when `API_TOKEN` is not set.

## TON (USD₮ jetton)

Enable with `CHAINS=ton` (TON only) or `CHAINS=tron,ton` (both, concurrently):

```bash
CHAINS=tron,ton TONCENTER_API_KEY=… TON_RPS=9 \
TON_WATCH_ADDRESSES=UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg ./bin/tron-usdt-listener
# backfill a TON range (masterchain seqnos) when several chains are enabled:
./bin/tron-usdt-listener -chain ton -from 98632900 -to 98632905
```

How it works (read-only, [toncenter v3 / TON Index](https://toncenter.com/api/v3/)):

- **Block number = masterchain seqno.** Head is `GET /api/v3/masterchainInfo` (`last.seqno`), the
  safe head is `head − ton.confirmations`. TON masterchain blocks are final once committed, the
  small margin only covers indexer lag.
- For each masterchain block: `GET /api/v3/masterchainBlockShards?seqno=N` returns the shard
  blocks it committed with their `[start_lt, end_lt]`; then
  `GET /api/v3/jetton/transfers?jetton_master=…&start_lt=…&end_lt=…&sort=asc` (paged, 1000/page).
  A transfer belongs to block `N` only if its transaction (on the sender's jetton wallet) is in one
  of those shards *and* its logical time is inside that shard block's lt range, so every transfer is
  attributed to exactly one masterchain block even though lt ranges of different shards overlap.
  Aborted transactions and other jettons are skipped. A block the indexer does not have yet
  (`404`) is retried, never skipped.
- **Addresses** are normalised to the raw form `0:<64 lowercase hex>` (owner wallets, not jetton
  wallets) for matching, storage and output; config and API accept raw and user-friendly
  (`EQ…`/`UQ…`, bounceable or not, base64url or std, checksum verified). Telegram shows the
  wallet-style `UQ…` form.
- **Amounts**: USD₮ on TON has 6 decimals, like TRON (`11250000` → `11.25`).
- **Dedupe key / id**: `ton:<tx hash hex>:0` (TRON keys stay `<tx_id>:<log_index>`). The text
  comment of the transfer (often an invoice / order id) is exposed as `memo`.
- **Rate limits**: 2 requests per masterchain block and the masterchain produces ~2.4 blocks/s, so
  keyless toncenter (≈1 req/s) **cannot keep up with mainnet head** (fine for backfills of a few
  blocks / testing). Get a free key from [@tonapibot](https://t.me/tonapibot) (10 rps) and set
  `TONCENTER_API_KEY` + `TON_RPS=9`, or point `TON_API_URL` at your own
  [ton-indexer](https://github.com/toncenter/ton-indexer). `429` / `5xx` are retried with backoff and
  `Retry-After`.

**Live test with a key (2026-10-13):** with a free toncenter key and `TON_RPS=9`, a 90 s mainnet run
processed 204 masterchain blocks, stayed 3 blocks behind head (the confirmation margin) and logged 0 errors
(no 401 / 429).

Example TON stdout line (real mainnet transfer, masterchain block 98632901):

```json
{"msg":"transfer","id":"ton:540a39b8299cc5d0239eefa2be4268445a262b897a6cd970f1fb2ac4e70aef19:0","chain":"ton",
 "tx_id":"540a39b8299cc5d0239eefa2be4268445a262b897a6cd970f1fb2ac4e70aef19","log_index":0,"block":98632901,
 "block_time":"2026-10-12T21:29:58Z","token":"USDT",
 "from":"0:0c8c0fb575238aaa844a89b36eb7546359e4e40297bcbe574c05611d884f36f1",
 "to":"0:852443f8599fe6a5da34fe43049ac4e0beb3071bb2bfb56635ea9421287c283a",
 "amount":"11.25","amount_raw":"11250000","reasons":"all","memo":"750 Telegram Stars \n\nRef#zopsXyorO"}
```

## Ethereum (ERC-20 USDT)

Enable with `CHAINS=eth` or together with the others, e.g. `CHAINS=tron,ton,eth`:

```bash
CHAINS=tron,eth ETH_RPC_URL=https://mainnet.infura.io/v3/<key> \
ETH_WATCH_ADDRESSES=0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed ./bin/tron-usdt-listener
```

How it works (read-only, standard JSON-RPC – any node or provider works):

- **Head** = `eth_blockNumber`; safe head = `head − eth.confirmations` (default 12 blocks ≈ 2.5 min).
- **Transfers** = `eth_getLogs` filtered by the USDT contract and topic0
  `keccak256("Transfer(address,address,uint256)")` =
  `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`, fetched in chunks of
  `eth.log_chunk` blocks. When a provider rejects a range (too many results / block-range limit) the
  chunk is split in half automatically down to single blocks. `429`, `Retry-After`, `-32005` and
  similar rate-limit errors are retried with backoff.
- Block timestamps come from the logs' `blockTimestamp` field (geth ≥ 1.14 / most providers) or,
  when absent, from `eth_getBlockByNumber` for blocks that contain transfers.
- Addresses are stored in canonical **lowercase** `0x…` form; the config and API accept lowercase,
  uppercase or a valid **EIP-55** checksum (mixed case with a wrong checksum is rejected). Telegram
  alerts show the checksummed form. `tx_id` is the `0x`-prefixed hash; idempotency id
  `eth:<tx_hash>:<log_index>`.
- The free public node (`ethereum-rpc.publicnode.com`) is fine for following the head (verified:
  ~60 transfers/block, stays 12 blocks behind head) but refuses old ("archive") ranges – use your own
  node or a provider URL for historical backfills.

## Output formats

### stdout (one line per transfer)

```json
{"time":"2026-10-06T18:16:54.955-04:00","level":"INFO","msg":"transfer",
 "id":"4cc4b76b25b84691f1ff248928d1fff957e6b7c163118ba92079ea60e57464c7:0",
 "tx_id":"4cc4b76b25b84691f1ff248928d1fff957e6b7c163118ba92079ea60e57464c7","log_index":0,
 "block":86884075,"block_time":"2026-10-06T22:15:51Z","token":"USDT",
 "from":"TGkPWyGnXoTEsY6Wn95J9rru8fNTHR2mBs","to":"TLawgrKkiT3z4Z6993KLoLCQRhrxMvyNrP",
 "amount":"3069749.967994","amount_raw":"3069749967994","reasons":"all"}
```

(A real mainnet transfer captured during testing –
[view on Tronscan](https://tronscan.org/#/transaction/4cc4b76b25b84691f1ff248928d1fff957e6b7c163118ba92079ea60e57464c7).)

### Webhook

One `POST` per block with matched transfers:

```http
POST /your/endpoint
Content-Type: application/json
X-Webhook-Event: trc20.transfers
X-Webhook-Delivery: 86884075-1f0c2a9d4e5b6c7d        # deterministic → use for idempotency
X-Signature-Timestamp: 1791324951
X-Signature-256: sha256=<hex HMAC-SHA256(secret, "<timestamp>.<raw body>")>

{"event":"trc20.transfers","delivery_id":"86884075-1f0c2a9d4e5b6c7d","block_number":86884075,
 "block_time":"2026-10-06T22:15:51Z","transfers":[{"id":"4cc4…64c7:0","tx_id":"4cc4…64c7",
 "log_index":0,"block_number":86884075,"block_time":"2026-10-06T22:15:51Z",
 "contract":"TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t","symbol":"USDT","decimals":6,
 "from":"TGkP…2mBs","to":"TLaw…yNrP","amount":"3069749.967994","amount_raw":"3069749967994",
 "reasons":["large_transfer"],"chain":"tron"}]}
```

Every transfer carries `"chain":"tron"`, `"chain":"ton"` or `"chain":"eth"` (TON transfers may also carry `"memo"`).
A webhook batch always contains one block of one chain; TON `block_number` is the masterchain seqno.

`5xx`, `408`, `429` (with `Retry-After`) and network errors are retried with backoff; other `4xx` are
permanent. Verify signatures in Go with the bundled package:

```go
import "github.com/co-codin/USDT-Tracker/pkg/webhooksig"

err := webhooksig.Verify([]byte(secret),
    r.Header.Get(webhooksig.HeaderTimestamp), r.Header.Get(webhooksig.HeaderSignature),
    body, 5*time.Minute, time.Now())
```

### Telegram alert (example)

```
🐋 Large USDT transfer
💵 3,069,749.96 USDT
From: TGkPWyGnXoTEsY6Wn95J9rru8fNTHR2mBs
To:   TLawgrKkiT3z4Z6993KLoLCQRhrxMvyNrP
Block: 86884075 · 2026-10-06 22:15:51 UTC
Tx: 4cc4b76b…7464c7   (links to Tronscan)
```

TON alerts link to [Tonviewer](https://tonviewer.com) and show the `Memo` when present.
Watched addresses get `📥 Incoming USDT` / `📤 Outgoing USDT` titles. Messages are rate limited
(~1/s per chat), `429 retry_after` is honoured, and at most `max_messages_per_block` alerts are sent
per block (the rest are summarised). Create a bot with [@BotFather](https://t.me/BotFather), add it to
your chat and set `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`.

### Large-transfer channel (whale alerts)

A separate, watch-list-independent rule that posts **every transfer ≥ threshold (default 100,000 USDT)
on every enabled chain** to a Telegram channel – a ready-made "USDT whale alerts" channel to grow an
audience and show off the project:

```bash
LARGE_ALERTS_ENABLED=true LARGE_ALERTS_CHAT_ID=@usdt_whales \
LARGE_ALERTS_BOT_TOKEN=<bot token, or reuse TELEGRAM_BOT_TOKEN> LARGE_ALERTS_THRESHOLD=100000 \
LARGE_ALERTS_FOOTER='via <a href="https://github.com/co-codin/USDT-Tracker">USDT-Tracker</a>' \
CHAINS=tron,ton,eth ./bin/tron-usdt-listener
```

```
🐋 1,250,000 USDT on Ethereum
From: 0x5aAe…eAed
To: 0xfB69…d359
Etherscan · 2026-10-12 21:25 UTC
```

- Fed with *all* decoded transfers of each confirmed block (before the filter), so it works even with
  an empty watch list or a payment-only filter. Not triggered by `-from/-to` backfills.
- Links: tronscan (TRON), tonviewer (TON, wallet-style `UQ…` addresses + memo), etherscan (Ethereum,
  EIP-55 addresses).
- **Telegram limits:** at most one message per `interval` (default 3 s = 20/min, Telegram's per-channel
  limit); a burst is batched into one message listing up to `max_per_message` transfers (largest first).
  `429 retry_after` is honoured. Pending alerts live in a bounded queue (`queue_size`, oldest dropped);
  restarts don't re-alert recently seen transfers. Best-effort – it never blocks the listeners.
- Add the bot to the channel as an **administrator** (post messages). Metric:
  `tron_listener_large_transfer_alerts_total{status="sent|failed|dropped"}`.

### PostgreSQL

Migrations are embedded and applied automatically (guarded by an advisory lock).

```sql
trc20_transfers(id, tx_id, log_index, block_number, block_time, contract, symbol,
                from_address, to_address, amount_raw NUMERIC(78,0), amount NUMERIC(78,18),
                reasons TEXT[], created_at, UNIQUE (tx_id, log_index))
listener_cursor(name PRIMARY KEY, last_block, updated_at)
watch_addresses(address TEXT PRIMARY KEY  -- base58 T… or TON raw 0:<hex>, label TEXT NULL,
                direction TEXT DEFAULT 'both' CHECK (both|incoming|outgoing),
                enabled BOOLEAN DEFAULT true, created_at TIMESTAMPTZ)   -- migration 0002
-- migration 0003 (backward compatible, existing rows become 'tron'):
--   trc20_transfers  + chain TEXT NOT NULL DEFAULT 'tron', + memo TEXT, index (chain, block_number)
--   watch_addresses  + chain TEXT NOT NULL DEFAULT 'tron', address check per chain
-- migration 0004: watch_addresses accepts chain 'eth' with lowercase 0x<40 hex>
```

The table keeps its historical name `trc20_transfers` for compatibility; filter TON rows with
`WHERE chain = 'ton'`. The TRON cursor row is `<cursor_name>` (unchanged), the TON one `<cursor_name>:ton`, the Ethereum one `<cursor_name>:eth`
(Ethereum rows: `chain = 'eth'`, `tx_id` = `0x…` hash, lowercase addresses).

## Metrics & health

`GET /healthz` → `200 {"ok":true,"status":{"head":…,"safe_head":…,"cursor":…,"lag":…}}`, or `503`
when no progress was made for `health_stale_after` (node unreachable, sink stuck, DB down).
With several chains the status is per chain: `{"ok":true,"status":{"chains":{"tron":{…},"ton":{…}}}}`
and `ok` is false if any chain is stalled.
The Docker image uses `listener -healthcheck` (no curl in distroless).

Selected metrics (`tron_listener_*`, name kept for compatibility; per-chain ones carry a `chain`
label, `tron`, `ton` or `eth`): `head_block`, `safe_head_block`, `cursor_block`, `lag_blocks`,
`blocks_processed_total`, `transfers_decoded_total`, `transfers_matched_total{chain,reason}`,
`rpc_requests_total{chain,endpoint,status}`, `rpc_request_duration_seconds`, `rpc_retries_total{chain,endpoint,reason}`,
`sink_deliveries_total{sink,status}`, `sink_delivery_duration_seconds`, `dispatch_retries_total`,
`decode_errors_total`, `block_fetch_errors_total`, `large_transfer_alerts_total{status}`.

## Testing

```bash
make test            # unit + end-to-end tests, no network
make test-race       # same with the race detector
make cover           # per-package coverage + coverage.html
make lint            # golangci-lint (config in .golangci.yml)
# Postgres tests (skipped unless TEST_DATABASE_URL is set):
docker run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=test postgres:17-alpine
make test-integration TEST_DATABASE_URL='postgres://postgres:test@localhost:55432/postgres?sslmode=disable'
```

- **Unit tests**: base58check / hex ↔ `T…` addresses, Transfer decoding against a real mainnet block
  fixture (values cross-checked with Tronscan), 6-decimal amount formatting, filters, cursor &
  dedupe, retry/backoff, webhook signing, Telegram formatting, config/env parsing, admin API
  (auth, validation, CRUD, 503 without Postgres) and the runtime filter merge (an address added to an
  in-memory store is matched by the running filter without a restart).
- **End-to-end tests** (`internal/app`) run the real wiring against an in-process mock TRON node
  (`internal/chain/tron/trontest`) plus mock webhook and Telegram servers: ordered block-range processing,
  confirmations, 429/5xx retries, lagging-node empty responses, restart from cursor without gaps,
  crash-replay with stable idempotency ids, per-sink dedupe when one sink fails, filters, `/healthz`
  and `/metrics`, Postgres as sink + cursor, and adding/removing an address through the HTTP API
  while the listener runs (Postgres tests use a throw-away schema).
- **TON tests**: address parsing/formatting checked against real toncenter `address_book` pairs,
  shard prefix matching, the toncenter client (API key header, 429 + `Retry-After`, 5xx retries,
  404 = not indexed), the jetton source against a **real captured mainnet response** (masterchain
  block 98632901) and a synthetic mock index (`internal/chain/ton/tontest`) with two shards whose lt
  ranges overlap across masterchain blocks (every transfer exactly once, in the right block, with
  page sizes 1/2/1000), and e2e runs: TON only with faults and restart, TRON + TON concurrently with
  separate cursors, `-chain ton` backfill, and Postgres with a TON address added via the API.

## Design notes & limitations

- **Delivery semantics** are at-least-once. A crash *between* delivering a block and persisting the
  cursor replays that block on restart; use `id` (`tx_id:log_index`) / `X-Webhook-Delivery` for
  idempotency (Postgres does this automatically).
- `log_index` is the position of the log inside its transaction (that's what the TRON HTTP API
  exposes), so `tx_id + log_index` is unique.
- With `confirmations ≥ 19` blocks are solidified, so reorg handling is not needed; with lower values
  you trade safety for latency.
- One `gettransactioninfobyblocknum` request per block (≈80–300 KB). Without an API key keep
  `tron.rps` around 2–3 – plenty for live following (1 block / 3 s) and slow catch-up. For long
  backfills use an API key or your own node (`TRON_API_URL=http://your-node:8090`).
- TON: one `masterchainBlockShards` + ≥1 `jetton/transfers` request per masterchain block; the
  data comes from the toncenter index (a trusted third party, like TronGrid on TRON). Bounced
  jetton transfers (the recipient's wallet failing after the sender's wallet succeeded) are rare
  and not detected; use `memo` / your own reconciliation for payment-critical flows.
- Only the `Transfer` event is decoded (not `Approval`, mint/burn helpers like `Issue`/`Redeem`).
- The runtime watch list is loaded fully into memory on each reload (one indexed query every
  `reload_interval`); fine for tens of thousands of addresses. Adding an address does not backfill
  past blocks – use `-from/-to` if a payment may already have happened.

## 中文简介

**USDT-Tracker**（程序和目录名沿用 `tron-usdt-listener`）是一个用 Go 编写的 **只读** 多链 USDT 转账监听器，
支持 **TRON（TRC-20）、TON（USD₮ jetton）和以太坊（ERC-20）**，可单独或同时运行，每条链独立游标。
功能包括：地址监控（转入/转出）和大额阈值过滤；输出到 stdout JSON、HMAC-SHA256 签名 Webhook、Telegram
和 PostgreSQL；超过 10 万 USDT 自动发到 Telegram 频道的大额提醒；按地址导出 CSV 流水；用户自助的
Telegram 收款通知机器人；运行时增删地址的管理 API（`/v1/addresses`）；按链设置确认数（TRON 20、TON 3、
以太坊 12）、去重、退避重试、Prometheus `/metrics` 与 `/healthz`；Docker 和 Helm 部署。不需要也不会接触
任何私钥，从不发送交易。快速开始：`make build && ./bin/tron-usdt-listener` 或 `docker compose up -d --build`。
商业支持（定制接入、维护、托管）见上文 Commercial support。

**TON 支持：** 设置 `CHAINS=ton`（只监听 TON）或 `CHAINS=tron,ton`（两条链同时运行，各自独立游标，
共用输出和过滤规则）即可监听 TON 上官方 USD₮ jetton（`EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs`，
6 位小数）的转账。数据来自 toncenter v3 公共索引 API，同样只读；区块号是 masterchain seqno，
地址统一为 raw 形式 `0:<hex>`（配置和 API 也接受 `EQ…`/`UQ…`），转账备注会放在 `memo` 字段。
TON 地址写在 `ton.watch_addresses` / `TON_WATCH_ADDRESSES`，也可以通过 `/v1/addresses` 动态添加
（自动识别链，或传 `"chain":"ton"`）。不带 API key 的 toncenter 每秒只能请求约 1 次，跟不上主网出块，
生产环境请在 @tonapibot 申请免费 key，设置 `TONCENTER_API_KEY` 和 `TON_RPS=9`。

**运行时地址管理（v1 API）：** 启用 Postgres sink 并设置 `API_TOKEN` 后，可以通过
`GET/POST/DELETE /v1/addresses`（`Authorization: Bearer <token>`）在运行中增删监听地址，无需重启；
地址存在 `watch_addresses` 表中，与配置文件里的静态地址合并生效。未设置 token 时 API 关闭；
未启用 Postgres 时 API 返回 503，静态配置照常工作。Kubernetes 部署见 `deploy/helm/usdt-tracker`
（Helm chart，可内置 PostgreSQL 或连接外部数据库，token 通过 Secret 注入）。

**以太坊支持：** 设置 `CHAINS=eth`（或 `tron,ton,eth`）即可监听以太坊上的 USDT（ERC-20，合约
`0xdAC17F958D2ee523a2206206994597C13D831ec7`，6 位小数）。通过标准 JSON-RPC 的 `eth_blockNumber` 和
`eth_getLogs` 只读获取，按区块范围分段请求，节点拒绝时自动减半，429 自动重试。RPC 地址用
`ETH_RPC_URL` 配置（常带服务商 key，按密钥处理，日志里只显示域名），默认是免费的 publicnode
公共节点。地址统一存成小写 `0x…`，配置和 API 接受小写或正确的 EIP-55 校验格式。

**大额转账提醒频道：** 设置 `LARGE_ALERTS_ENABLED=true`、`LARGE_ALERTS_CHAT_ID=@你的频道` 后，所有已启用
链上 ≥ 阈值（默认 100000 USDT，`LARGE_ALERTS_THRESHOLD`）的转账都会自动发到 Telegram 频道，和监听地址
无关，附 tronscan / tonviewer / etherscan 链接。按 Telegram 限制控速（默认每 3 秒最多一条，突发时
合并成一条），429 自动等待重试，队列有上限，不会拖慢监听。可用 `LARGE_ALERTS_FOOTER` 加项目链接做曝光。
机器人需要是频道管理员。

**CSV / 会计导出：** `GET /v1/export.csv?address=&chain=&from=&to=`（和管理 API 同一个 Bearer
`API_TOKEN`）或命令行 `tron-usdt-listener export -address … -from 2026-10-01 -to 2026-10-31 -o out.csv`，
从 Postgres 流式导出某个地址的 USDT 流水：时间(UTC)、链、交易哈希、from/to、相对该地址的方向
(in/out/self)、金额、备注、浏览器链接。标准 CSV 转义并防止表格公式注入。只能导出已入库的转账，
所以要先把地址加入监听列表。

**Telegram 收款通知机器人：** 设置 `NOTIFY_BOT_ENABLED=true` 和 `TELEGRAM_NOTIFY_BOT_TOKEN`（需要启用
Postgres），用户在机器人里发 `/add <地址>`（自动识别 TRON / TON / 以太坊），之后每笔转入、转出的 USDT
都会私信通知；还有 `/list`、`/remove`、`/plan`、`/help`。免费版默认最多 3 个地址（`NOTIFY_FREE_LIMIT`），
`notify_users.plan = 'premium'` 时上限为 `NOTIFY_PREMIUM_LIMIT`（默认 50），这是以后接付费的入口，
目前没有付款功能。机器人在监听进程里用长轮询运行，不需要公网地址；小于 0.01 的转账（投毒垃圾转账）会被忽略。

## Кратко на русском

**USDT-Tracker** (бинарник и папка исторически называются `tron-usdt-listener`) — **read-only** сервис на Go
для отслеживания переводов USDT в сетях **TRON (TRC-20), TON (жеттон USD₮) и Ethereum (ERC-20)** — по
отдельности или одновременно, у каждой сети свой курсор. Возможности: фильтр по адресам (входящие/исходящие)
и порогу; вывод в stdout (JSON), webhook с подписью HMAC-SHA256, Telegram и PostgreSQL; канал крупных
переводов (≥ 100 000 USDT) в Telegram; CSV-выгрузка по адресу; бот уведомлений в Telegram для пользователей;
admin API для адресов без перезапуска (`/v1/addresses`); подтверждения по сетям (TRON 20, TON 3, Ethereum 12),
дедупликация, ретраи, метрики Prometheus и `/healthz`; Docker и Helm. Приватные ключи не используются,
транзакции не отправляются. Быстрый старт: `make build && ./bin/tron-usdt-listener` или
`docker compose up -d --build`. Коммерческая поддержка — см. раздел Commercial support.

**TON:** `CHAINS=ton` (только TON) или `CHAINS=tron,ton` (обе сети параллельно, у каждой свой курсор,
синки и правила фильтра общие) включают отслеживание официального жеттона USD₮ в TON
(`EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs`, 6 знаков) через публичный read-only API toncenter v3.
Номер блока — seqno мастерчейна, адреса приводятся к raw-виду `0:<hex>` (в конфиге и API можно
`EQ…`/`UQ…`), комментарий перевода попадает в поле `memo`. TON-адреса задаются в `ton.watch_addresses`
/ `TON_WATCH_ADDRESSES` или через `/v1/addresses` (сеть определяется автоматически или `"chain":"ton"`).
Без API-ключа toncenter даёт ~1 запрос/с и не успевает за мейннетом — получите бесплатный ключ в
@tonapibot и задайте `TONCENTER_API_KEY` и `TON_RPS=9`.

**Ethereum:** `CHAINS=eth` (или `tron,ton,eth`) включает отслеживание USDT ERC-20
(`0xdAC17F958D2ee523a2206206994597C13D831ec7`, 6 знаков) через стандартный JSON-RPC (`eth_blockNumber`,
`eth_getLogs` по диапазонам блоков с автоматическим делением, ретраи при 429). Адрес RPC — `ETH_RPC_URL`
(считается секретом, в логах только хост); по умолчанию бесплатный узел publicnode. Адреса хранятся
в нижнем регистре `0x…`, на входе принимается нижний регистр или корректная контрольная сумма EIP-55.

**Канал крупных переводов:** `LARGE_ALERTS_ENABLED=true` и `LARGE_ALERTS_CHAT_ID=@канал` — каждый
перевод ≥ порога (по умолчанию 100000 USDT, `LARGE_ALERTS_THRESHOLD`) в любой включённой сети
публикуется в Telegram-канал независимо от списка адресов, со ссылками на tronscan / tonviewer / etherscan.
Скорость ограничена под лимиты Telegram (по умолчанию не чаще раза в 3 с, всплески объединяются в одно
сообщение), 429 обрабатывается, очередь ограничена. Бот должен быть администратором канала.

**CSV-выгрузка для бухгалтерии:** `GET /v1/export.csv?address=&chain=&from=&to=` (тот же Bearer
`API_TOKEN`) или CLI `tron-usdt-listener export -address … -from … -to … -o out.csv` — потоковая выписка
по адресу из Postgres: время (UTC), сеть, хеш, from/to, направление (in/out/self), сумма, memo, ссылка
на эксплорер. Экранирование по RFC 4180 и защита от формул в таблицах. Выгружаются только сохранённые
переводы — адрес нужно заранее добавить в список отслеживания.

**Бот уведомлений в Telegram:** `NOTIFY_BOT_ENABLED=true` и `TELEGRAM_NOTIFY_BOT_TOKEN` (нужен Postgres) —
пользователь отправляет `/add <адрес>` (сеть TRON / TON / Ethereum определяется сама) и получает личное
сообщение о каждом входящем и исходящем переводе USDT; также `/list`, `/remove`, `/plan`, `/help`.
Бесплатно до 3 адресов (`NOTIFY_FREE_LIMIT`), план `premium` в `notify_users.plan` — до
`NOTIFY_PREMIUM_LIMIT` (50); это задел под платную версию, оплаты в коде нет. Работает в процессе
листенера через long polling, публичный URL не нужен; переводы меньше 0.01 (спам-«отравление») пропускаются.

**Адреса во время работы (API v1):** при включённом Postgres-синке и заданном `API_TOKEN` адреса
можно добавлять и удалять без перезапуска через `GET/POST/DELETE /v1/addresses`
(`Authorization: Bearer <token>`); они хранятся в таблице `watch_addresses` и объединяются со
статическим списком из конфига. Без токена API выключен; без Postgres API отвечает 503, а
статический список работает как раньше. Для Kubernetes есть Helm-чарт `deploy/helm/usdt-tracker`
(встроенный или внешний PostgreSQL, токен через Secret).

## Commercial support

USDT-Tracker is **open core**: the code here is free under MIT. If you need help, I offer custom
integration, ongoing maintenance / monitoring and a hosted setup. Contact: GitHub
[@co-codin](https://github.com/co-codin) or Telegram [@zzwwyy112233](https://t.me/zzwwyy112233).
Payments are accepted in USDT.

## License

[MIT](LICENSE) © Tsui Etsin
