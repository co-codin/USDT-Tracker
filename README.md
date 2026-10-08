# tron-usdt-listener

[![Go](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A **read-only** listener for **USDT (TRC-20) transfers on TRON**, written in Go.
It follows the chain block by block, decodes every `Transfer` event of the USDT contract
[`TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t`](https://tronscan.org/#/token20/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t),
filters them (watch list, large-transfer threshold) and fans them out to pluggable sinks:
**stdout JSON**, **signed webhooks**, **Telegram alerts** and **PostgreSQL**.

Typical uses: deposit detection for a payment flow, treasury / whale alerts, feeding an analytics
pipeline, compliance monitoring of a set of addresses.

> 🔒 **Read-only by design.** The listener only calls public query endpoints. It never asks for,
> stores or handles private keys and has no code path that signs or broadcasts transactions.

---

## Features

- **Block-range processing with a persisted cursor** – resumes exactly where it stopped, no gaps
  (file cursor by default, PostgreSQL when the Postgres sink is enabled).
- **Confirmations** – stays `N` blocks behind head (default 20 ≈ 60 s, i.e. solidified/irreversible on TRON).
- **At-least-once delivery, deduplicated** – the cursor only advances after every required sink
  accepted a block; per-sink dedupe by `tx_id + log_index` so a retried block never double-sends to
  sinks that already succeeded. Postgres has a unique key; webhooks carry stable idempotency ids.
- **Resilient TRON client** – client-side rate limiting, retries with exponential backoff + jitter,
  honours `429` / `Retry-After` and TronGrid's 403 "frequency limit" responses, verifies ambiguous
  empty responses (lagging nodes) instead of silently skipping blocks.
- **Filters** – watch list (incoming / outgoing / both), minimum amount for large-transfer alerts,
  `any` (watched OR large) / `all` (watched AND large) modes, or firehose mode (everything).
- **Runtime watch list + admin API** – add/remove deposit addresses over HTTP
  (`/v1/addresses`, bearer token) without a restart; stored in Postgres and merged with the static
  config list. See [Runtime watch list (admin API)](#runtime-watch-list-admin-api).
- **Sinks** – stdout (structured JSON via `slog`), webhook (HMAC-SHA256 signed, retries),
  Telegram bot, PostgreSQL (`pgx`, embedded migrations). Required vs best-effort per sink.
- **Ops** – Prometheus `/metrics`, `/healthz` (503 when stalled), graceful shutdown on
  SIGINT/SIGTERM (finishes the in-flight block), distroless Docker image (~19 MB), docker-compose.
- **Backfill mode** – `-from/-to` re-processes any historical range without touching the cursor.
- Works **without an API key** at a low request rate; set `TRON_PRO_API_KEY` for more headroom.
- Any TRC-20 token can be watched by changing `token.contract/decimals/symbol`.

## Architecture

```mermaid
flowchart LR
    subgraph TRON["TRON full-node HTTP API (TronGrid)"]
        H["/wallet/getblock<br/>(head, header only)"]
        I["/wallet/gettransactioninfobyblocknum<br/>(receipts + event logs)"]
        B["/wallet/getblockbynum<br/>(verify empty blocks)"]
    end

    subgraph L["tron-usdt-listener"]
        C["tron.Client<br/>rate limit · retries · backoff"]
        S["chain/tron/trc20.Source<br/>(implements chain.Source)"]
        D["trc20 decoder<br/>Transfer log → T-addresses, amount"]
        F["filter<br/>static + runtime watch list · min amount"]
        P["listener loop<br/>head − confirmations<br/>ordered delivery"]
        X["sink.Dispatcher<br/>per-sink dedupe (tx_id:log_index)"]
        K[("cursor<br/>file | Postgres")]
        M["/metrics · /healthz · /v1/addresses"]
    end

    H & I & B --> C --> S --> D --> P
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

Processing loop:

1. Poll head (`/wallet/getblock`, header only), compute `safe = head − confirmations`.
2. Fetch blocks `cursor+1 … min(safe, cursor+batch_size)` concurrently (one
   `gettransactioninfobyblocknum` call per block), but **deliver strictly in order**.
3. Decode `Transfer(address,address,uint256)` logs emitted by the token contract in **successful**
   transactions (reverted/failed ones are skipped; transfers made inside contract calls – DEX swaps,
   routers, multisends – are included).
4. Filter → dispatch to all sinks → persist cursor. If a required sink fails, the same block is
   retried with backoff; nothing moves forward until it succeeds.

Repository layout (chain-specific code lives under `internal/chain/<chain>`, so an Ethereum or
Solana backend can be added as `internal/chain/eth`, `internal/chain/sol` implementing `chain.Source`):

```
cmd/listener/                 main: flags, logging, signals, -healthcheck
internal/
  app/                        composition root (wires everything; used by main and e2e tests)
  chain/                      chain-agnostic Source interface + Block
    tron/                     TRON: base58check addresses, read-only HTTP client
      trc20/                  TRC-20 Transfer decoding + chain.Source implementation
      trontest/               in-process mock TRON node for integration tests
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
```

Recognised keys: `API_TOKEN`, `TRON_PRO_API_KEY`, `TELEGRAM_BOT_TOKEN`, `WEBHOOK_SECRET`,
`DATABASE_URL`. No token = admin API disabled. `values.yaml` only contains empty placeholders.

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
| `GET /v1/addresses` | `200 {"addresses":[…],"static_addresses":[…]}` | |
| `POST /v1/addresses` body `{"address":"T…","label":"order 42","direction":"incoming"}` (`label`, `direction` optional; direction `both` \| `incoming` \| `outgoing`, default `both`) | `201` + the stored row | `400` invalid address/JSON, `409` already watched |
| `DELETE /v1/addresses/{address}` | `204` | `400` invalid address, `404` not in the runtime list |

Every request needs `Authorization: Bearer <token>` (`401` otherwise). With an empty token the API is
**disabled** (`404` for every `/v1` path – it is never left open). With a token but the Postgres sink
disabled the API answers `503` with an explanation; the static config list keeps working as before.
Addresses must be base58 `T…` addresses with a valid checksum (hex is rejected by the API).

```bash
H="Authorization: Bearer $API_TOKEN"

# add a deposit address (incoming transfers only)
curl -s -X POST localhost:9090/v1/addresses -H "$H" -H 'Content-Type: application/json' \
  -d '{"address":"TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv","label":"order 1001","direction":"incoming"}'
# {"address":"TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv","label":"order 1001","direction":"incoming",
#  "enabled":true,"created_at":"2026-10-08T20:41:07.512Z"}

# list (runtime rows + read-only static config addresses)
curl -s localhost:9090/v1/addresses -H "$H" | jq .

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
 "reasons":["large_transfer"]}]}
```

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

Watched addresses get `📥 Incoming USDT` / `📤 Outgoing USDT` titles. Messages are rate limited
(~1/s per chat), `429 retry_after` is honoured, and at most `max_messages_per_block` alerts are sent
per block (the rest are summarised). Create a bot with [@BotFather](https://t.me/BotFather), add it to
your chat and set `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`.

### PostgreSQL

Migrations are embedded and applied automatically (guarded by an advisory lock).

```sql
trc20_transfers(id, tx_id, log_index, block_number, block_time, contract, symbol,
                from_address, to_address, amount_raw NUMERIC(78,0), amount NUMERIC(78,18),
                reasons TEXT[], created_at, UNIQUE (tx_id, log_index))
listener_cursor(name PRIMARY KEY, last_block, updated_at)
watch_addresses(address TEXT PRIMARY KEY  -- base58 T…, label TEXT NULL,
                direction TEXT DEFAULT 'both' CHECK (both|incoming|outgoing),
                enabled BOOLEAN DEFAULT true, created_at TIMESTAMPTZ)   -- migration 0002
```

## Metrics & health

`GET /healthz` → `200 {"ok":true,"status":{"head":…,"safe_head":…,"cursor":…,"lag":…}}`, or `503`
when no progress was made for `health_stale_after` (node unreachable, sink stuck, DB down).
The Docker image uses `listener -healthcheck` (no curl in distroless).

Selected metrics (`tron_listener_*`): `head_block`, `safe_head_block`, `cursor_block`, `lag_blocks`,
`blocks_processed_total`, `transfers_decoded_total`, `transfers_matched_total{reason}`,
`rpc_requests_total{endpoint,status}`, `rpc_request_duration_seconds`, `rpc_retries_total{endpoint,reason}`,
`sink_deliveries_total{sink,status}`, `sink_delivery_duration_seconds`, `dispatch_retries_total`,
`decode_errors_total`, `block_fetch_errors_total`.

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
- Only the `Transfer` event is decoded (not `Approval`, mint/burn helpers like `Issue`/`Redeem`).
- The runtime watch list is loaded fully into memory on each reload (one indexed query every
  `reload_interval`); fine for tens of thousands of addresses. Adding an address does not backfill
  past blocks – use `-from/-to` if a payment may already have happened.

## 中文简介

**tron-usdt-listener** 是一个用 Go 编写的 **只读** TRON 链 USDT（TRC-20）转账监听器。它按区块范围
扫描 USDT 合约的 `Transfer` 事件，把十六进制地址解码为 `T…` 地址、按 6 位小数换算金额，并支持
地址监控（转入/转出）和大额转账阈值过滤。结果可输出到 stdout JSON、带 HMAC-SHA256 签名的
Webhook、Telegram 机器人以及 PostgreSQL。游标持久化（文件或 Postgres）保证重启后无缝续扫，
支持确认数、按 `tx_id + log_index` 去重、指数退避重试和 429 限流处理，并提供 Prometheus
`/metrics` 与 `/healthz`。不需要也不会接触任何私钥，从不发送交易。快速开始：`make build && ./bin/tron-usdt-listener`
或 `docker compose up -d --build`。

**运行时地址管理（v1 API）：** 启用 Postgres sink 并设置 `API_TOKEN` 后，可以通过
`GET/POST/DELETE /v1/addresses`（`Authorization: Bearer <token>`）在运行中增删监听地址，无需重启；
地址存在 `watch_addresses` 表中，与配置文件里的静态地址合并生效。未设置 token 时 API 关闭；
未启用 Postgres 时 API 返回 503，静态配置照常工作。Kubernetes 部署见 `deploy/helm/usdt-tracker`
（Helm chart，可内置 PostgreSQL 或连接外部数据库，token 通过 Secret 注入）。

## Кратко на русском

**tron-usdt-listener** — **read-only** сервис на Go для отслеживания переводов USDT (TRC-20) в сети
TRON. Он обходит блоки по диапазонам, декодирует события `Transfer` контракта USDT (hex → `T…`-адреса,
сумма с 6 знаками), фильтрует по списку адресов (входящие/исходящие) и порогу крупных переводов и
отправляет результат в stdout (JSON), webhook с подписью HMAC-SHA256, Telegram-бота и PostgreSQL.
Курсор хранится в файле или Postgres — после перезапуска обработка продолжается без пропусков; есть
подтверждения (confirmations), дедупликация по `tx_id + log_index`, ретраи с экспоненциальной
задержкой и обработка 429, метрики Prometheus и `/healthz`. Приватные ключи не используются,
транзакции не отправляются. Быстрый старт: `make build && ./bin/tron-usdt-listener` или
`docker compose up -d --build`.

**Адреса во время работы (API v1):** при включённом Postgres-синке и заданном `API_TOKEN` адреса
можно добавлять и удалять без перезапуска через `GET/POST/DELETE /v1/addresses`
(`Authorization: Bearer <token>`); они хранятся в таблице `watch_addresses` и объединяются со
статическим списком из конфига. Без токена API выключен; без Postgres API отвечает 503, а
статический список работает как раньше. Для Kubernetes есть Helm-чарт `deploy/helm/usdt-tracker`
(встроенный или внешний PostgreSQL, токен через Secret).

## License

[MIT](LICENSE) © Tsui Etsin
