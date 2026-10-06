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
        S["source.TronSource"]
        D["decoder<br/>Transfer log → T-addresses, amount"]
        F["filter<br/>watch list · min amount"]
        P["listener loop<br/>head − confirmations<br/>ordered delivery"]
        X["sink.Dispatcher<br/>per-sink dedupe (tx_id:log_index)"]
        K[("cursor<br/>file | Postgres")]
        M["/metrics · /healthz"]
    end

    H & I & B --> C --> S --> D --> P
    P --> F --> X
    P <--> K
    X --> O["stdout JSON"]
    X --> W["Webhook<br/>HMAC-SHA256"]
    X --> T["Telegram bot"]
    X --> G[("PostgreSQL")]
    P -.-> M
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

```
cmd/listener/            main: flags, logging, signals, -healthcheck
internal/app/            composition root (wires everything; used by main and e2e tests)
internal/config/         YAML + env overrides + validation
internal/tron/           base58check addresses, read-only HTTP client
internal/tron/trontest/  in-process mock TRON node for integration tests
internal/decoder/        TRC-20 Transfer log decoding
internal/source/         Source interface + TRON implementation
internal/filter/         watch list / threshold matching
internal/listener/       block-range loop, confirmations, retries, status
internal/cursor/         cursor stores (file, memory) + dedupe set
internal/sink/           Sink interface, dispatcher, stdout, webhook, telegram
internal/sink/postgres/  pgx store (sink + cursor) with embedded migrations
internal/metrics/        Prometheus collectors
internal/server/         /metrics and /healthz
pkg/webhooksig/          sign/verify helpers you can import in webhook consumers
```

## Quick start

### Local (Go 1.26+)

```bash
git clone https://github.com/co-codin/tron-usdt-listener && cd tron-usdt-listener
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
| `http.addr` | `HTTP_ADDR` | `:9090` | `/metrics`, `/healthz`; empty disables |
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
import "github.com/co-codin/tron-usdt-listener/pkg/webhooksig"

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
  dedupe, retry/backoff, webhook signing, Telegram formatting, config/env parsing.
- **End-to-end tests** (`internal/app`) run the real wiring against an in-process mock TRON node
  (`internal/tron/trontest`) plus mock webhook and Telegram servers: ordered block-range processing,
  confirmations, 429/5xx retries, lagging-node empty responses, restart from cursor without gaps,
  crash-replay with stable idempotency ids, per-sink dedupe when one sink fails, filters, `/healthz`
  and `/metrics`, and Postgres as sink + cursor.

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

## 中文简介

**tron-usdt-listener** 是一个用 Go 编写的 **只读** TRON 链 USDT（TRC-20）转账监听器。它按区块范围
扫描 USDT 合约的 `Transfer` 事件，把十六进制地址解码为 `T…` 地址、按 6 位小数换算金额，并支持
地址监控（转入/转出）和大额转账阈值过滤。结果可输出到 stdout JSON、带 HMAC-SHA256 签名的
Webhook、Telegram 机器人以及 PostgreSQL。游标持久化（文件或 Postgres）保证重启后无缝续扫，
支持确认数、按 `tx_id + log_index` 去重、指数退避重试和 429 限流处理，并提供 Prometheus
`/metrics` 与 `/healthz`。不需要也不会接触任何私钥，从不发送交易。快速开始：`make build && ./bin/tron-usdt-listener`
或 `docker compose up -d --build`。

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

## License

[MIT](LICENSE) © Tsui Etsin
