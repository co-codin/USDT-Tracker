# Changelog

All notable changes are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/) (0.x: minor versions may break).

## [Unreleased]

### Added

- `chains:` as a mapping of per-chain blocks (`chains.tron`, `chains.ton`, `chains.eth`, with
  `enabled`); the old list + top-level blocks keep working with a deprecation warning. Chains are
  registered in one registry used by config validation and wiring.
- `DATABASE_PASSWORD` / `sinks.postgres.password`; Helm `externalDatabase.passwordSecret.{name,key}`,
  bundled Postgres password injected from the subchart Secret (no longer embedded in the URL).
- Optional Helm PodDisruptionBudget (`podDisruptionBudget.enabled`).
- Postgres outbox (`deliveries` table, migration 0007) for notify-bot messages and large-transfer
  alerts: at-least-once with per-(chat, transfer) dedupe, 30-day retention.
- Real end-to-end suite `test/e2e` (`make e2e`, CI job), `make helm-e2e` (kubeconform) and
  `make smoke` (live read-only mainnet check).

### Changed

- All HTTP clients (TRON, TON, Ethereum, Telegram, webhook) share one retry / rate-limit / Retry-After
  implementation (`internal/httpx`). TronGrid 403 frequency bans without a `Retry-After` header wait 30 s.
- Listener taps (notify bot, alerts) run after sinks and before the cursor is saved, and are retried.
- `make test-integration` runs every package against Postgres.

### Fixed

- Concurrent migrations could deadlock (`pg_advisory_lock` + `CREATE INDEX CONCURRENTLY`); the
  migrator now polls `pg_try_advisory_lock`.
- Notify-bot messages could be lost on a crash (were at-most-once).
- `config.example.yaml` (also the Docker image's default config) now uses the `chains.<name>` shape,
  so the image no longer logs deprecation warnings at startup.
- A client closing the connection during a CSV export is logged at info, not as an error.

## [0.2.0] - 2026-10-13

### ⚠ Breaking changes

- **Binary and command renamed** to `usdt-tracker` (`cmd/usdt-tracker`, `bin/usdt-tracker`, image
  entrypoint `/usdt-tracker`, config path in the image `/etc/usdt-tracker/config.yaml`).
- **Prometheus metrics prefix** changed from `tron_listener_` to `usdt_tracker_`
  (e.g. `usdt_tracker_last_processed_timestamp_seconds`). Update dashboards and alert rules.
- **HTTP server listens on `127.0.0.1:9090` by default** (was all interfaces). The Docker image and
  Helm chart set `HTTP_ADDR=0.0.0.0:9090` explicitly.
- `/v1` API errors are now JSON `{"error","code"}`; `GET /v1/addresses` is paginated
  (`limit`, `cursor`, `next_cursor`).
- Notify bot only serves private chats.

### Added

- Release pipeline: GitHub releases with binaries (goreleaser) and multi-arch image
  `ghcr.io/co-codin/usdt-tracker` (linux/amd64, linux/arm64); version via `-ldflags`.
- `/livez` and `/readyz` endpoints; `/healthz` details only with `HTTP_HEALTH_DETAILS`; Helm startup probe.
- `ETH_HEAD_TAG` (`latest` | `safe` | `finalized`) and reorg detection by parent-hash continuity.
- `TRON_VERIFY_TX_COUNT` cross-checks each block's transaction-info count.
- Notify bot: English / 中文 / Русский replies, abuse limits (`NOTIFY_MAX_USERS`,
  `NOTIFY_MAX_TOTAL_SUBS`, `NOTIFY_COMMANDS_PER_MINUTE`), `NOTIFY_PREMIUM_CONTACT`.
- CSV export: `bom=1` / `-bom` for Excel, JSON errors.
- Helm: `secrets.existingSecretHasDatabaseUrl`, validation that the notify bot has a database.
- SECURITY, CONTRIBUTING, CODE_OF_CONDUCT, issue/PR templates, Dependabot, golangci-lint and
  govulncheck in CI.

### Fixed

- Ethereum: logs could be skipped when a load-balanced RPC node lagged behind the head.
- TON: transfers beyond the page limit in busy blocks were dropped (now paged by `lt`).
- Webhook URLs (which may carry tokens) are redacted from logs and errors; HTTP clients refuse
  cross-host redirects.
- Listener taps (alerts, notify bot) run only after the cursor is saved, so a failed save no longer
  duplicates notifications.
- HTTP server timeouts; graceful shutdown flushes pending alerts.
- Migration 0005 builds its indexes `CONCURRENTLY` and rebuilds invalid ones.

### Security

- Go toolchain 1.26.9 and updated dependencies (govulncheck clean); base images pinned by digest.

## Before 0.2.0 (untagged)

- Initial release: TRON TRC-20, TON jetton and Ethereum ERC-20 USDT listener with stdout, webhook,
  Telegram and PostgreSQL sinks, watch-list API, CSV export, notify bot and Helm chart.

[Unreleased]: https://github.com/co-codin/USDT-Tracker/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/co-codin/USDT-Tracker/releases/tag/v0.2.0
