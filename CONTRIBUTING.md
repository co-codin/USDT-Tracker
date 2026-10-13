# Contributing

Thanks for helping! Bug reports, docs fixes and pull requests are welcome.

## Development

Requirements: Go (version from `go.mod`), optionally Docker, PostgreSQL 17 and Helm.

```bash
make build          # bin/usdt-tracker
make test           # unit tests
TEST_DATABASE_URL=postgres://listener:listener@localhost:5432/tron_test?sslmode=disable make test-race
make lint           # golangci-lint (see .golangci.yml)
```

Before opening a PR:

- `gofmt`, `go vet ./...`, `golangci-lint run` and `go test -race ./...` pass (CI runs the
  same, plus `govulncheck` and `helm lint`).
- New behaviour has tests; tests must not need network access (use the fakes in
  `internal/chain/*test`).
- New config options are documented in `config.example.yaml`, `.env.example` and the README table.
- User-visible changes get a line in `CHANGELOG.md` under *Unreleased*.
- Never commit secrets, real API keys or `.env` files.

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
(`fix(eth): …`, `feat(notify): …`, `docs: …`).

Security issues: see [SECURITY.md](SECURITY.md) instead of opening an issue.
