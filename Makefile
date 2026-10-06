BINARY   := bin/tron-usdt-listener
PKG      := ./cmd/listener
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(VERSION)

# Load .env (if present) for `make run`.
ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: help build run backfill test test-race cover test-integration vet fmt lint tidy docker-build up down logs clean

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build the binary into bin/
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) $(PKG)

run: build ## Run locally (uses config.yaml if present, else defaults + env)
	$(BINARY)

backfill: build ## Re-process a block range once: make backfill FROM=86883900 TO=86883910
	$(BINARY) -from $(FROM) -to $(TO)

test: ## Unit tests (no network)
	go test ./...

test-race: ## Unit tests with the race detector
	go test -race ./...

cover: ## Tests with per-package coverage + HTML report (coverage.html)
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
	go tool cover -html=coverage.out -o coverage.html

test-integration: ## Postgres tests: make test-integration TEST_DATABASE_URL=postgres://...
	TEST_DATABASE_URL=$(TEST_DATABASE_URL) go test -race -count=1 -run 'Integration|Postgres' -v ./internal/sink/postgres/ ./internal/app/

vet: ## go vet
	go vet ./...

fmt: ## gofmt all files
	gofmt -s -w .

lint: vet ## golangci-lint if installed, else vet + gofmt check
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run ./...; \
	else echo "golangci-lint not found, running gofmt check"; test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1); fi

tidy: ## go mod tidy
	go mod tidy

docker-build: ## Build the Docker image
	docker build --build-arg VERSION=$(VERSION) -t tron-usdt-listener:$(VERSION) .

up: ## docker compose up (listener + Postgres)
	docker compose up -d --build

down: ## docker compose down
	docker compose down

logs: ## Follow listener logs
	docker compose logs -f listener

clean: ## Remove build artefacts and local state
	rm -rf bin/ data/ coverage.out coverage.html
