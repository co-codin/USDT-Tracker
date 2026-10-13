# syntax=docker/dockerfile:1

# Base images are pinned by digest for reproducible, tamper-evident builds;
# Dependabot (.github/dependabot.yml) proposes updates. Keep the Go version in
# line with the toolchain directive in go.mod.
# ---- build ----
FROM golang:1.26.9-alpine@sha256:3082400e369fa24d5fc60bca20edab3f6d604e0c5a690ec66b295eff4dd87ade AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/listener ./cmd/listener \
 && mkdir -p /out/data

# ---- runtime ----
# distroless/static: no shell, CA certificates included, runs as nonroot (65532).
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/listener /listener
COPY --from=build --chown=65532:65532 /out/data /data
COPY config.example.yaml /etc/tron-usdt-listener/config.yaml
ENV CONFIG_FILE=/etc/tron-usdt-listener/config.yaml \
    CURSOR_FILE=/data/cursor.json
WORKDIR /data
EXPOSE 9090
USER nonroot:nonroot
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s CMD ["/listener", "-healthcheck"]
ENTRYPOINT ["/listener"]
