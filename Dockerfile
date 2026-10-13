# syntax=docker/dockerfile:1

# Base images are pinned by digest for reproducible, tamper-evident builds;
# Dependabot (.github/dependabot.yml) proposes updates. Keep the Go version in
# line with the toolchain directive in go.mod.
# ---- build ----
FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
# Cross-compiles on the build host (no QEMU) for multi-arch images.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/usdt-tracker ./cmd/usdt-tracker \
 && mkdir -p /out/data

# ---- runtime ----
# distroless/static: no shell, CA certificates included, runs as nonroot (65532).
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/usdt-tracker /usdt-tracker
COPY --from=build --chown=65532:65532 /out/data /data
COPY config.example.yaml /etc/usdt-tracker/config.yaml
# The binary defaults to 127.0.0.1:9090; inside a container it must listen on
# all interfaces (publish the port on 127.0.0.1 or behind a TLS proxy).
ENV CONFIG_FILE=/etc/usdt-tracker/config.yaml \
    CURSOR_FILE=/data/cursor.json \
    HTTP_ADDR=0.0.0.0:9090
WORKDIR /data
EXPOSE 9090
USER nonroot:nonroot
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s CMD ["/usdt-tracker", "-healthcheck"]
ENTRYPOINT ["/usdt-tracker"]
