# syntax=docker/dockerfile:1

# ---- build ----
ARG GO_VERSION=1.27
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/listener ./cmd/listener \
 && mkdir -p /out/data

# ---- runtime ----
# distroless/static: no shell, CA certificates included, runs as nonroot (65532).
FROM gcr.io/distroless/static-debian12:nonroot
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
