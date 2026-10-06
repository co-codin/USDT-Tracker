// Command listener watches TRC-20 (USDT by default) Transfer events on TRON
// and forwards matching transfers to the configured sinks.
//
// It is strictly read-only: it never handles private keys and never signs or
// broadcasts transactions.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/co-codin/tron-usdt-listener/internal/app"
	"github.com/co-codin/tron-usdt-listener/internal/config"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("tron-usdt-listener", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		configPath  = fs.String("config", defaultConfigPath(), "path to YAML config (env: CONFIG_FILE)")
		from        = fs.Int64("from", 0, "backfill: first block (inclusive); runs once and exits, cursor untouched")
		to          = fs.Int64("to", 0, "backfill: last block (inclusive, default = -from)")
		showVersion = fs.Bool("version", false, "print version and exit")
		healthcheck = fs.Bool("healthcheck", false, "query the local /healthz endpoint and exit 0/1 (for container health checks)")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, "tron-usdt-listener", version)
		return 0
	}
	if *to > 0 && *from == 0 {
		fmt.Fprintln(stderr, "-to requires -from")
		return 2
	}

	cfg, err := config.Load(*configPath, os.LookupEnv)
	if err != nil {
		fmt.Fprintln(stderr, "configuration error:\n"+err.Error())
		return 2
	}
	if *healthcheck {
		return probeHealth(cfg.HTTP.Addr, stderr)
	}
	log := newLogger(cfg.Log, stderr)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	exited := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			log.Info("shutdown signal received, finishing in-flight block (press Ctrl+C again to force)")
			stop() // restore default signal handling: a second signal kills the process
		case <-exited:
		}
	}()
	defer func() { close(exited); wg.Wait() }()

	a, err := app.New(ctx, cfg, log, app.Options{Version: version, Backfill: *from > 0, Stdout: stdout})
	if err != nil {
		log.Error("startup failed", "err", err)
		return 1
	}
	defer func() {
		if err := a.Close(); err != nil {
			log.Warn("closing sinks", "err", err)
		}
	}()
	if err := a.Run(ctx, *from, *to); err != nil {
		log.Error("fatal", "err", err)
		return 1
	}
	log.Info("bye")
	return 0
}

// probeHealth performs GET http://127.0.0.1<addr>/healthz (distroless images
// have no curl/wget, so the binary checks itself).
func probeHealth(addr string, stderr io.Writer) int {
	if addr == "" {
		fmt.Fprintln(stderr, "http server disabled")
		return 1
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, port)+"/healthz", nil)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "unhealthy:", resp.Status)
		return 1
	}
	return 0
}

func defaultConfigPath() string {
	if p := os.Getenv("CONFIG_FILE"); p != "" {
		return p
	}
	if _, err := os.Stat("config.yaml"); err == nil {
		return "config.yaml"
	}
	return "" // defaults + env only
}

func newLogger(c config.Log, w io.Writer) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(c.Level)); err != nil {
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	if strings.EqualFold(c.Format, "text") {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}
