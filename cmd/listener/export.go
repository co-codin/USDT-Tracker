package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/co-codin/USDT-Tracker/internal/config"
	"github.com/co-codin/USDT-Tracker/internal/export"
	"github.com/co-codin/USDT-Tracker/internal/sink/postgres"
)

// runExport implements `listener export`: writes the CSV statement of one
// address from the Postgres transfers table (DATABASE_URL) to -o or stdout.
func runExport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		configPath = fs.String("config", defaultConfigPath(), "path to YAML config (env: CONFIG_FILE)")
		address    = fs.String("address", "", "address to export (required)")
		chainName  = fs.String("chain", "", "tron|ton|eth (default: detected from the address)")
		from       = fs.String("from", "", "start, inclusive: YYYY-MM-DD or RFC 3339 (UTC)")
		to         = fs.String("to", "", "end: YYYY-MM-DD (whole day included) or RFC 3339 (exclusive)")
		out        = fs.String("o", "", "output file (default stdout)")
		bom        = fs.Bool("bom", false, "prefix a UTF-8 byte order mark (for Excel)")
	)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: tron-usdt-listener export -address <addr> [-chain tron|ton|eth] [-from YYYY-MM-DD] [-to YYYY-MM-DD] [-o file.csv] [-bom]")
		fmt.Fprintln(stderr, "Times are UTC (RFC 3339); amounts are exact decimal strings.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	q, err := export.ParseQuery(*chainName, *address, *from, *to)
	if err != nil {
		fmt.Fprintln(stderr, "export:", err)
		return 2
	}
	cfg, err := config.Load(*configPath, os.LookupEnv)
	if err != nil {
		fmt.Fprintln(stderr, "configuration error:\n"+err.Error())
		return 2
	}
	if cfg.Sinks.Postgres.DSN == "" {
		fmt.Fprintln(stderr, "export: DATABASE_URL (sinks.postgres.dsn) is required")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pg, err := postgres.Open(ctx, cfg.Sinks.Postgres.DSN, cfg.Sinks.Postgres.CursorName)
	if err != nil {
		fmt.Fprintln(stderr, "export:", err)
		return 1
	}
	defer pg.Close()
	w := stdout
	var f *os.File
	if *out != "" {
		f, err = os.Create(*out) //nolint:gosec // G304: operator-supplied output path
		if err != nil {
			fmt.Fprintln(stderr, "export:", err)
			return 1
		}
		w = f
	}
	n, err := export.Write(ctx, w, pg, q, export.Options{BOM: *bom})
	if f != nil {
		// A failed Close (e.g. disk full on flush) means a truncated file.
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, "export:", err)
		return 1
	}
	fmt.Fprintf(stderr, "exported %d transfers\n", n)
	return 0
}
