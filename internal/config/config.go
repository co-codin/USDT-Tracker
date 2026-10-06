// Package config loads configuration from a YAML file with environment
// variable overrides (env wins). Secrets are expected to come from env.
package config

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/co-codin/tron-usdt-listener/internal/model"
	"github.com/co-codin/tron-usdt-listener/internal/tron"
)

// USDTContract is the USDT TRC-20 contract on TRON mainnet.
const USDTContract = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"

// Duration is a time.Duration that unmarshals from strings like "15s".
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q", n.Line, n.Value)
	}
	*d = Duration(v)
	return nil
}

// D returns the value as time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Config is the full application configuration.
type Config struct {
	Log      Log      `yaml:"log"`
	Tron     Tron     `yaml:"tron"`
	Token    Token    `yaml:"token"`
	Listener Listener `yaml:"listener"`
	Cursor   Cursor   `yaml:"cursor"`
	Filter   Filter   `yaml:"filter"`
	HTTP     HTTP     `yaml:"http"`
	Sinks    Sinks    `yaml:"sinks"`
}

// Log configures application logging (stderr).
type Log struct {
	Level  string `yaml:"level"`  // debug|info|warn|error
	Format string `yaml:"format"` // json|text
}

// Tron configures the TRON HTTP API client.
type Tron struct {
	APIURL     string   `yaml:"api_url"`
	APIKey     string   `yaml:"api_key"`
	Timeout    Duration `yaml:"timeout"`
	RPS        float64  `yaml:"rps"`
	MaxRetries int      `yaml:"max_retries"`
}

// Token selects the TRC-20 contract (USDT by default).
type Token struct {
	Contract string `yaml:"contract"`
	Symbol   string `yaml:"symbol"`
	Decimals int    `yaml:"decimals"`
}

// Listener tunes the processing loop.
type Listener struct {
	Confirmations   int64    `yaml:"confirmations"`
	PollInterval    Duration `yaml:"poll_interval"`
	StartBlock      int64    `yaml:"start_block"`
	BatchSize       int      `yaml:"batch_size"`
	Concurrency     int      `yaml:"concurrency"`
	DedupeCacheSize int      `yaml:"dedupe_cache_size"`
	DispatchTimeout Duration `yaml:"dispatch_timeout"`
}

// Cursor configures the file cursor (ignored when the postgres sink is on).
type Cursor struct {
	File string `yaml:"file"`
}

// Filter selects which transfers are emitted.
type Filter struct {
	WatchAddresses []string `yaml:"watch_addresses"`
	Direction      string   `yaml:"direction"` // both|incoming|outgoing
	MinAmount      string   `yaml:"min_amount"`
	Mode           string   `yaml:"mode"` // any|all
}

// HTTP configures the ops server.
type HTTP struct {
	Addr       string   `yaml:"addr"` // empty disables the server
	StaleAfter Duration `yaml:"health_stale_after"`
}

// Sinks groups all sink configs.
type Sinks struct {
	Stdout   StdoutSink   `yaml:"stdout"`
	Webhook  WebhookSink  `yaml:"webhook"`
	Telegram TelegramSink `yaml:"telegram"`
	Postgres PostgresSink `yaml:"postgres"`
}

// StdoutSink config.
type StdoutSink struct {
	Enabled bool `yaml:"enabled"`
}

// WebhookSink config.
type WebhookSink struct {
	Enabled    bool              `yaml:"enabled"`
	URL        string            `yaml:"url"`
	Secret     string            `yaml:"secret"`
	Timeout    Duration          `yaml:"timeout"`
	MaxRetries int               `yaml:"max_retries"`
	Headers    map[string]string `yaml:"headers"`
	BestEffort bool              `yaml:"best_effort"`
}

// TelegramSink config.
type TelegramSink struct {
	Enabled     bool   `yaml:"enabled"`
	BotToken    string `yaml:"bot_token"`
	ChatID      string `yaml:"chat_id"`
	MinAmount   string `yaml:"min_amount"`
	MaxPerBlock int    `yaml:"max_messages_per_block"`
	BestEffort  bool   `yaml:"best_effort"`
	APIBase     string `yaml:"api_base"` // default https://api.telegram.org (override for proxies/tests)
}

// PostgresSink config.
type PostgresSink struct {
	Enabled    bool   `yaml:"enabled"`
	DSN        string `yaml:"dsn"`
	CursorName string `yaml:"cursor_name"`
	BestEffort bool   `yaml:"best_effort"`
}

// Default returns the built-in defaults.
func Default() Config {
	return Config{
		Log:  Log{Level: "info", Format: "json"},
		Tron: Tron{APIURL: tron.DefaultBaseURL, Timeout: Duration(15 * time.Second), RPS: 3, MaxRetries: 8},
		Token: Token{
			Contract: USDTContract, Symbol: "USDT", Decimals: 6,
		},
		Listener: Listener{
			Confirmations: 20, PollInterval: Duration(3 * time.Second), BatchSize: 20, Concurrency: 2,
			DedupeCacheSize: 100_000, DispatchTimeout: Duration(30 * time.Second),
		},
		Cursor: Cursor{File: "./data/cursor.json"},
		Filter: Filter{Direction: "both", Mode: "any"},
		HTTP:   HTTP{Addr: ":9090", StaleAfter: Duration(2 * time.Minute)},
		Sinks: Sinks{
			Stdout:   StdoutSink{Enabled: true},
			Webhook:  WebhookSink{Timeout: Duration(10 * time.Second), MaxRetries: 5},
			Telegram: TelegramSink{MaxPerBlock: 10, BestEffort: true},
			Postgres: PostgresSink{CursorName: "default"},
		},
	}
}

// Load reads path (optional: "" or a missing default file means defaults
// only), then applies env overrides via lookup (os.LookupEnv in production)
// and validates the result.
func Load(path string, lookup func(string) (string, bool)) (Config, error) {
	cfg := Default()
	if path != "" {
		b, err := os.ReadFile(path) //nolint:gosec // G304: the path is the operator-supplied -config flag
		if err != nil {
			return cfg, fmt.Errorf("config: %w", err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(b)))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
			return cfg, fmt.Errorf("config: parse %s: %w", path, err)
		}
	}
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if err := applyEnv(&cfg, lookup); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if _, err := tron.ParseAddress(c.Token.Contract); err != nil {
		add("token.contract: %v", err)
	}
	if c.Token.Decimals < 0 || c.Token.Decimals > 36 {
		add("token.decimals must be between 0 and 36")
	}
	if c.Listener.Confirmations < 0 {
		add("listener.confirmations must be >= 0")
	}
	if c.Listener.BatchSize < 1 || c.Listener.BatchSize > 1000 {
		add("listener.batch_size must be between 1 and 1000")
	}
	if c.Listener.Concurrency < 1 || c.Listener.Concurrency > 64 {
		add("listener.concurrency must be between 1 and 64")
	}
	if c.Tron.RPS <= 0 {
		add("tron.rps must be > 0")
	}
	for _, a := range c.Filter.WatchAddresses {
		if _, err := tron.ParseAddress(a); err != nil {
			add("filter.watch_addresses: %q: %v", a, err)
		}
	}
	switch c.Filter.Direction {
	case "both", "incoming", "outgoing":
	default:
		add("filter.direction must be both|incoming|outgoing, got %q", c.Filter.Direction)
	}
	switch c.Filter.Mode {
	case "any", "all":
	default:
		add("filter.mode must be any|all, got %q", c.Filter.Mode)
	}
	if _, err := c.MinAmount(); err != nil {
		add("filter.min_amount: %v", err)
	}
	if _, err := model.ParseUnits(c.Sinks.Telegram.MinAmount, c.Token.Decimals); err != nil {
		add("sinks.telegram.min_amount: %v", err)
	}
	s := c.Sinks
	if !s.Stdout.Enabled && !s.Webhook.Enabled && !s.Telegram.Enabled && !s.Postgres.Enabled {
		add("at least one sink must be enabled")
	}
	if s.Webhook.Enabled && s.Webhook.URL == "" {
		add("sinks.webhook.url is required (WEBHOOK_URL)")
	}
	if s.Telegram.Enabled && (s.Telegram.BotToken == "" || s.Telegram.ChatID == "") {
		add("sinks.telegram requires bot_token and chat_id (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)")
	}
	if s.Postgres.Enabled && s.Postgres.DSN == "" {
		add("sinks.postgres.dsn is required (DATABASE_URL)")
	}
	if !s.Postgres.Enabled && c.Cursor.File == "" {
		add("cursor.file is required when the postgres sink is disabled")
	}
	return errors.Join(errs...)
}

// MinAmount returns the large-transfer threshold in raw units.
func (c *Config) MinAmount() (*big.Int, error) {
	return model.ParseUnits(c.Filter.MinAmount, c.Token.Decimals)
}

// envBinding maps an env var to a setter.
type envBinding struct {
	name string
	set  func(c *Config, v string) error
}

func str(dst func(*Config) *string) func(*Config, string) error {
	return func(c *Config, v string) error { *dst(c) = v; return nil }
}

func boolean(dst func(*Config) *bool) func(*Config, string) error {
	return func(c *Config, v string) error {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return err
		}
		*dst(c) = b
		return nil
	}
}

func integer(dst func(*Config) *int64) func(*Config, string) error {
	return func(c *Config, v string) error {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}
		*dst(c) = n
		return nil
	}
}

func intVal(dst func(*Config) *int) func(*Config, string) error {
	return func(c *Config, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		*dst(c) = n
		return nil
	}
}

func duration(dst func(*Config) *Duration) func(*Config, string) error {
	return func(c *Config, v string) error {
		d, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		*dst(c) = Duration(d)
		return nil
	}
}

// EnvBindings lists every supported environment variable (documented in README).
var envBindings = []envBinding{
	{"LOG_LEVEL", str(func(c *Config) *string { return &c.Log.Level })},
	{"LOG_FORMAT", str(func(c *Config) *string { return &c.Log.Format })},
	{"TRON_API_URL", str(func(c *Config) *string { return &c.Tron.APIURL })},
	{"TRON_PRO_API_KEY", str(func(c *Config) *string { return &c.Tron.APIKey })},
	{"TRON_RPS", func(c *Config, v string) error {
		f, err := strconv.ParseFloat(v, 64)
		c.Tron.RPS = f
		return err
	}},
	{"TOKEN_CONTRACT", str(func(c *Config) *string { return &c.Token.Contract })},
	{"LISTENER_CONFIRMATIONS", integer(func(c *Config) *int64 { return &c.Listener.Confirmations })},
	{"LISTENER_START_BLOCK", integer(func(c *Config) *int64 { return &c.Listener.StartBlock })},
	{"LISTENER_POLL_INTERVAL", duration(func(c *Config) *Duration { return &c.Listener.PollInterval })},
	{"LISTENER_BATCH_SIZE", intVal(func(c *Config) *int { return &c.Listener.BatchSize })},
	{"LISTENER_CONCURRENCY", intVal(func(c *Config) *int { return &c.Listener.Concurrency })},
	{"CURSOR_FILE", str(func(c *Config) *string { return &c.Cursor.File })},
	{"FILTER_WATCH_ADDRESSES", func(c *Config, v string) error {
		c.Filter.WatchAddresses = nil
		for _, a := range strings.Split(v, ",") {
			if a = strings.TrimSpace(a); a != "" {
				c.Filter.WatchAddresses = append(c.Filter.WatchAddresses, a)
			}
		}
		return nil
	}},
	{"FILTER_DIRECTION", str(func(c *Config) *string { return &c.Filter.Direction })},
	{"FILTER_MIN_AMOUNT", str(func(c *Config) *string { return &c.Filter.MinAmount })},
	{"FILTER_MODE", str(func(c *Config) *string { return &c.Filter.Mode })},
	{"HTTP_ADDR", str(func(c *Config) *string { return &c.HTTP.Addr })},
	{"SINK_STDOUT_ENABLED", boolean(func(c *Config) *bool { return &c.Sinks.Stdout.Enabled })},
	{"SINK_WEBHOOK_ENABLED", boolean(func(c *Config) *bool { return &c.Sinks.Webhook.Enabled })},
	{"WEBHOOK_URL", str(func(c *Config) *string { return &c.Sinks.Webhook.URL })},
	{"WEBHOOK_SECRET", str(func(c *Config) *string { return &c.Sinks.Webhook.Secret })},
	{"SINK_TELEGRAM_ENABLED", boolean(func(c *Config) *bool { return &c.Sinks.Telegram.Enabled })},
	{"TELEGRAM_BOT_TOKEN", str(func(c *Config) *string { return &c.Sinks.Telegram.BotToken })},
	{"TELEGRAM_CHAT_ID", str(func(c *Config) *string { return &c.Sinks.Telegram.ChatID })},
	{"TELEGRAM_MIN_AMOUNT", str(func(c *Config) *string { return &c.Sinks.Telegram.MinAmount })},
	{"SINK_POSTGRES_ENABLED", boolean(func(c *Config) *bool { return &c.Sinks.Postgres.Enabled })},
	{"DATABASE_URL", str(func(c *Config) *string { return &c.Sinks.Postgres.DSN })},
}

// EnvVars returns the names of all supported environment variables.
func EnvVars() []string {
	out := make([]string, len(envBindings))
	for i, b := range envBindings {
		out[i] = b.name
	}
	return out
}

func applyEnv(c *Config, lookup func(string) (string, bool)) error {
	var errs []error
	for _, b := range envBindings {
		v, ok := lookup(b.name)
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		if v == "" {
			continue // empty values (e.g. from .env templates) don't override
		}
		if err := b.set(c, v); err != nil {
			errs = append(errs, fmt.Errorf("env %s: %w", b.name, err))
		}
	}
	return errors.Join(errs...)
}
