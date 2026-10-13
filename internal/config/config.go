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

	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
	"github.com/co-codin/USDT-Tracker/internal/model"
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
	Log Log `yaml:"log"`
	// Chains lists the chains to listen on: any of tron (default), ton, eth.
	// Each chain runs its own listener loop and cursor; sinks are shared.
	Chains   []string `yaml:"chains"`
	Tron     Tron     `yaml:"tron"`
	TON      TON      `yaml:"ton"`
	ETH      ETH      `yaml:"eth"`
	Token    Token    `yaml:"token"`
	Listener Listener `yaml:"listener"`
	Cursor   Cursor   `yaml:"cursor"`
	Filter   Filter   `yaml:"filter"`
	HTTP     HTTP     `yaml:"http"`
	API      API      `yaml:"api"`
	Sinks    Sinks    `yaml:"sinks"`
	Alerts   Alerts   `yaml:"alerts"`
	// NotifyBot is the self-service Telegram bot (/add your address, get a DM per transfer).
	NotifyBot NotifyBot `yaml:"notify_bot"`
}

// MinAPITokenLen is the minimum accepted admin API token length.
const MinAPITokenLen = 16

// API configures the admin API served on http.addr under /v1/.
type API struct {
	Token string `yaml:"token"` // bearer token; empty disables the API
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
	// VerifyTxCount cross-checks every non-empty block's transaction-info
	// count against getblockbynum (one extra request per block, default
	// true). Disable only to speed up large backfills on a trusted node.
	VerifyTxCount bool `yaml:"verify_tx_count"`
}

// TON configures the TON listener (toncenter v3 index API). Block numbers
// on TON are masterchain seqnos.
type TON struct {
	APIURL       string   `yaml:"api_url"`
	APIKey       string   `yaml:"api_key"` // optional toncenter key (TONCENTER_API_KEY)
	Timeout      Duration `yaml:"timeout"`
	RPS          float64  `yaml:"rps"`
	MaxRetries   int      `yaml:"max_retries"`
	JettonMaster string   `yaml:"jetton_master"` // default: official USD₮ master
	Symbol       string   `yaml:"symbol"`
	Decimals     int      `yaml:"decimals"`
	// WatchAddresses are TON addresses (raw "0:<hex>" or EQ…/UQ…) watched
	// with filter.direction; filter.min_amount and filter.mode apply too.
	WatchAddresses []string `yaml:"watch_addresses"`
	Confirmations  int64    `yaml:"confirmations"`
	PollInterval   Duration `yaml:"poll_interval"`
	StartBlock     int64    `yaml:"start_block"`
	BatchSize      int      `yaml:"batch_size"`
	Concurrency    int      `yaml:"concurrency"`
	// CursorFile is the TON file cursor (ignored when the postgres sink is
	// on: the Postgres cursor "<cursor_name>:ton" is used instead).
	CursorFile string `yaml:"cursor_file"`
}

// ETH configures the Ethereum listener (ERC-20 USDT via JSON-RPC
// eth_blockNumber + eth_getLogs). Read-only: no keys are ever needed.
type ETH struct {
	// RPCURL is the JSON-RPC endpoint (ETH_RPC_URL). Provider URLs often
	// embed an API key, so it is treated as a secret and never logged.
	RPCURL     string   `yaml:"rpc_url"`
	Timeout    Duration `yaml:"timeout"`
	RPS        float64  `yaml:"rps"`
	MaxRetries int      `yaml:"max_retries"`
	Contract   string   `yaml:"contract"` // default: USDT 0xdAC17F958D2ee523a2206206994597C13D831ec7
	Symbol     string   `yaml:"symbol"`
	Decimals   int      `yaml:"decimals"`
	// WatchAddresses are Ethereum addresses (0x…, lowercase or EIP-55).
	WatchAddresses []string `yaml:"watch_addresses"`
	Confirmations  int64    `yaml:"confirmations"`
	PollInterval   Duration `yaml:"poll_interval"`
	StartBlock     int64    `yaml:"start_block"`
	// BatchSize is the number of blocks processed per iteration.
	BatchSize int `yaml:"batch_size"`
	// HeadTag selects the head block: "latest" (default, then minus
	// Confirmations), "safe" or "finalized" (reorg-proof; ~13 min behind,
	// use with confirmations 0).
	HeadTag string `yaml:"head_tag"`
	// LogChunk is the max block range per eth_getLogs call (halved
	// automatically when the node rejects a range).
	LogChunk int `yaml:"log_chunk"`
	// CursorFile is the ETH file cursor (Postgres cursor "<cursor_name>:eth"
	// is used when the postgres sink is on).
	CursorFile string `yaml:"cursor_file"`
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
	// ReloadInterval is how often enabled rows of the Postgres
	// watch_addresses table are re-read (API writes apply immediately).
	ReloadInterval Duration `yaml:"reload_interval"`
}

// HTTP configures the ops server.
type HTTP struct {
	Addr       string   `yaml:"addr"` // empty disables the server
	StaleAfter Duration `yaml:"health_stale_after"`
	// HealthDetails includes raw upstream error messages in /healthz
	// (off by default: they can reveal hosts or provider details).
	HealthDetails bool `yaml:"health_details"`
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

// Alerts groups chat alerts that are independent of the watch list.
type Alerts struct {
	LargeTransfers LargeTransferAlerts `yaml:"large_transfers"`
}

// LargeTransferAlerts posts every transfer >= Threshold on any enabled chain
// to a Telegram channel (the bot must be an admin of the channel).
type LargeTransferAlerts struct {
	Enabled bool `yaml:"enabled"`
	// BotToken defaults to sinks.telegram.bot_token (TELEGRAM_BOT_TOKEN).
	BotToken      string   `yaml:"bot_token"`
	ChatID        string   `yaml:"chat_id"`   // e.g. @usdt_whales or -100…
	Threshold     string   `yaml:"threshold"` // token units, default 100000
	Interval      Duration `yaml:"interval"`  // min gap between messages, default 3s (20/min)
	MaxPerMessage int      `yaml:"max_per_message"`
	QueueSize     int      `yaml:"queue_size"`
	Footer        string   `yaml:"footer"`   // optional HTML line under every alert
	APIBase       string   `yaml:"api_base"` // default https://api.telegram.org
}

// Token returns the bot token for large-transfer alerts.
func (l LargeTransferAlerts) Token(c *Config) string {
	if l.BotToken != "" {
		return l.BotToken
	}
	return c.Sinks.Telegram.BotToken
}

// NotifyBot configures the self-service notify bot (runs inside the
// listener process; needs the postgres sink for users and subscriptions).
type NotifyBot struct {
	Enabled      bool     `yaml:"enabled"`
	BotToken     string   `yaml:"bot_token"`     // TELEGRAM_NOTIFY_BOT_TOKEN (a separate bot from the alert bot)
	FreeLimit    int      `yaml:"free_limit"`    // addresses per user on the free plan (default 3)
	PremiumLimit int      `yaml:"premium_limit"` // addresses on plan 'premium' (default 50)
	MinAmount    string   `yaml:"min_amount"`    // skip dust / zero-value poisoning transfers (default 0.01)
	PollTimeout  Duration `yaml:"poll_timeout"`  // getUpdates long-poll timeout (default 50s)
	APIBase      string   `yaml:"api_base"`      // default https://api.telegram.org
	// MaxUsers / MaxTotalSubs cap bot-wide users and subscriptions (0 = unlimited).
	MaxUsers     int `yaml:"max_users"`
	MaxTotalSubs int `yaml:"max_total_subs"`
	// PremiumContact is shown by /plan as the way to upgrade (e.g. @you or a URL).
	PremiumContact string `yaml:"premium_contact"`
	// CommandsPerMinute is the per-chat command rate limit (default 20).
	CommandsPerMinute int `yaml:"commands_per_minute"`
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
		Log:    Log{Level: "info", Format: "json"},
		Chains: []string{model.ChainTron},
		Tron:   Tron{APIURL: tron.DefaultBaseURL, Timeout: Duration(15 * time.Second), RPS: 3, MaxRetries: 8, VerifyTxCount: true},
		TON: TON{
			APIURL: ton.DefaultBaseURL, Timeout: Duration(15 * time.Second), RPS: 1, MaxRetries: 8,
			JettonMaster: ton.USDTMaster, Symbol: "USDT", Decimals: 6,
			Confirmations: 3, PollInterval: Duration(time.Second), BatchSize: 20, Concurrency: 2,
			CursorFile: "./data/cursor-ton.json",
		},
		ETH: ETH{
			RPCURL: eth.DefaultRPCURL, Timeout: Duration(20 * time.Second), RPS: 5, MaxRetries: 8,
			Contract: eth.USDTContract, Symbol: "USDT", Decimals: 6,
			Confirmations: 12, PollInterval: Duration(6 * time.Second), BatchSize: 50, LogChunk: 50, HeadTag: "latest",
			CursorFile: "./data/cursor-eth.json",
		},
		Token: Token{
			Contract: USDTContract, Symbol: "USDT", Decimals: 6,
		},
		Listener: Listener{
			Confirmations: 20, PollInterval: Duration(3 * time.Second), BatchSize: 20, Concurrency: 2,
			DedupeCacheSize: 100_000, DispatchTimeout: Duration(30 * time.Second),
		},
		Cursor: Cursor{File: "./data/cursor.json"},
		Filter: Filter{Direction: "both", Mode: "any", ReloadInterval: Duration(5 * time.Second)},
		HTTP:   HTTP{Addr: "127.0.0.1:9090", StaleAfter: Duration(2 * time.Minute)},
		Sinks: Sinks{
			Stdout:   StdoutSink{Enabled: true},
			Webhook:  WebhookSink{Timeout: Duration(10 * time.Second), MaxRetries: 5},
			Telegram: TelegramSink{MaxPerBlock: 10, BestEffort: true},
			Postgres: PostgresSink{CursorName: "default"},
		},
		NotifyBot: NotifyBot{FreeLimit: 3, PremiumLimit: 50, MinAmount: "0.01", PollTimeout: Duration(50 * time.Second),
			MaxUsers: 10000, MaxTotalSubs: 50000, CommandsPerMinute: 20},
		Alerts: Alerts{LargeTransfers: LargeTransferAlerts{
			Threshold: "100000", Interval: Duration(3 * time.Second), MaxPerMessage: 10, QueueSize: 1000,
		}},
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

	if len(c.Chains) == 0 {
		add("chains must list at least one of tron, ton, eth")
	}
	seenChain := map[string]bool{}
	for _, ch := range c.Chains {
		if ch != model.ChainTron && ch != model.ChainTON && ch != model.ChainETH {
			add("chains: unknown chain %q (want tron|ton|eth)", ch)
		}
		if seenChain[ch] {
			add("chains: %q listed twice", ch)
		}
		seenChain[ch] = true
	}
	if c.HasChain(model.ChainTON) {
		c.validateTON(add)
	}
	if c.HasChain(model.ChainETH) {
		c.validateETH(add)
	}
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
			if _, terr := ton.ParseAddress(a); terr == nil {
				add("filter.watch_addresses: %q is a TON address: put it in ton.watch_addresses (TON_WATCH_ADDRESSES)", a)
				continue
			}
			if eth.IsAddress(a) {
				add("filter.watch_addresses: %q is an Ethereum address: put it in eth.watch_addresses (ETH_WATCH_ADDRESSES)", a)
				continue
			}
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
	if c.Filter.ReloadInterval.D() < 100*time.Millisecond || c.Filter.ReloadInterval.D() > time.Hour {
		add("filter.reload_interval must be between 100ms and 1h, got %s", c.Filter.ReloadInterval.D())
	}
	if c.API.Token != "" {
		if len(c.API.Token) < MinAPITokenLen {
			add("api.token (API_TOKEN) must be at least %d characters, e.g. `openssl rand -hex 32`", MinAPITokenLen)
		}
		if strings.ContainsAny(c.API.Token, " \t\r\n") {
			add("api.token (API_TOKEN) must not contain whitespace")
		}
		if c.HTTP.Addr == "" {
			add("api.token is set but http.addr is empty: the admin API is served on http.addr")
		}
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
	if la := c.Alerts.LargeTransfers; la.Enabled {
		if la.ChatID == "" || la.Token(c) == "" {
			add("alerts.large_transfers requires chat_id and a bot token (LARGE_ALERTS_CHAT_ID, LARGE_ALERTS_BOT_TOKEN or TELEGRAM_BOT_TOKEN)")
		}
		if v, err := model.ParseUnits(la.Threshold, 18); err != nil || v.Sign() <= 0 {
			add("alerts.large_transfers.threshold must be a positive amount, got %q", la.Threshold)
		}
		if la.Interval.D() < 500*time.Millisecond {
			add("alerts.large_transfers.interval must be >= 500ms (Telegram allows ~20 messages/minute per channel)")
		}
		if la.MaxPerMessage < 1 || la.MaxPerMessage > 30 {
			add("alerts.large_transfers.max_per_message must be between 1 and 30")
		}
		if la.QueueSize < 1 {
			add("alerts.large_transfers.queue_size must be >= 1")
		}
	}
	if nb := c.NotifyBot; nb.Enabled {
		if nb.BotToken == "" {
			add("notify_bot requires bot_token (TELEGRAM_NOTIFY_BOT_TOKEN)")
		}
		if !s.Postgres.Enabled {
			add("notify_bot needs the postgres sink (SINK_POSTGRES_ENABLED=true, DATABASE_URL) to store users and subscriptions")
		}
		if nb.MaxUsers < 0 || nb.MaxTotalSubs < 0 || nb.CommandsPerMinute < 0 {
			add("notify_bot: max_users and max_total_subs must be >= 0 (0 = unlimited) and commands_per_minute >= 0 (0 = default 20)")
		}
		if nb.FreeLimit < 1 || nb.PremiumLimit < nb.FreeLimit {
			add("notify_bot: free_limit must be >= 1 and premium_limit >= free_limit")
		}
		if _, err := model.ParseUnits(nb.MinAmount, 18); err != nil {
			add("notify_bot.min_amount: %v", err)
		}
		if nb.PollTimeout.D() < time.Second || nb.PollTimeout.D() > 60*time.Second {
			add("notify_bot.poll_timeout must be between 1s and 60s")
		}
	}
	if s.Postgres.Enabled && s.Postgres.DSN == "" {
		add("sinks.postgres.dsn is required (DATABASE_URL)")
	}
	if !s.Postgres.Enabled && c.Cursor.File == "" && c.HasChain(model.ChainTron) {
		add("cursor.file is required when the postgres sink is disabled")
	}
	return errors.Join(errs...)
}

func (c *Config) validateTON(add func(string, ...any)) {
	t := c.TON
	if _, err := ton.ParseAddress(t.JettonMaster); err != nil {
		add("ton.jetton_master: %v", err)
	}
	if t.APIURL == "" {
		add("ton.api_url is required")
	}
	if t.Decimals < 0 || t.Decimals > 36 {
		add("ton.decimals must be between 0 and 36")
	}
	if t.RPS <= 0 {
		add("ton.rps must be > 0")
	}
	if t.Confirmations < 0 {
		add("ton.confirmations must be >= 0")
	}
	if t.BatchSize < 1 || t.BatchSize > 1000 {
		add("ton.batch_size must be between 1 and 1000")
	}
	if t.Concurrency < 1 || t.Concurrency > 64 {
		add("ton.concurrency must be between 1 and 64")
	}
	for _, a := range t.WatchAddresses {
		if _, err := ton.ParseAddress(a); err != nil {
			add("ton.watch_addresses: %q: %v", a, err)
		}
	}
	if _, err := model.ParseUnits(c.Filter.MinAmount, t.Decimals); err != nil {
		add("filter.min_amount (ton decimals): %v", err)
	}
	if !c.Sinks.Postgres.Enabled && t.CursorFile == "" {
		add("ton.cursor_file is required when the postgres sink is disabled")
	}
	if !c.Sinks.Postgres.Enabled && t.CursorFile == c.Cursor.File && c.HasChain(model.ChainTron) {
		add("ton.cursor_file must differ from cursor.file")
	}
}

func (c *Config) validateETH(add func(string, ...any)) {
	e := c.ETH
	if _, err := eth.NormalizeAddress(e.Contract); err != nil {
		add("eth.contract: %v", err)
	}
	if e.RPCURL == "" {
		add("eth.rpc_url is required (ETH_RPC_URL)")
	} else if r := eth.RedactURL(e.RPCURL); r == "<invalid url>" || (!strings.HasPrefix(e.RPCURL, "http://") && !strings.HasPrefix(e.RPCURL, "https://")) {
		add("eth.rpc_url must be an http(s) URL")
	}
	if e.Decimals < 0 || e.Decimals > 36 {
		add("eth.decimals must be between 0 and 36")
	}
	if e.RPS <= 0 {
		add("eth.rps must be > 0")
	}
	if e.Confirmations < 0 {
		add("eth.confirmations must be >= 0")
	}
	if e.BatchSize < 1 || e.BatchSize > 5000 {
		add("eth.batch_size must be between 1 and 5000")
	}
	switch e.HeadTag {
	case "latest", "safe", "finalized":
	default:
		add("eth.head_tag must be latest, safe or finalized (ETH_HEAD_TAG)")
	}
	if e.LogChunk < 1 || e.LogChunk > 5000 {
		add("eth.log_chunk must be between 1 and 5000")
	}
	for _, a := range e.WatchAddresses {
		if _, err := eth.NormalizeAddress(a); err != nil {
			add("eth.watch_addresses: %v", err)
		}
	}
	if _, err := model.ParseUnits(c.Filter.MinAmount, e.Decimals); err != nil {
		add("filter.min_amount (eth decimals): %v", err)
	}
	if !c.Sinks.Postgres.Enabled {
		if e.CursorFile == "" {
			add("eth.cursor_file is required when the postgres sink is disabled")
		}
		if (e.CursorFile == c.Cursor.File && c.HasChain(model.ChainTron)) || (e.CursorFile == c.TON.CursorFile && c.HasChain(model.ChainTON)) {
			add("eth.cursor_file must differ from the other chains' cursor files")
		}
	}
}

// ETHMinAmount returns the large-transfer threshold in ERC-20 units.
func (c *Config) ETHMinAmount() (*big.Int, error) {
	return model.ParseUnits(c.Filter.MinAmount, c.ETH.Decimals)
}

// HasChain reports whether chain is enabled.
func (c *Config) HasChain(chain string) bool {
	for _, x := range c.Chains {
		if x == chain {
			return true
		}
	}
	return false
}

// TONMinAmount returns the large-transfer threshold in TON jetton units.
func (c *Config) TONMinAmount() (*big.Int, error) {
	return model.ParseUnits(c.Filter.MinAmount, c.TON.Decimals)
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
	{"TRON_VERIFY_TX_COUNT", boolean(func(c *Config) *bool { return &c.Tron.VerifyTxCount })},
	{"TRON_RPS", func(c *Config, v string) error {
		f, err := strconv.ParseFloat(v, 64)
		c.Tron.RPS = f
		return err
	}},
	{"CHAINS", func(c *Config, v string) error {
		c.Chains = splitList(strings.ToLower(v))
		return nil
	}},
	{"TON_API_URL", str(func(c *Config) *string { return &c.TON.APIURL })},
	{"TONCENTER_API_KEY", str(func(c *Config) *string { return &c.TON.APIKey })},
	{"TON_RPS", func(c *Config, v string) error {
		f, err := strconv.ParseFloat(v, 64)
		c.TON.RPS = f
		return err
	}},
	{"TON_JETTON_MASTER", str(func(c *Config) *string { return &c.TON.JettonMaster })},
	{"TON_WATCH_ADDRESSES", func(c *Config, v string) error {
		c.TON.WatchAddresses = splitList(v)
		return nil
	}},
	{"TON_CONFIRMATIONS", integer(func(c *Config) *int64 { return &c.TON.Confirmations })},
	{"TON_START_BLOCK", integer(func(c *Config) *int64 { return &c.TON.StartBlock })},
	{"TON_POLL_INTERVAL", duration(func(c *Config) *Duration { return &c.TON.PollInterval })},
	{"TON_CURSOR_FILE", str(func(c *Config) *string { return &c.TON.CursorFile })},
	{"ETH_RPC_URL", str(func(c *Config) *string { return &c.ETH.RPCURL })},
	{"ETH_RPS", func(c *Config, v string) error {
		f, err := strconv.ParseFloat(v, 64)
		c.ETH.RPS = f
		return err
	}},
	{"ETH_CONTRACT", str(func(c *Config) *string { return &c.ETH.Contract })},
	{"ETH_WATCH_ADDRESSES", func(c *Config, v string) error {
		c.ETH.WatchAddresses = splitList(v)
		return nil
	}},
	{"ETH_CONFIRMATIONS", integer(func(c *Config) *int64 { return &c.ETH.Confirmations })},
	{"ETH_START_BLOCK", integer(func(c *Config) *int64 { return &c.ETH.StartBlock })},
	{"ETH_POLL_INTERVAL", duration(func(c *Config) *Duration { return &c.ETH.PollInterval })},
	{"ETH_BATCH_SIZE", intVal(func(c *Config) *int { return &c.ETH.BatchSize })},
	{"ETH_LOG_CHUNK", intVal(func(c *Config) *int { return &c.ETH.LogChunk })},
	{"ETH_HEAD_TAG", str(func(c *Config) *string { return &c.ETH.HeadTag })},
	{"ETH_CURSOR_FILE", str(func(c *Config) *string { return &c.ETH.CursorFile })},
	{"TOKEN_CONTRACT", str(func(c *Config) *string { return &c.Token.Contract })},
	{"LISTENER_CONFIRMATIONS", integer(func(c *Config) *int64 { return &c.Listener.Confirmations })},
	{"LISTENER_START_BLOCK", integer(func(c *Config) *int64 { return &c.Listener.StartBlock })},
	{"LISTENER_POLL_INTERVAL", duration(func(c *Config) *Duration { return &c.Listener.PollInterval })},
	{"LISTENER_BATCH_SIZE", intVal(func(c *Config) *int { return &c.Listener.BatchSize })},
	{"LISTENER_CONCURRENCY", intVal(func(c *Config) *int { return &c.Listener.Concurrency })},
	{"CURSOR_FILE", str(func(c *Config) *string { return &c.Cursor.File })},
	{"FILTER_WATCH_ADDRESSES", func(c *Config, v string) error {
		c.Filter.WatchAddresses = splitList(v)
		return nil
	}},
	{"FILTER_DIRECTION", str(func(c *Config) *string { return &c.Filter.Direction })},
	{"FILTER_MIN_AMOUNT", str(func(c *Config) *string { return &c.Filter.MinAmount })},
	{"FILTER_MODE", str(func(c *Config) *string { return &c.Filter.Mode })},
	{"FILTER_RELOAD_INTERVAL", duration(func(c *Config) *Duration { return &c.Filter.ReloadInterval })},
	{"HTTP_ADDR", str(func(c *Config) *string { return &c.HTTP.Addr })},
	{"API_TOKEN", str(func(c *Config) *string { return &c.API.Token })},
	{"SINK_STDOUT_ENABLED", boolean(func(c *Config) *bool { return &c.Sinks.Stdout.Enabled })},
	{"SINK_WEBHOOK_ENABLED", boolean(func(c *Config) *bool { return &c.Sinks.Webhook.Enabled })},
	{"WEBHOOK_URL", str(func(c *Config) *string { return &c.Sinks.Webhook.URL })},
	{"WEBHOOK_SECRET", str(func(c *Config) *string { return &c.Sinks.Webhook.Secret })},
	{"SINK_TELEGRAM_ENABLED", boolean(func(c *Config) *bool { return &c.Sinks.Telegram.Enabled })},
	{"TELEGRAM_BOT_TOKEN", str(func(c *Config) *string { return &c.Sinks.Telegram.BotToken })},
	{"TELEGRAM_CHAT_ID", str(func(c *Config) *string { return &c.Sinks.Telegram.ChatID })},
	{"TELEGRAM_MIN_AMOUNT", str(func(c *Config) *string { return &c.Sinks.Telegram.MinAmount })},
	{"LARGE_ALERTS_ENABLED", boolean(func(c *Config) *bool { return &c.Alerts.LargeTransfers.Enabled })},
	{"LARGE_ALERTS_BOT_TOKEN", str(func(c *Config) *string { return &c.Alerts.LargeTransfers.BotToken })},
	{"LARGE_ALERTS_CHAT_ID", str(func(c *Config) *string { return &c.Alerts.LargeTransfers.ChatID })},
	{"LARGE_ALERTS_THRESHOLD", str(func(c *Config) *string { return &c.Alerts.LargeTransfers.Threshold })},
	{"LARGE_ALERTS_INTERVAL", duration(func(c *Config) *Duration { return &c.Alerts.LargeTransfers.Interval })},
	{"LARGE_ALERTS_FOOTER", str(func(c *Config) *string { return &c.Alerts.LargeTransfers.Footer })},
	{"NOTIFY_BOT_ENABLED", boolean(func(c *Config) *bool { return &c.NotifyBot.Enabled })},
	{"TELEGRAM_NOTIFY_BOT_TOKEN", str(func(c *Config) *string { return &c.NotifyBot.BotToken })},
	{"NOTIFY_FREE_LIMIT", intVal(func(c *Config) *int { return &c.NotifyBot.FreeLimit })},
	{"NOTIFY_PREMIUM_LIMIT", intVal(func(c *Config) *int { return &c.NotifyBot.PremiumLimit })},
	{"NOTIFY_MAX_USERS", intVal(func(c *Config) *int { return &c.NotifyBot.MaxUsers })},
	{"NOTIFY_MAX_TOTAL_SUBS", intVal(func(c *Config) *int { return &c.NotifyBot.MaxTotalSubs })},
	{"NOTIFY_PREMIUM_CONTACT", str(func(c *Config) *string { return &c.NotifyBot.PremiumContact })},
	{"NOTIFY_COMMANDS_PER_MINUTE", intVal(func(c *Config) *int { return &c.NotifyBot.CommandsPerMinute })},
	{"NOTIFY_MIN_AMOUNT", str(func(c *Config) *string { return &c.NotifyBot.MinAmount })},
	{"HTTP_HEALTH_DETAILS", boolean(func(c *Config) *bool { return &c.HTTP.HealthDetails })},
	{"SINK_POSTGRES_ENABLED", boolean(func(c *Config) *bool { return &c.Sinks.Postgres.Enabled })},
	{"DATABASE_URL", str(func(c *Config) *string { return &c.Sinks.Postgres.DSN })},
}

func splitList(v string) []string {
	var out []string
	for _, a := range strings.Split(v, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
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
