package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestDefaultsAreValid(t *testing.T) {
	cfg, err := Load("", env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token.Contract != USDTContract || cfg.Token.Decimals != 6 || !cfg.Sinks.Stdout.Enabled {
		t.Fatalf("unexpected defaults %+v", cfg)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	if _, err := Load(filepath.Join("..", "..", "config.example.yaml"), env(nil)); err != nil {
		t.Fatalf("config.example.yaml must be valid: %v", err)
	}
}

func TestYAMLAndEnvOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	yaml := `
tron:
  timeout: 7s
listener:
  confirmations: 30
  poll_interval: 1500ms
filter:
  min_amount: "50000"
  watch_addresses: [TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx]
sinks:
  webhook:
    enabled: true
    url: https://example.invalid/hook
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, env(map[string]string{
		"LISTENER_CONFIRMATIONS": "19",
		"TRON_PRO_API_KEY":       "k",
		"WEBHOOK_SECRET":         "s",
		"FILTER_WATCH_ADDRESSES": "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv, TEHwL3F2kpkJExYAZYziHGLFR2CR8Bq3bo",
		"TELEGRAM_CHAT_ID":       "", // empty must not override
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Tron.Timeout.D() != 7*time.Second || cfg.Listener.PollInterval.D() != 1500*time.Millisecond {
		t.Fatal("durations not parsed")
	}
	if cfg.Listener.Confirmations != 19 || cfg.Tron.APIKey != "k" || cfg.Sinks.Webhook.Secret != "s" {
		t.Fatal("env overrides not applied")
	}
	if len(cfg.Filter.WatchAddresses) != 2 {
		t.Fatalf("watch addresses = %v", cfg.Filter.WatchAddresses)
	}
	minAmt, _ := cfg.MinAmount()
	if minAmt.String() != "50000000000" {
		t.Fatalf("min amount = %s", minAmt)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := map[string]map[string]string{
		"telegram": {"SINK_TELEGRAM_ENABLED": "true"},
		"postgres": {"SINK_POSTGRES_ENABLED": "true"},
		"webhook":  {"SINK_WEBHOOK_ENABLED": "true"},
		"address":  {"FILTER_WATCH_ADDRESSES": "Tnope"},
		"amount":   {"FILTER_MIN_AMOUNT": "1.0000001"},
		"no sinks": {"SINK_STDOUT_ENABLED": "false"},
		"bad bool": {"SINK_STDOUT_ENABLED": "maybe"},
	}
	for name, e := range cases {
		if _, err := Load("", env(e)); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestAPITokenValidation(t *testing.T) {
	good := "0123456789abcdef0123456789abcdef"
	if c, err := Load("", env(map[string]string{"API_TOKEN": good})); err != nil || c.API.Token != good {
		t.Fatalf("valid token rejected: %v", err)
	}
	if c, _ := Load("", env(nil)); c.API.Token != "" || c.Filter.ReloadInterval.D() != 5*time.Second {
		t.Fatalf("defaults: token=%q reload=%s", c.API.Token, c.Filter.ReloadInterval.D())
	}
	for name, e := range map[string]map[string]string{
		"too short":  {"API_TOKEN": "abc"},
		"whitespace": {"API_TOKEN": "0123456789 abcdef0123"},
	} {
		if _, err := Load("", env(e)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	path := filepath.Join(t.TempDir(), "c.yaml")
	_ = os.WriteFile(path, []byte("http:\n  addr: \"\"\napi:\n  token: "+good+"\n"), 0o600)
	if _, err := Load(path, env(nil)); err == nil || !strings.Contains(err.Error(), "http.addr") {
		t.Fatalf("token without http.addr must be rejected, got %v", err)
	}
}

func TestUnknownYAMLKeyRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	_ = os.WriteFile(path, []byte("listener:\n  confirmatoins: 3\n"), 0o600)
	_, err := Load(path, env(nil))
	if err == nil || !strings.Contains(err.Error(), "confirmatoins") {
		t.Fatalf("typo should be reported, got %v", err)
	}
}

func TestEveryEnvVarIsApplied(t *testing.T) {
	e := map[string]string{
		"LOG_LEVEL": "debug", "LOG_FORMAT": "text",
		"TRON_API_URL": "http://node:8090", "TRON_PRO_API_KEY": "key", "TRON_RPS": "7.5",
		"TOKEN_CONTRACT":         "41a614f803b6fd780986a42c78ec9c7f77e6ded13c",
		"LISTENER_CONFIRMATIONS": "3", "LISTENER_START_BLOCK": "-50", "LISTENER_POLL_INTERVAL": "2s",
		"LISTENER_BATCH_SIZE": "9", "LISTENER_CONCURRENCY": "4", "CURSOR_FILE": "/tmp/c.json",
		"FILTER_WATCH_ADDRESSES": "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx", "FILTER_DIRECTION": "outgoing",
		"FILTER_MIN_AMOUNT": "1000.5", "FILTER_MODE": "all", "FILTER_RELOAD_INTERVAL": "750ms", "HTTP_ADDR": ":9999",
		"API_TOKEN":            "0123456789abcdef0123",
		"SINK_STDOUT_ENABLED":  "false",
		"SINK_WEBHOOK_ENABLED": "true", "WEBHOOK_URL": "https://example.invalid/h", "WEBHOOK_SECRET": "ws",
		"SINK_TELEGRAM_ENABLED": "1", "TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c", "TELEGRAM_MIN_AMOUNT": "5",
		"SINK_POSTGRES_ENABLED": "true", "DATABASE_URL": "postgres://x",
		"CHAINS": "TRON, ton", "TON_API_URL": "https://ton.example/api/v3", "TONCENTER_API_KEY": "tk", "TON_RPS": "9",
		"TON_JETTON_MASTER":   "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe",
		"TON_WATCH_ADDRESSES": "UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg, 0:852443f8599fe6a5da34fe43049ac4e0beb3071bb2bfb56635ea9421287c283a",
		"TON_CONFIRMATIONS":   "5", "TON_START_BLOCK": "-10", "TON_POLL_INTERVAL": "500ms", "TON_CURSOR_FILE": "/tmp/t.json",
		"ETH_RPC_URL": "https://eth.example/v3/key", "ETH_RPS": "4", "ETH_CONTRACT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
		"ETH_WATCH_ADDRESSES": "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed", "ETH_CONFIRMATIONS": "6", "ETH_START_BLOCK": "-3",
		"ETH_POLL_INTERVAL": "4s", "ETH_BATCH_SIZE": "20", "ETH_LOG_CHUNK": "10", "ETH_HEAD_TAG": "finalized", "ETH_CURSOR_FILE": "/tmp/e.json",
		"LARGE_ALERTS_ENABLED": "true", "LARGE_ALERTS_BOT_TOKEN": "lb", "LARGE_ALERTS_CHAT_ID": "@whales",
		"LARGE_ALERTS_THRESHOLD": "250000", "LARGE_ALERTS_INTERVAL": "4s", "LARGE_ALERTS_FOOTER": "via x",
		"NOTIFY_BOT_ENABLED": "true", "TELEGRAM_NOTIFY_BOT_TOKEN": "nb", "NOTIFY_FREE_LIMIT": "2", "NOTIFY_PREMIUM_LIMIT": "20",
		"NOTIFY_MIN_AMOUNT": "1",
	}
	if len(e) != len(EnvVars()) {
		t.Fatalf("test covers %d env vars, config supports %d", len(e), len(EnvVars()))
	}
	c, err := Load("", env(e))
	if err != nil {
		t.Fatal(err)
	}
	checks := []bool{
		c.Log.Level == "debug", c.Log.Format == "text",
		c.Tron.APIURL == "http://node:8090", c.Tron.APIKey == "key", c.Tron.RPS == 7.5,
		c.Token.Contract == "41a614f803b6fd780986a42c78ec9c7f77e6ded13c",
		c.Listener.Confirmations == 3, c.Listener.StartBlock == -50, c.Listener.PollInterval.D() == 2*time.Second,
		c.Listener.BatchSize == 9, c.Listener.Concurrency == 4, c.Cursor.File == "/tmp/c.json",
		len(c.Filter.WatchAddresses) == 1, c.Filter.Direction == "outgoing", c.Filter.MinAmount == "1000.5",
		c.Filter.Mode == "all", c.Filter.ReloadInterval.D() == 750*time.Millisecond, c.HTTP.Addr == ":9999",
		c.API.Token == "0123456789abcdef0123", !c.Sinks.Stdout.Enabled,
		c.Sinks.Webhook.Enabled, c.Sinks.Webhook.URL != "", c.Sinks.Webhook.Secret == "ws",
		c.Sinks.Telegram.Enabled, c.Sinks.Telegram.BotToken == "t", c.Sinks.Telegram.ChatID == "c", c.Sinks.Telegram.MinAmount == "5",
		c.Sinks.Postgres.Enabled, c.Sinks.Postgres.DSN == "postgres://x",
		len(c.Chains) == 2 && c.Chains[0] == "tron" && c.Chains[1] == "ton", c.TON.APIURL == "https://ton.example/api/v3",
		c.TON.APIKey == "tk", c.TON.RPS == 9, c.TON.JettonMaster != "", len(c.TON.WatchAddresses) == 2,
		c.TON.Confirmations == 5, c.TON.StartBlock == -10, c.TON.PollInterval.D() == 500*time.Millisecond, c.TON.CursorFile == "/tmp/t.json",
		c.ETH.RPCURL == "https://eth.example/v3/key", c.ETH.RPS == 4, c.ETH.Contract != "", len(c.ETH.WatchAddresses) == 1,
		c.ETH.Confirmations == 6, c.ETH.StartBlock == -3, c.ETH.PollInterval.D() == 4*time.Second, c.ETH.BatchSize == 20,
		c.ETH.LogChunk == 10, c.ETH.HeadTag == "finalized", c.ETH.CursorFile == "/tmp/e.json",
		c.Alerts.LargeTransfers.Enabled, c.Alerts.LargeTransfers.BotToken == "lb", c.Alerts.LargeTransfers.ChatID == "@whales",
		c.Alerts.LargeTransfers.Threshold == "250000", c.Alerts.LargeTransfers.Interval.D() == 4*time.Second,
		c.Alerts.LargeTransfers.Footer == "via x",
		c.NotifyBot.Enabled, c.NotifyBot.BotToken == "nb", c.NotifyBot.FreeLimit == 2, c.NotifyBot.PremiumLimit == 20,
		c.NotifyBot.MinAmount == "1",
	}
	for i, ok := range checks {
		if !ok {
			t.Errorf("check %d failed: %+v", i, c)
		}
	}
}

func TestInvalidEnvValues(t *testing.T) {
	for k, v := range map[string]string{
		"TRON_RPS": "fast", "LISTENER_CONFIRMATIONS": "x", "LISTENER_BATCH_SIZE": "x",
		"LISTENER_POLL_INTERVAL": "3 parsecs", "LISTENER_CONCURRENCY": "0", "TOKEN_CONTRACT": "nope",
		"FILTER_DIRECTION": "up", "FILTER_MODE": "most", "TELEGRAM_MIN_AMOUNT": "x",
		"FILTER_RELOAD_INTERVAL": "10ms", "API_TOKEN": "short",
	} {
		if _, err := Load("", env(map[string]string{k: v})); err == nil {
			t.Errorf("%s=%s: expected error", k, v)
		}
	}
	path := filepath.Join(t.TempDir(), "bad.yaml")
	_ = os.WriteFile(path, []byte("listener:\n  poll_interval: soon\n"), 0o600)
	if _, err := Load(path, env(nil)); err == nil {
		t.Error("invalid duration in YAML: expected error")
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml"), env(nil)); err == nil {
		t.Error("missing explicit config file: expected error")
	}
}

func TestTONConfig(t *testing.T) {
	c, err := Load("", env(nil))
	if err != nil || len(c.Chains) != 1 || c.Chains[0] != "tron" || c.HasChain("ton") {
		t.Fatalf("default chains %v %v", c.Chains, err)
	}
	if c.TON.JettonMaster != "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs" || c.TON.Decimals != 6 || c.TON.APIURL != "https://toncenter.com/api/v3" {
		t.Fatalf("ton defaults %+v", c.TON)
	}
	c, err = Load("", env(map[string]string{"CHAINS": "ton", "TON_WATCH_ADDRESSES": "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs"}))
	if err != nil || !c.HasChain("ton") || c.HasChain("tron") {
		t.Fatalf("ton only: %v %v", c.Chains, err)
	}
	path := filepath.Join(t.TempDir(), "c.yaml")
	_ = os.WriteFile(path, []byte("chains: [tron, ton]\nton:\n  watch_addresses: [\"0:852443f8599fe6a5da34fe43049ac4e0beb3071bb2bfb56635ea9421287c283a\"]\n  confirmations: 1\n"), 0o600)
	if c, err := Load(path, env(nil)); err != nil || len(c.Chains) != 2 || c.TON.Confirmations != 1 {
		t.Fatalf("yaml: %+v %v", c.Chains, err)
	}
	for name, e := range map[string]map[string]string{
		"unknown chain":       {"CHAINS": "sol"},
		"eth bad rpc":         {"CHAINS": "eth", "ETH_RPC_URL": "ftp://x"},
		"eth bad watch":       {"CHAINS": "eth", "ETH_WATCH_ADDRESSES": "0xF4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7F07"},
		"eth same cursor":     {"CHAINS": "tron,eth", "CURSOR_FILE": "/tmp/x.json", "ETH_CURSOR_FILE": "/tmp/x.json"},
		"eth chunk":           {"CHAINS": "eth", "ETH_LOG_CHUNK": "0"},
		"eth head tag":        {"CHAINS": "eth", "ETH_HEAD_TAG": "pending"},
		"notify no token":     {"NOTIFY_BOT_ENABLED": "true", "SINK_POSTGRES_ENABLED": "true", "DATABASE_URL": "postgres://x"},
		"notify no postgres":  {"NOTIFY_BOT_ENABLED": "true", "TELEGRAM_NOTIFY_BOT_TOKEN": "t"},
		"notify limits":       {"NOTIFY_BOT_ENABLED": "true", "TELEGRAM_NOTIFY_BOT_TOKEN": "t", "SINK_POSTGRES_ENABLED": "true", "DATABASE_URL": "postgres://x", "NOTIFY_FREE_LIMIT": "5", "NOTIFY_PREMIUM_LIMIT": "2"},
		"alerts no chat":      {"LARGE_ALERTS_ENABLED": "true", "LARGE_ALERTS_BOT_TOKEN": "x"},
		"alerts no token":     {"LARGE_ALERTS_ENABLED": "true", "LARGE_ALERTS_CHAT_ID": "@x"},
		"alerts threshold":    {"LARGE_ALERTS_ENABLED": "true", "LARGE_ALERTS_CHAT_ID": "@x", "TELEGRAM_BOT_TOKEN": "t", "LARGE_ALERTS_THRESHOLD": "0"},
		"alerts interval":     {"LARGE_ALERTS_ENABLED": "true", "LARGE_ALERTS_CHAT_ID": "@x", "TELEGRAM_BOT_TOKEN": "t", "LARGE_ALERTS_INTERVAL": "10ms"},
		"duplicate chain":     {"CHAINS": "ton,ton"},
		"bad ton watch":       {"CHAINS": "ton", "TON_WATCH_ADDRESSES": "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"},
		"bad master":          {"CHAINS": "ton", "TON_JETTON_MASTER": "nope"},
		"ton rps":             {"CHAINS": "ton", "TON_RPS": "0"},
		"ton in tron list":    {"FILTER_WATCH_ADDRESSES": "UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg"},
		"same cursor file":    {"CHAINS": "tron,ton", "CURSOR_FILE": "/tmp/x.json", "TON_CURSOR_FILE": "/tmp/x.json"},
		"ton min too precise": {"CHAINS": "ton", "FILTER_MIN_AMOUNT": "0.0000001"},
	} {
		_, err := Load("", env(e))
		if err == nil {
			t.Errorf("%s: expected error", name)
		}
		if name == "ton in tron list" && err != nil && !strings.Contains(err.Error(), "ton.watch_addresses") {
			t.Errorf("hint missing: %v", err)
		}
	}
	// TON settings are not validated while TON is disabled
	if _, err := Load("", env(map[string]string{"TON_RPS": "0"})); err != nil {
		t.Errorf("disabled ton validated: %v", err)
	}
}

func TestLargeAlertsTokenFallback(t *testing.T) {
	c, err := Load("", env(map[string]string{"LARGE_ALERTS_ENABLED": "true", "LARGE_ALERTS_CHAT_ID": "@x", "TELEGRAM_BOT_TOKEN": "shared"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Alerts.LargeTransfers.Token(&c) != "shared" || c.Alerts.LargeTransfers.Threshold != "100000" {
		t.Fatalf("%+v", c.Alerts.LargeTransfers)
	}
}
