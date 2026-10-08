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
