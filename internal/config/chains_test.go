package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func loadYAML(t *testing.T, y string, e map[string]string) (Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(p, env(e))
}

const newShape = `
chains:
  tron:
    api_url: http://node:8090
    rps: 7
    confirmations: 19
    batch_size: 9
    watch_addresses: [TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx]
    cursor_file: /tmp/tron.json
  ton:
    api_key: k
    rps: 9
    confirmations: 4
  eth:
    enabled: false
    rpc_url: https://eth.example/v3/x
`

const oldShape = `
chains: [tron, ton]
tron:
  api_url: http://node:8090
  rps: 7
listener:
  confirmations: 19
  batch_size: 9
filter:
  watch_addresses: [TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx]
cursor:
  file: /tmp/tron.json
ton:
  api_key: k
  rps: 9
  confirmations: 4
eth:
  rpc_url: https://eth.example/v3/x
`

// Both shapes must produce the same effective configuration.
func TestChainsNewAndOldShapesAgree(t *testing.T) {
	for name, y := range map[string]string{"new": newShape, "old": oldShape} {
		c, err := loadYAML(t, y, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		ok := strings.Join(c.Chains, ",") == "tron,ton" && c.Tron.APIURL == "http://node:8090" && c.Tron.RPS == 7 &&
			c.Listener.Confirmations == 19 && c.Listener.BatchSize == 9 && len(c.Filter.WatchAddresses) == 1 &&
			c.Cursor.File == "/tmp/tron.json" && c.TON.APIKey == "k" && c.TON.RPS == 9 && c.TON.Confirmations == 4 &&
			c.ETH.RPCURL == "https://eth.example/v3/x" && c.Tron.Timeout.D() == 15*time.Second && c.Token.Symbol == "USDT"
		if !ok {
			t.Fatalf("%s: %+v", name, c)
		}
		if name == "new" && len(c.Deprecated) != 0 {
			t.Fatalf("new shape flagged deprecated: %v", c.Deprecated)
		}
		if name == "old" && len(c.Deprecated) != 2 {
			t.Fatalf("old shape notices: %v", c.Deprecated)
		}
	}
}

func TestChainsEnvStillOverrides(t *testing.T) {
	c, err := loadYAML(t, newShape, map[string]string{"CHAINS": "eth", "ETH_RPS": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(c.Chains, ",") != "eth" || c.ETH.RPS != 2 {
		t.Fatalf("%v %v", c.Chains, c.ETH.RPS)
	}
}

func TestChainsShapeErrors(t *testing.T) {
	cases := map[string]string{
		"unknown chain":   "chains:\n  sol: {}\n",
		"unknown key":     "chains:\n  ton:\n    rpss: 3\n",
		"tron unknown":    "chains:\n  tron:\n    nope: 1\n",
		"mixed shapes":    "chains:\n  ton: {}\nton:\n  rps: 2\n",
		"all disabled":    "chains:\n  tron: {enabled: false}\n",
		"bad enabled":     "chains:\n  tron: {enabled: maybe}\n",
		"scalar chains":   "chains: tron\n",
		"scalar block":    "chains:\n  tron: 5\n",
		"bad duration":    "chains:\n  eth:\n    poll_interval: soon\n",
		"validate per ch": "chains:\n  eth:\n    contract: nope\n",
	}
	for name, y := range cases {
		if _, err := loadYAML(t, y, nil); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	// Empty block = enabled with defaults; legacy blocks without chains are flagged.
	c, err := loadYAML(t, "chains:\n  ton:\n", nil)
	if err != nil || strings.Join(c.Chains, ",") != "ton" {
		t.Fatal(c.Chains, err)
	}
	c, err = loadYAML(t, "ton:\n  rps: 2\n", nil)
	if err != nil || len(c.Deprecated) != 1 || c.TON.RPS != 2 {
		t.Fatal(c.Deprecated, err)
	}
	if got := strings.Join(KnownChains(), ","); got != "tron,ton,eth" {
		t.Fatal(got)
	}
}
