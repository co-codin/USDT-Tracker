package app_test

import (
	"strings"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/eth/ethtest"
	"github.com/co-codin/USDT-Tracker/internal/config"
	"github.com/co-codin/USDT-Tracker/internal/tgbot/tgtest"
)

// Large-transfer channel: alerts are independent of the watch list (which
// matches nothing here) and link to etherscan; Telegram 429 is retried.
func TestE2E_LargeTransferChannel(t *testing.T) {
	node := ethtest.New(60)
	defer node.Close()
	tg := tgtest.New("999:alerts")
	defer tg.Close()
	tg.Fail429(1)

	cfg := ethCfg(t, baseCfg(t, "http://unused.invalid"), node.URL)
	cfg.Chains = []string{"eth"}
	cfg.ETH.StartBlock = 40
	cfg.ETH.WatchAddresses = []string{"0x" + strings.Repeat("99", 20)}
	cfg.Alerts.LargeTransfers = config.LargeTransferAlerts{
		Enabled: true, BotToken: "999:alerts", ChatID: "@usdt_whales", APIBase: tg.URL,
		Threshold: "500", Interval: config.Duration(500 * time.Millisecond), MaxPerMessage: 30, QueueSize: 100,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	out := &syncBuffer{}
	a := newApp(t, cfg, out, false)
	stop := startApp(t, a)
	waitChain(t, a.Listener, 58)
	want := 0
	for b := int64(40); b <= 58; b++ {
		for i := 0; i < int(b%3); i++ {
			if b*10+int64(i)+1 >= 500 {
				want++
			}
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	count := func() int {
		n := 0
		for _, m := range tg.SentTo("@usdt_whales") {
			n += strings.Count(m.Text, "https://etherscan.io/tx/0x")
		}
		return n
	}
	for count() < want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	if got := count(); got != want || want == 0 {
		t.Fatalf("alerted %d transfers, want %d", got, want)
	}
	if len(out.lines(t)) != 0 {
		t.Fatal("watch list matched nothing, stdout must be empty")
	}
	if a.Alerts.Stats().Failed != 0 {
		t.Fatalf("%+v", a.Alerts.Stats())
	}
}
