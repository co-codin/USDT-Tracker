package app_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth/ethtest"
	"github.com/co-codin/USDT-Tracker/internal/config"
	"github.com/co-codin/USDT-Tracker/internal/tgbot/tgtest"
)

// Notify bot end to end: a user /adds an Ethereum address through the
// (mock) Bot API, then gets a DM for each transfer touching it, while the
// watch list (empty, admin API on) matches nothing.
func TestE2E_NotifyBot(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	sdsn, _ := isolatedSchema(t, dsn)
	node := ethtest.New(2) // safe head 0: nothing is processed before the user subscribes
	defer node.Close()
	tg := tgtest.New("777:notify")
	defer tg.Close()

	cfg := ethCfg(t, baseCfg(t, "http://unused.invalid"), node.URL)
	cfg.Chains = []string{"eth"}
	cfg.ETH.StartBlock = 1
	cfg.API.Token = apiToken
	cfg.Sinks.Stdout.Enabled = false
	cfg.Sinks.Postgres = config.PostgresSink{Enabled: true, DSN: sdsn, CursorName: "main"}
	cfg.NotifyBot = config.NotifyBot{Enabled: true, BotToken: "777:notify", APIBase: tg.URL, FreeLimit: 1, PremiumLimit: 5,
		MinAmount: "0.01", PollTimeout: config.Duration(time.Second)}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a := newApp(t, cfg, nil, false)
	stop := startApp(t, a)
	tg.UserSays(555, "/add "+eth.Checksum(ethtest.Alice))
	tg.UserSays(555, "/add "+ethtest.Bob) // over the free limit of 1
	deadline := time.Now().Add(10 * time.Second)
	for len(tg.SentTo("555")) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // subscription cache refresh
	node.Head.Store(20)
	waitChain(t, a.Listener, 18)
	// Alice appears in every USDT transfer of the mock chain (sender or recipient).
	want := 0
	for b := int64(1); b <= 18; b++ {
		want += int(b % 3)
	}
	count := func() int {
		n := 0
		for _, m := range tg.SentTo("555") {
			n += strings.Count(m.Text, "https://etherscan.io/tx/0x")
		}
		return n
	}
	for count() < want && time.Now().Before(deadline.Add(10*time.Second)) {
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	msgs := tg.SentTo("555")
	if !strings.Contains(msgs[0].Text, "Watching Ethereum") || !strings.Contains(msgs[1].Text, "plan allows 1") {
		t.Fatalf("%q / %q", msgs[0].Text, msgs[1].Text)
	}
	if got := count(); got != want {
		t.Fatalf("notifications %d, want %d", got, want)
	}
	all := ""
	for _, m := range msgs {
		all += m.Text
	}
	if !strings.Contains(all, "received on Ethereum") || !strings.Contains(all, "sent on Ethereum") {
		t.Fatal("missing directions")
	}
}
