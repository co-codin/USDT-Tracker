package alert_test

import (
	"context"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/alert"
	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
	"github.com/co-codin/USDT-Tracker/internal/tgbot"
	"github.com/co-codin/USDT-Tracker/internal/tgbot/tgtest"
)

func usdt(chainName string, whole int64, i int) model.Transfer {
	tx := strings.Repeat("ab", 32)
	from, to := "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv", "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"
	switch chainName {
	case model.ChainETH:
		tx = "0x" + tx
		from, to = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed", "0xfb6916095ca1df60bb79ce92ce3ea74c37c5d359"
	case model.ChainTON:
		from, to = "0:9db2cae853cf2db870ea06d1e8286f202cd9ad689dfc74b98195520d06495859", "0:852443f8599fe6a5da34fe43049ac4e0beb3071bb2bfb56635ea9421287c283a"
	}
	return model.Transfer{Chain: chainName, TxID: tx, LogIndex: i, Symbol: "USDT", Decimals: 6,
		Amount: new(big.Int).Mul(big.NewInt(whole), big.NewInt(1_000_000)), From: from, To: to,
		BlockTime: time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)}
}

type rec struct {
	mu   sync.Mutex
	msgs []string
}

func (r *rec) SendMessage(_ context.Context, chatID, text string, _ tgbot.SendOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, chatID+"|"+text)
	return nil
}

func TestThresholdDedupeAndQueue(t *testing.T) {
	r := &rec{}
	a, err := alert.New(alert.Config{ChatID: "@w", Threshold: "100000", QueueSize: 3}, r, nil)
	if err != nil {
		t.Fatal(err)
	}
	b := chain.Block{Number: 1, Transfers: []model.Transfer{
		usdt("tron", 99_999, 0), usdt("tron", 100_000, 1), usdt("eth", 250_000, 2), usdt("ton", 5, 3),
	}}
	a.Observe(context.Background(), b)
	a.Observe(context.Background(), b) // same block again (restart): deduped
	if a.Pending() != 2 {
		t.Fatalf("pending %d", a.Pending())
	}
	// 18-decimals token: threshold compares in its own units.
	big18 := model.Transfer{Chain: "eth", TxID: "0x01", Symbol: "X", Decimals: 18, Amount: new(big.Int).Exp(big.NewInt(10), big.NewInt(23), nil)} // 100000
	a.Observe(context.Background(), chain.Block{Transfers: []model.Transfer{big18, usdt("tron", 200_000, 9), usdt("tron", 300_000, 10)}})
	st := a.Stats()
	if a.Pending() != 3 || st.Dropped != 2 || st.Queued != 5 {
		t.Fatalf("pending %d stats %+v", a.Pending(), st)
	}
}

func TestRunBatchesAndPaces(t *testing.T) {
	srv := tgtest.New("tok")
	defer srv.Close()
	bot, _ := tgbot.New(tgbot.Options{Token: "tok", APIBase: srv.URL, Backoff: retry.Backoff{Initial: time.Millisecond, Max: time.Millisecond}})
	a, _ := alert.New(alert.Config{ChatID: "@whales", Interval: 150 * time.Millisecond, MaxPerMessage: 3, Footer: `via <a href="https://github.com/co-codin/USDT-Tracker">USDT-Tracker</a>`}, bot, nil)
	var ts []model.Transfer
	for i := range 7 {
		ts = append(ts, usdt([]string{"tron", "ton", "eth"}[i%3], int64(100_000+i*1000), i))
	}
	a.Observe(context.Background(), chain.Block{Transfers: ts})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx, time.Second) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(srv.Sent()) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	sent := srv.Sent()
	if len(sent) != 3 { // 3 + 3 + 1
		t.Fatalf("messages %d", len(sent))
	}
	for i := 1; i < len(sent); i++ {
		if gap := sent[i].At.Sub(sent[i-1].At); gap < 140*time.Millisecond {
			t.Fatalf("messages %d and %d only %v apart", i-1, i, gap)
		}
	}
	if !strings.Contains(sent[0].Text, "3 large USDT transfers") || !strings.Contains(sent[0].Text, "USDT-Tracker</a>") {
		t.Fatalf("batch text: %s", sent[0].Text)
	}
	if st := a.Stats(); st.Sent != 7 || st.Messages != 3 {
		t.Fatalf("%+v", st)
	}
}

func TestFlushOnShutdownAnd429(t *testing.T) {
	srv := tgtest.New("tok")
	defer srv.Close()
	srv.Fail429(1)
	bot, _ := tgbot.New(tgbot.Options{Token: "tok", APIBase: srv.URL, Backoff: retry.Backoff{Initial: time.Millisecond, Max: time.Millisecond}})
	a, _ := alert.New(alert.Config{ChatID: "@w", Interval: 10 * time.Millisecond}, bot, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a.Observe(context.Background(), chain.Block{Transfers: []model.Transfer{usdt("eth", 1_000_000, 0)}})
	a.Run(ctx, 5*time.Second) // ctx already done: flushes the pending alert, retrying the 429
	if len(srv.Sent()) != 1 {
		t.Fatalf("sent %d", len(srv.Sent()))
	}
}

func TestFormatSingleLinksPerChain(t *testing.T) {
	cases := map[string][]string{
		"tron": {"https://tronscan.org/#/transaction/" + strings.Repeat("ab", 32), "Tronscan", "on TRON", "https://tronscan.org/#/address/TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"},
		"ton":  {"https://tonviewer.com/transaction/", "Tonviewer", "on TON", "https://tonviewer.com/UQ"},
		"eth":  {"https://etherscan.io/tx/0x" + strings.Repeat("ab", 32), "Etherscan", "on Ethereum", "https://etherscan.io/address/0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"},
	}
	for c, want := range cases {
		tr := usdt(c, 1_234_567, 0)
		tr.Memo = "<script>"
		msg := alert.Format([]model.Transfer{tr}, "")
		for _, w := range append(want, "1,234,567 USDT", "&lt;script&gt;", "2026-10-12 10:00 UTC") {
			if !strings.Contains(msg, w) {
				t.Errorf("%s: missing %q in\n%s", c, w, msg)
			}
		}
	}
}

func TestFormatBatchFitsTelegram(t *testing.T) {
	var ts []model.Transfer
	for i := range 30 {
		ts = append(ts, usdt("eth", int64(100_000+i), i))
	}
	msg := alert.Format(ts, "footer")
	if !strings.HasPrefix(msg, "🐋 <b>30 large USDT transfers</b>") || !strings.HasSuffix(msg, "footer") {
		t.Fatal(msg)
	}
	// largest first
	if strings.Index(msg, "100,029 USDT") > strings.Index(msg, "100,000 USDT") {
		t.Fatal("not sorted by amount")
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := alert.New(alert.Config{}, &rec{}, nil); err == nil {
		t.Fatal("missing chat accepted")
	}
	if _, err := alert.New(alert.Config{ChatID: "@x", Threshold: "abc"}, &rec{}, nil); err == nil {
		t.Fatal("bad threshold accepted")
	}
}
