package notify

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
	"github.com/co-codin/USDT-Tracker/internal/tgbot"
	"github.com/co-codin/USDT-Tracker/internal/tgbot/tgtest"
)

const (
	tronA = "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"
	tronB = "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"
	ethA  = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"
	tonF  = "UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg"
	tonR  = "0:9db2cae853cf2db870ea06d1e8286f202cd9ad689dfc74b98195520d06495859"
)

func newBot(t *testing.T, cfg Config) (*Bot, *MemoryStore, *tgtest.Server) {
	t.Helper()
	srv := tgtest.New("42:notify")
	t.Cleanup(srv.Close)
	api, err := tgbot.New(tgbot.Options{Token: "42:notify", APIBase: srv.URL, Backoff: retry.Backoff{Initial: time.Millisecond, Max: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	st := NewMemoryStore()
	cfg.PollTimeout, cfg.RefreshInterval = time.Second, 20*time.Millisecond
	b, err := New(cfg, st, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b, st, srv
}

func msg(chat int64, text string) *tgbot.Message {
	m := &tgbot.Message{Text: text}
	m.Chat.ID = chat
	return m
}

func TestCommands(t *testing.T) {
	b, st, _ := newBot(t, Config{FreeLimit: 3, PremiumLimit: 5})
	ctx := context.Background()
	say := func(text string) string { return b.Handle(ctx, msg(7, text)) }
	if r := say("/start"); !strings.Contains(r, "/add") || !strings.Contains(r, "up to 3 addresses") {
		t.Fatal(r)
	}
	if r := say("/add@MyNotifyBot " + tronA); !strings.Contains(r, "Watching TRON") {
		t.Fatal(r)
	}
	if r := say("/add " + tonF); !strings.Contains(r, "Watching TON") || !strings.Contains(r, tonF) {
		t.Fatal(r)
	}
	if r := say("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"); !strings.Contains(r, "Watching Ethereum") { // bare address = /add
		t.Fatal(r)
	}
	if r := say("/add " + tronA); !strings.Contains(r, "Already") {
		t.Fatal(r)
	}
	if r := say("/add " + tronB); !strings.Contains(r, "allows 3 addresses") || !strings.Contains(r, "premium") {
		t.Fatal(r)
	}
	if r := say("/add nope"); !strings.Contains(r, "unrecognised") {
		t.Fatal(r)
	}
	if r := say("/add 0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAeD"); !strings.Contains(r, "❌") {
		t.Fatal("bad checksum accepted: " + r)
	}
	r := say("/list")
	if !strings.Contains(r, "(3/3, free plan)") || !strings.Contains(r, "1. TRON") || !strings.Contains(r, "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed") {
		t.Fatal(r)
	}
	if r := say("/remove 2"); !strings.Contains(r, "Stopped watching") {
		t.Fatal(r)
	}
	if r := say("/remove " + tonF); !strings.Contains(r, "not watching") {
		t.Fatal(r)
	}
	if r := say("/remove 9"); !strings.Contains(r, "No such number") {
		t.Fatal(r)
	}
	// premium hook: plan field raises the limit
	st.SetPlan(7, PlanPremium)
	if r := say("/add " + tronB); !strings.Contains(r, "Watching") {
		t.Fatal(r)
	}
	if r := say("/plan"); !strings.Contains(r, "premium") || !strings.Contains(r, "3/5") {
		t.Fatal(r)
	}
	if r := say("/whatever"); !strings.Contains(r, "Unknown") {
		t.Fatal(r)
	}
	if r := say("hello"); !strings.Contains(r, "/help") {
		t.Fatal(r)
	}
	// other users are isolated
	if r := b.Handle(ctx, msg(8, "/list")); !strings.Contains(r, "not watching any") {
		t.Fatal(r)
	}
}

func TestChainsRestriction(t *testing.T) {
	b, _, _ := newBot(t, Config{Chains: []string{model.ChainTron}})
	if r := b.Handle(context.Background(), msg(1, "/add "+ethA)); !strings.Contains(r, "not supported") {
		t.Fatal(r)
	}
}

func usdt(c, from, to string, raw int64, i int) model.Transfer {
	return model.Transfer{Chain: c, TxID: strings.Repeat("ab", 32), LogIndex: i, Symbol: "USDT", Decimals: 6,
		From: from, To: to, Amount: big.NewInt(raw), BlockTime: time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)}
}

func TestDispatchPollingAndBlocked(t *testing.T) {
	b, st, srv := newBot(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = b.Run(ctx) }()

	// Commands arrive through long polling.
	srv.UserSays(100, "/add "+tronA)
	srv.UserSays(200, "/add "+tonF)
	srv.UserSays(300, "/add "+tronA)
	waitFor(t, func() bool {
		subs, _ := st.ActiveSubs(context.Background())
		return len(subs) == 3 && len(*b.index.Load()) == 2
	})
	waitFor(t, func() bool { off, _ := st.LoadOffset(context.Background()); return off == 4 })
	srv.Forbid("300") // user 300 blocked the bot
	b.Observe(ctx, chain.Block{Transfers: []model.Transfer{
		usdt("tron", tronB, tronA, 15_000_000, 0), // in for 100 and 300
		usdt("tron", tronA, tronB, 2_500_000, 1),  // out for 100 and 300
		usdt("tron", tronB, tronA, 0, 2),          // zero-value poisoning: skipped
		usdt("ton", tronB, tonR, 1_000_000, 3),    // in for 200 (memo below)
		usdt("eth", ethA, ethA, 9_000_000, 4),     // nobody
	}})
	b.Observe(ctx, chain.Block{Transfers: []model.Transfer{usdt("tron", tronB, tronA, 15_000_000, 0)}}) // replay: deduped
	count := func(chat string) int {
		n := 0
		for _, m := range srv.SentTo(chat) {
			n += strings.Count(m.Text, "View transaction")
		}
		return n
	}
	waitFor(t, func() bool { return count("100") >= 2 && count("200") >= 1 })
	time.Sleep(50 * time.Millisecond)
	notes100 := count("100")
	// 100 got its /add reply + the 2 notifications (merged in one or two messages)
	if notes100 != 2 {
		t.Fatalf("user 100 got %d notifications: %+v", notes100, srv.SentTo("100"))
	}
	all := ""
	for _, m := range srv.SentTo("100") {
		all += m.Text
	}
	if !strings.Contains(all, "📥 <b>+15 USDT</b> received on TRON") || !strings.Contains(all, "📤 <b>−2.5 USDT</b> sent on TRON") ||
		!strings.Contains(all, "https://tronscan.org/#/transaction/") {
		t.Fatal(all)
	}
	ton := srv.SentTo("200")
	if !strings.Contains(ton[len(ton)-1].Text, "received on TON") || !strings.Contains(ton[len(ton)-1].Text, tonF) {
		t.Fatal(ton[len(ton)-1].Text)
	}
	waitFor(t, func() bool {
		subs, _ := st.ActiveSubs(context.Background())
		return len(subs) == 2 // 300 is paused after 403
	})
	// /start again un-blocks
	b.Handle(ctx, msg(300, "/start"))
	if subs, _ := st.ActiveSubs(context.Background()); len(subs) != 3 {
		t.Fatal("unblock failed")
	}
	cancel()
	<-done
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFormatMemoEscaping(t *testing.T) {
	tr := usdt("ton", tonR, tonR, 1_000_000, 0)
	tr.Memo = "<b>x</b>"
	s := format(LangEN, []note{{t: tr, dir: "self"}})
	if !strings.Contains(s, "self-transfer on TON") || !strings.Contains(s, "&lt;b&gt;x&lt;/b&gt;") {
		t.Fatal(s)
	}
}

func TestReadOnlyCommandsDoNotCreateUsers(t *testing.T) {
	b, st, _ := newBot(t, Config{})
	ctx := context.Background()
	for _, c := range []string{"/start", "/help", "/list", "/plan", "/remove 1", "hello"} {
		b.Handle(ctx, msg(55, c))
	}
	if _, ok, _ := st.GetUser(ctx, 55); ok {
		t.Fatal("read-only commands created a user row")
	}
	b.Handle(ctx, msg(55, "/add "+tronA))
	if _, ok, _ := st.GetUser(ctx, 55); !ok {
		t.Fatal("/add did not create the user")
	}
}

func TestGroupChatsAreRefused(t *testing.T) {
	b, st, _ := newBot(t, Config{})
	ctx := context.Background()
	m := msg(-100123, "/add "+tronA)
	m.Chat.Type = "supergroup"
	if r := b.Handle(ctx, m); !strings.Contains(r, "private chat") {
		t.Fatal(r)
	}
	m.Text = "/remove 1"
	if r := b.Handle(ctx, m); r != "" {
		t.Fatal(r)
	}
	if subs, _ := st.ActiveSubs(ctx); len(subs) != 0 {
		t.Fatal("group subscription created")
	}
}

func TestBotCapacity(t *testing.T) {
	b, _, _ := newBot(t, Config{MaxUsers: 1, MaxTotalSubs: 2})
	ctx := context.Background()
	if r := b.Handle(ctx, msg(1, "/add "+tronA)); !strings.Contains(r, "Watching") {
		t.Fatal(r)
	}
	if r := b.Handle(ctx, msg(2, "/add "+tronA)); !strings.Contains(r, "capacity") {
		t.Fatal(r)
	}
	b.Handle(ctx, msg(1, "/add "+tronB))
	if r := b.Handle(ctx, msg(1, "/add "+ethA)); !strings.Contains(r, "capacity") {
		t.Fatal(r)
	}
}

func TestPerChatRateLimit(t *testing.T) {
	b, _, srv := newBot(t, Config{CommandsPerMinute: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = b.Run(ctx) }()
	for range 12 {
		srv.UserSays(77, "/help")
	}
	srv.UserSays(78, "/help")
	waitFor(t, func() bool { return len(srv.SentTo("78")) == 1 && b.RateLimited() >= 7 })
	if n := len(srv.SentTo("77")); n != 5 { // burst 5, then dropped
		t.Fatalf("chat 77 got %d replies", n)
	}
	cancel()
	<-done
}

func TestLanguages(t *testing.T) {
	b, _, _ := newBot(t, Config{FreeLimit: 3, PremiumContact: "@owner"})
	ctx := context.Background()
	say := func(lang, text string) string {
		m := msg(9, text)
		m.From = &struct {
			ID           int64  `json:"id"`
			Username     string `json:"username"`
			LanguageCode string `json:"language_code"`
		}{ID: 9, LanguageCode: lang}
		return b.Handle(ctx, m)
	}
	if r := say("zh-hans", "/start"); !strings.Contains(r, "到账通知") || !strings.Contains(r, ProjectURL) || !strings.Contains(r, "最多 3 个地址") {
		t.Fatal(r)
	}
	if r := say("ru", "/add "+tronA); !strings.Contains(r, "Отслеживаю адрес TRON") {
		t.Fatal(r)
	}
	if r := say("ru", "/plan"); !strings.Contains(r, "Тариф") || !strings.Contains(r, "@owner") {
		t.Fatal(r)
	}
	if r := say("de", "/help"); !strings.Contains(r, "/add &lt;address&gt;") {
		t.Fatal(r)
	}
	if r := say("zh", "/add nonsense"); !strings.Contains(r, "无法识别") {
		t.Fatal(r)
	}
	tr0 := usdt("tron", tronB, tronA, 1_000_000, 0)
	if s := format(LangZH, []note{{chatID: 9, t: tr0, dir: "in"}}); !strings.Contains(s, "已到账") || !strings.Contains(s, "查看交易") {
		t.Fatal(s)
	}
	for _, l := range []string{LangZH, LangRU} {
		for k := range catalog[LangEN] {
			if _, ok := catalog[l][k]; !ok {
				t.Errorf("%s: missing translation for key %d", l, k)
			}
		}
	}
}
