package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chainaddr"
	"github.com/co-codin/USDT-Tracker/internal/explorer"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/outbox"
	"github.com/co-codin/USDT-Tracker/internal/tgbot"
)

// API is the subset of the Bot API used (implemented by *tgbot.Client).
type API interface {
	SendMessage(ctx context.Context, chatID, text string, o tgbot.SendOptions) error
	GetUpdates(ctx context.Context, offset int64, timeout time.Duration) ([]tgbot.Update, error)
}

// Config tunes the bot.
type Config struct {
	// Limits maps plan → max addresses. Missing plans fall back to FreeLimit.
	// Premium is the hook for paid tiers (no payment logic here).
	FreeLimit    int // default 3
	PremiumLimit int // default 50
	// MinAmount (token units) skips dust / zero-value "address poisoning"
	// transfers; default "0.01".
	MinAmount string
	// Chains that are actually being listened to (addresses of other chains are rejected).
	Chains          []string
	PollTimeout     time.Duration // getUpdates long-poll timeout, default 50s
	RefreshInterval time.Duration // subscription cache refresh, default 5s
	SendRPS         float64       // global message rate, default 25/s (Telegram: ~30/s)
	QueueSize       int           // pending notifications for the in-memory outbox, default 10000
	// Outbox stores pending notifications. With Postgres (the app's default)
	// delivery is at-least-once and deduplicated across restarts; nil uses a
	// bounded in-memory outbox (lost on restart).
	Outbox outbox.Store
	// MaxAttempts per message before it is given up (default 5; each attempt
	// already retries transient Telegram errors).
	MaxAttempts int
	// MaxUsers / MaxTotalSubs cap bot-wide users (chats that used /add) and
	// subscriptions; /add is refused beyond them. 0 = unlimited.
	MaxUsers     int
	MaxTotalSubs int
	// PremiumContact (optional) is shown by /plan as the way to upgrade,
	// e.g. "@your_username" or a URL.
	PremiumContact string
	// CommandsPerMinute is the per-chat command budget (burst 5); extra
	// messages are dropped silently. Default 20.
	CommandsPerMinute int
}

func (c *Config) defaults() {
	if c.FreeLimit <= 0 {
		c.FreeLimit = 3
	}
	if c.PremiumLimit <= 0 {
		c.PremiumLimit = 50
	}
	if c.MinAmount == "" {
		c.MinAmount = "0.01"
	}
	if len(c.Chains) == 0 {
		c.Chains = chainaddr.Chains
	}
	if c.PollTimeout <= 0 {
		c.PollTimeout = 50 * time.Second
	}
	if c.RefreshInterval <= 0 {
		c.RefreshInterval = 5 * time.Second
	}
	if c.SendRPS <= 0 {
		c.SendRPS = 25
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 10000
	}
	if c.CommandsPerMinute <= 0 {
		c.CommandsPerMinute = 20
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
}

// maxTrackedChats bounds the per-chat limiter table.
const maxTrackedChats = 50_000

type chatLimiter struct {
	lim  *rate.Limiter
	seen time.Time
}

// allow reports whether chatID may run another command now.
func (b *Bot) allow(chatID int64) bool {
	b.rlMu.Lock()
	defer b.rlMu.Unlock()
	now := time.Now()
	cl, ok := b.chats[chatID]
	if !ok {
		if len(b.chats) >= maxTrackedChats {
			for id, c := range b.chats { // evict idle chats (their buckets are full again)
				if now.Sub(c.seen) > time.Minute {
					delete(b.chats, id)
				}
			}
			if len(b.chats) >= maxTrackedChats {
				return false // flood of new chats: shed load
			}
		}
		cl = &chatLimiter{lim: rate.NewLimiter(rate.Limit(float64(b.cfg.CommandsPerMinute)/60), 5)}
		b.chats[chatID] = cl
	}
	cl.seen = now
	return cl.lim.Allow()
}

func (b *Bot) limits() Limits {
	return Limits{PerPlan: b.cfg.Limit, MaxUsers: b.cfg.MaxUsers, MaxTotalSubs: b.cfg.MaxTotalSubs}
}

// Limit returns the address limit of plan.
func (c Config) Limit(plan string) int {
	if plan == PlanPremium {
		return c.PremiumLimit
	}
	return c.FreeLimit
}

type note struct {
	chatID int64
	t      model.Transfer
	dir    string // in|out|self
	addr   string // the subscribed address
	lang   string
}

// notePayload is the outbox JSON of a note.
type notePayload struct {
	T    model.Transfer `json:"t"`
	Dir  string         `json:"dir"`
	Addr string         `json:"addr"`
	Lang string         `json:"lang,omitempty"`
}

func (n note) key() string { return fmt.Sprintf("%d|%s|%s", n.chatID, n.t.Key(), n.dir) }

// Bot is the notify bot. Create with New, feed blocks with Observe (a
// listener.Tap) and run with Run.
type Bot struct {
	cfg     Config
	store   Store
	api     API
	log     *slog.Logger
	index   atomic.Pointer[map[string][]int64] // chain|address → chat ids
	dirty   chan struct{}
	mu      sync.Mutex
	box     outbox.Store
	mem     *outbox.Memory
	wake    chan struct{}
	minAmt  map[int]*big.Int
	limiter *rate.Limiter
	sent    atomic.Int64
	dropped atomic.Int64
	rlMu    sync.Mutex
	chats   map[int64]*chatLimiter
	limited atomic.Int64
	langs   sync.Map // chat id → language of the last command
}

// New creates a bot.
func New(cfg Config, store Store, api API, log *slog.Logger) (*Bot, error) {
	cfg.defaults()
	if _, err := model.ParseUnits(cfg.MinAmount, 18); err != nil {
		return nil, fmt.Errorf("notify: min_amount: %w", err)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	b := &Bot{cfg: cfg, store: store, api: api, log: log, dirty: make(chan struct{}, 1), wake: make(chan struct{}, 1),
		box: cfg.Outbox, minAmt: map[int]*big.Int{}, limiter: rate.NewLimiter(rate.Limit(cfg.SendRPS), 1),
		chats: map[int64]*chatLimiter{}}
	if b.box == nil {
		b.mem = outbox.NewMemory(cfg.QueueSize)
		b.box = b.mem
	}
	empty := map[string][]int64{}
	b.index.Store(&empty)
	return b, nil
}

// Refresh reloads the subscription index from the store.
func (b *Bot) Refresh(ctx context.Context) error {
	subs, err := b.store.ActiveSubs(ctx)
	if err != nil {
		return err
	}
	idx := make(map[string][]int64, len(subs))
	for _, s := range subs {
		k := s.Chain + "|" + s.Address
		idx[k] = append(idx[k], s.ChatID)
	}
	b.index.Store(&idx)
	return nil
}

func (b *Bot) markDirty() {
	select {
	case b.dirty <- struct{}{}:
	default:
	}
}

// Stats returns delivered and dropped notification counts.
func (b *Bot) Stats() (sent, dropped int64) { return b.sent.Load(), b.dropped.Load() }

// RateLimited returns how many commands were dropped by the per-chat limit.
func (b *Bot) RateLimited() int64 { return b.limited.Load() }

func (b *Bot) min(decimals int) *big.Int {
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.minAmt[decimals]
	if !ok {
		m, _ = model.ParseUnits(b.cfg.MinAmount, decimals)
		if m == nil {
			m = big.NewInt(1)
		}
		b.minAmt[decimals] = m
	}
	return m
}

// Observe enqueues notifications for subscribed addresses in block bl
// (listener.Tap). Keys are chat|transfer|direction, so a replayed block adds
// nothing; an outbox error makes the listener retry the block before its
// cursor is saved. It never waits on Telegram.
func (b *Bot) Observe(ctx context.Context, bl chain.Block) error {
	idx := *b.index.Load()
	if len(idx) == 0 {
		return nil
	}
	var items []outbox.Item
	add := func(n note) error {
		if v, ok := b.langs.Load(n.chatID); ok {
			n.lang, _ = v.(string)
		}
		p, err := json.Marshal(notePayload{T: n.t, Dir: n.dir, Addr: n.addr, Lang: n.lang})
		if err != nil {
			return err
		}
		items = append(items, outbox.Item{Key: n.key(), ChatID: n.chatID, Payload: p})
		return nil
	}
	for _, t := range bl.Transfers {
		if t.Amount == nil || t.Amount.Cmp(b.min(t.Decimals)) < 0 {
			continue
		}
		c := t.ChainName()
		toChats, fromChats := idx[c+"|"+t.To], idx[c+"|"+t.From]
		for _, id := range toChats {
			dir := "in"
			if t.From == t.To {
				dir = "self"
			}
			if err := add(note{chatID: id, t: t, dir: dir, addr: t.To}); err != nil {
				return fmt.Errorf("notify: encode: %w", err)
			}
		}
		if t.From != t.To {
			for _, id := range fromChats {
				if err := add(note{chatID: id, t: t, dir: "out", addr: t.From}); err != nil {
					return fmt.Errorf("notify: encode: %w", err)
				}
			}
		}
	}
	if len(items) == 0 {
		return nil
	}
	var before int64
	if b.mem != nil {
		before = b.mem.Dropped()
	}
	added, err := b.box.Enqueue(ctx, outbox.KindNotify, items)
	if err != nil {
		return fmt.Errorf("notify: enqueue: %w", err)
	}
	if b.mem != nil {
		b.dropped.Add(b.mem.Dropped() - before)
	}
	if added > 0 {
		select {
		case b.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Run polls commands, refreshes the subscription index and delivers
// notifications until ctx is done.
func (b *Bot) Run(ctx context.Context) error {
	if err := b.Refresh(ctx); err != nil {
		b.log.Warn("notify: initial refresh failed", "err", err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); b.refreshLoop(ctx) }()
	go func() { defer wg.Done(); b.sendLoop(ctx) }()
	b.pollLoop(ctx)
	wg.Wait()
	return nil
}

func (b *Bot) refreshLoop(ctx context.Context) {
	t := time.NewTicker(b.cfg.RefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-b.dirty:
		}
		if err := b.Refresh(ctx); err != nil && ctx.Err() == nil {
			b.log.Warn("notify: refresh subscriptions failed", "err", err)
		}
	}
}

func (b *Bot) pollLoop(ctx context.Context) {
	offset, err := b.store.LoadOffset(ctx)
	if err != nil {
		b.log.Warn("notify: load offset failed", "err", err)
	}
	backoff := time.Second
	for ctx.Err() == nil {
		ups, err := b.api.GetUpdates(ctx, offset, b.cfg.PollTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.log.Warn("notify: getUpdates failed", "err", err)
			var ra interface{ RetryAfter() time.Duration }
			d := backoff
			if errors.As(err, &ra) && ra.RetryAfter() > d {
				d = ra.RetryAfter()
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		for _, u := range ups {
			if u.Message != nil && u.Message.Text != "" {
				if !b.allow(u.Message.Chat.ID) {
					b.limited.Add(1)
					offset = u.UpdateID + 1
					continue
				}
				reply := b.Handle(ctx, u.Message)
				if reply != "" {
					// Replies share the global send budget with notifications so
					// command floods cannot push the bot into Telegram's 429s.
					if err := b.limiter.Wait(ctx); err != nil {
						return
					}
					if err := b.api.SendMessage(ctx, strconv.FormatInt(u.Message.Chat.ID, 10), reply, tgbot.SendOptions{HTML: true, DisablePreview: true}); err != nil {
						b.log.Warn("notify: reply failed", "err", err)
					}
				}
			}
			offset = u.UpdateID + 1
		}
		if len(ups) > 0 {
			if err := b.store.SaveOffset(ctx, offset); err != nil {
				b.log.Warn("notify: save offset failed", "err", err)
			}
		}
	}
}

// Handle processes one message and returns the HTML reply ("" = none).
// Replies use the language of the user's Telegram client (en, zh, ru).
func (b *Bot) Handle(ctx context.Context, m *tgbot.Message) string {
	lang := LangEN
	username := ""
	if m.From != nil {
		lang = langOf(m.From.LanguageCode)
		username = m.From.Username
	}
	T := func(k msgKey, args ...any) string {
		if len(args) == 0 {
			return tr(lang, k)
		}
		return fmt.Sprintf(tr(lang, k), args...)
	}
	text := strings.TrimSpace(m.Text)
	if !strings.HasPrefix(text, "/") {
		if _, err := chainaddr.Detect(text); err == nil && !strings.ContainsAny(text, " \n") {
			text = "/add " + text // a bare address means /add
		} else if m.Chat.Type == "" || m.Chat.Type == "private" {
			return T(mHint)
		} else {
			return ""
		}
	}
	fields := strings.Fields(text)
	cmd := strings.ToLower(fields[0])
	if i := strings.IndexByte(cmd, '@'); i > 0 {
		cmd = cmd[:i] // /add@MyBot
	}
	args := fields[1:]
	chatID := m.Chat.ID
	if m.Chat.Type != "" && m.Chat.Type != "private" {
		// In groups any member could change the group's subscriptions:
		// only private chats are served.
		if cmd == "/start" || cmd == "/help" || cmd == "/add" {
			return T(mPrivateOnly)
		}
		return ""
	}
	b.langs.Store(chatID, lang) // notifications follow the last language seen
	user, known, err := b.store.GetUser(ctx, chatID)
	if err != nil {
		b.log.Error("notify: get user", "err", err)
		return T(mTempError)
	}
	if known && user.Blocked { // they talk to us again: resume deliveries
		if err := b.store.SetBlocked(ctx, chatID, false); err == nil {
			b.markDirty()
		}
	}
	switch cmd {
	case "/start":
		return T(mStart) + "\n\n" + T(mHelp) + "\n\n" + T(mFreePlan, b.cfg.FreeLimit)
	case "/help":
		return T(mHelp) + "\n\n" + T(mFreePlan, b.cfg.FreeLimit)
	case "/add":
		if len(args) != 1 {
			return T(mUsageAdd)
		}
		c, addr, err := b.parseAddress(lang, args[0])
		if err != nil {
			return "❌ " + html.EscapeString(err.Error())
		}
		switch err := b.store.AddSub(ctx, chatID, username, c, addr, b.limits()); {
		case errors.Is(err, ErrExists):
			return T(mAlready)
		case errors.Is(err, ErrCapacity):
			return T(mCapacity)
		case errors.Is(err, ErrLimit):
			return T(mLimit, html.EscapeString(user.Plan), b.cfg.Limit(user.Plan), b.cfg.PremiumLimit)
		case err != nil:
			b.log.Error("notify: add", "err", err)
			return T(mTempError)
		}
		b.markDirty()
		return T(mWatching, explorer.ChainLabel(c), html.EscapeString(explorer.DisplayAddress(c, addr)))
	case "/list":
		subs, err := b.store.ListSubs(ctx, chatID)
		if err != nil {
			return T(mTempError)
		}
		if len(subs) == 0 {
			return T(mNone)
		}
		var sb strings.Builder
		sb.WriteString(T(mListHeader, len(subs), b.cfg.Limit(user.Plan), html.EscapeString(user.Plan)))
		sb.WriteString("\n")
		for i, s := range subs {
			fmt.Fprintf(&sb, "%d. %s <code>%s</code>\n", i+1, explorer.ChainLabel(s.Chain), html.EscapeString(explorer.DisplayAddress(s.Chain, s.Address)))
		}
		return strings.TrimRight(sb.String(), "\n")
	case "/remove", "/delete":
		if len(args) != 1 {
			return T(mUsageRemove)
		}
		var c, addr string
		if n, err := strconv.Atoi(args[0]); err == nil {
			subs, err := b.store.ListSubs(ctx, chatID)
			if err != nil {
				return T(mTempError)
			}
			if n < 1 || n > len(subs) {
				return T(mNoSuchNumber)
			}
			c, addr = subs[n-1].Chain, subs[n-1].Address
		} else {
			c, addr, err = b.parseAddress(lang, args[0])
			if err != nil {
				return "❌ " + html.EscapeString(err.Error())
			}
		}
		switch err := b.store.RemoveSub(ctx, chatID, c, addr); {
		case errors.Is(err, ErrNotFound):
			return T(mNotWatching)
		case err != nil:
			return T(mTempError)
		}
		b.markDirty()
		return T(mStopped, html.EscapeString(explorer.DisplayAddress(c, addr)))
	case "/plan":
		subs, _ := b.store.ListSubs(ctx, chatID)
		how := T(mPremiumHow)
		if b.cfg.PremiumContact != "" {
			how = T(mPremiumContact, html.EscapeString(b.cfg.PremiumContact))
		}
		return T(mPlan, html.EscapeString(user.Plan), len(subs), b.cfg.Limit(user.Plan), b.cfg.PremiumLimit) + "\n" + how
	default:
		return T(mUnknown)
	}
}

func (b *Bot) parseAddress(lang, s string) (string, string, error) {
	c, err := chainaddr.Detect(s)
	if err != nil {
		return "", "", errors.New(tr(lang, mUnrecognised))
	}
	enabled := false
	for _, x := range b.cfg.Chains {
		enabled = enabled || x == c
	}
	if !enabled {
		return "", "", fmt.Errorf(tr(lang, mUnsupported), explorer.ChainLabel(c))
	}
	addr, err := chainaddr.Normalize(c, s)
	if err != nil {
		return "", "", fmt.Errorf(tr(lang, mInvalidAddr), explorer.ChainLabel(c))
	}
	return c, addr, nil
}

func (b *Bot) sendLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if !b.sendNext(ctx) {
			select {
			case <-ctx.Done():
				return
			case <-b.wake:
			case <-time.After(2 * time.Second): // retries and rows left by a previous run
			}
		}
	}
}

// sendNext sends the oldest pending notification, merged with up to 9 more
// pending for the same chat (keeps a busy address under Telegram's per-chat
// limit). It reports whether there was anything to send.
func (b *Bot) sendNext(ctx context.Context) bool {
	items, err := b.box.Pending(ctx, outbox.KindNotify, 200)
	if err != nil {
		if ctx.Err() == nil {
			b.log.Warn("notify: outbox unavailable", "err", err)
		}
		return false
	}
	if len(items) == 0 {
		return false
	}
	if err := b.limiter.Wait(ctx); err != nil {
		return false
	}
	first := items[0].ChatID
	var batch []note
	var keys, bad []string
	attempts := 0
	for _, it := range items {
		if it.ChatID != first || len(batch) == 10 {
			continue
		}
		var p notePayload
		if err := json.Unmarshal(it.Payload, &p); err != nil {
			bad = append(bad, it.Key)
			continue
		}
		batch = append(batch, note{chatID: it.ChatID, t: p.T, dir: p.Dir, addr: p.Addr, lang: p.Lang})
		keys = append(keys, it.Key)
		attempts = max(attempts, it.Attempts)
	}
	dctx := context.WithoutCancel(ctx)
	if len(bad) > 0 {
		_ = b.box.Done(dctx, outbox.KindNotify, bad, outbox.StatusFailed)
	}
	if len(batch) == 0 {
		return true
	}
	lang := batch[len(batch)-1].lang
	if v, ok := b.langs.Load(first); ok {
		lang, _ = v.(string)
	}
	if lang == "" {
		lang = LangEN
	}
	err = b.api.SendMessage(ctx, strconv.FormatInt(first, 10), format(lang, batch), tgbot.SendOptions{HTML: true, DisablePreview: true})
	switch {
	case tgbot.IsForbidden(err):
		b.log.Info("notify: user blocked the bot, pausing deliveries", "chat_id", first)
		_ = b.box.Done(dctx, outbox.KindNotify, keys, outbox.StatusDropped)
		b.dropped.Add(int64(len(batch)))
		if err := b.store.SetBlocked(ctx, first, true); err == nil {
			b.markDirty()
		}
	case err != nil && ctx.Err() != nil:
		// shutting down: leave pending, the next run resends
	case err != nil && attempts+1 < b.cfg.MaxAttempts:
		b.log.Warn("notify: delivery failed, will retry", "chat_id", first, "attempt", attempts+1, "err", err)
		_ = b.box.Failed(dctx, outbox.KindNotify, keys)
	case err != nil:
		b.log.Warn("notify: delivery failed, giving up", "chat_id", first, "attempts", attempts+1, "err", err)
		_ = b.box.Done(dctx, outbox.KindNotify, keys, outbox.StatusFailed)
		b.dropped.Add(int64(len(batch)))
	default:
		if derr := b.box.Done(dctx, outbox.KindNotify, keys, outbox.StatusSent); derr != nil {
			b.log.Warn("notify: sent but not marked done (may repeat after restart)", "err", derr)
		}
		b.sent.Add(int64(len(batch)))
	}
	return true
}

// format renders notifications for one chat in lang.
func format(lang string, ns []note) string {
	var sb strings.Builder
	for i, n := range ns {
		if i > 0 {
			sb.WriteString("\n\n")
		}
		t, c := n.t, n.t.ChainName()
		amt := model.HumanizeUnits(t.Amount, t.Decimals, 6) + " " + html.EscapeString(t.Symbol)
		to, from := html.EscapeString(explorer.DisplayAddress(c, t.To)), html.EscapeString(explorer.DisplayAddress(c, t.From))
		switch n.dir {
		case "in":
			fmt.Fprintf(&sb, tr(lang, mReceived)+"\n", amt, explorer.ChainLabel(c))
			fmt.Fprintf(&sb, "%s: <code>%s</code>\n%s: <code>%s</code>\n", tr(lang, mTo), to, tr(lang, mFrom), from)
		case "out":
			fmt.Fprintf(&sb, tr(lang, mSent)+"\n", amt, explorer.ChainLabel(c))
			fmt.Fprintf(&sb, "%s: <code>%s</code>\n%s: <code>%s</code>\n", tr(lang, mFrom), from, tr(lang, mTo), to)
		default:
			fmt.Fprintf(&sb, tr(lang, mSelf)+"\n", amt, explorer.ChainLabel(c))
			fmt.Fprintf(&sb, "%s: <code>%s</code>\n", tr(lang, mAddress), to)
		}
		if t.Memo != "" {
			memo := []rune(t.Memo)
			if len(memo) > 120 {
				memo = append(memo[:120], '…')
			}
			fmt.Fprintf(&sb, "%s: <code>%s</code>\n", tr(lang, mMemo), html.EscapeString(string(memo)))
		}
		fmt.Fprintf(&sb, "<a href=\"%s\">%s</a> · %s", html.EscapeString(explorer.TxURL(t)), tr(lang, mView), t.BlockTime.UTC().Format("2006-01-02 15:04 UTC"))
	}
	return sb.String()
}
