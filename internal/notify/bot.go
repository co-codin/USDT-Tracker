package notify

import (
	"context"
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
	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/explorer"
	"github.com/co-codin/USDT-Tracker/internal/model"
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
	QueueSize       int           // pending notifications, default 10000
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
}

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
	queue   []note
	wake    chan struct{}
	seen    *cursor.Dedupe
	minAmt  map[int]*big.Int
	limiter *rate.Limiter
	sent    atomic.Int64
	dropped atomic.Int64
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
		seen: cursor.NewDedupe(200_000), minAmt: map[int]*big.Int{}, limiter: rate.NewLimiter(rate.Limit(cfg.SendRPS), 1)}
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

// Observe queues notifications for subscribed addresses in block bl
// (listener.Tap: called with every decoded transfer, never blocks).
func (b *Bot) Observe(_ context.Context, bl chain.Block) {
	idx := *b.index.Load()
	if len(idx) == 0 {
		return
	}
	var notes []note
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
			notes = append(notes, note{chatID: id, t: t, dir: dir, addr: t.To})
		}
		if t.From != t.To {
			for _, id := range fromChats {
				notes = append(notes, note{chatID: id, t: t, dir: "out", addr: t.From})
			}
		}
	}
	if len(notes) == 0 {
		return
	}
	b.mu.Lock()
	for _, n := range notes {
		if !b.seen.Add(fmt.Sprintf("%d|%s|%s", n.chatID, n.t.Key(), n.dir)) {
			continue
		}
		b.queue = append(b.queue, n)
	}
	if over := len(b.queue) - b.cfg.QueueSize; over > 0 {
		b.queue = append([]note(nil), b.queue[over:]...)
		b.dropped.Add(int64(over))
	}
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
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
				reply := b.Handle(ctx, u.Message)
				if reply != "" {
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

const helpText = `👋 <b>USDT payment notifications</b>
I message you whenever USDT arrives at (or leaves) your addresses on TRON, TON or Ethereum. Read-only: I never ask for keys or seed phrases.

/add &lt;address&gt; – watch an address (network detected automatically)
/list – your addresses
/remove &lt;address or number&gt; – stop watching
/plan – your plan and limits
/help – this message`

// Handle processes one message and returns the HTML reply ("" = none).
func (b *Bot) Handle(ctx context.Context, m *tgbot.Message) string {
	text := strings.TrimSpace(m.Text)
	if !strings.HasPrefix(text, "/") {
		if _, err := chainaddr.Detect(text); err == nil && !strings.ContainsAny(text, " \n") {
			text = "/add " + text // a bare address means /add
		} else {
			return "Send /add &lt;address&gt; or /help."
		}
	}
	fields := strings.Fields(text)
	cmd := strings.ToLower(fields[0])
	if i := strings.IndexByte(cmd, '@'); i > 0 {
		cmd = cmd[:i] // /add@MyBot
	}
	args := fields[1:]
	chatID := m.Chat.ID
	username := ""
	if m.From != nil {
		username = m.From.Username
	}
	user, err := b.store.EnsureUser(ctx, chatID, username)
	if err != nil {
		b.log.Error("notify: ensure user", "err", err)
		return "⚠️ Temporary error, please try again later."
	}
	switch cmd {
	case "/start", "/help":
		return helpText + fmt.Sprintf("\n\nFree plan: up to %d addresses.", b.cfg.FreeLimit)
	case "/add":
		if len(args) != 1 {
			return "Usage: /add &lt;address&gt;\nExamples: T…, UQ…/EQ… or 0x…"
		}
		c, addr, err := b.parseAddress(args[0])
		if err != nil {
			return "❌ " + html.EscapeString(err.Error())
		}
		switch err := b.store.AddSub(ctx, chatID, c, addr, b.cfg.Limit); {
		case errors.Is(err, ErrExists):
			return "ℹ️ Already watching this address."
		case errors.Is(err, ErrLimit):
			return fmt.Sprintf("🔒 Your %s plan allows %d addresses. Remove one with /remove, or upgrade to premium for up to %d.",
				html.EscapeString(user.Plan), b.cfg.Limit(user.Plan), b.cfg.PremiumLimit)
		case err != nil:
			b.log.Error("notify: add", "err", err)
			return "⚠️ Temporary error, please try again later."
		}
		b.markDirty()
		return fmt.Sprintf("✅ Watching %s address\n<code>%s</code>\nYou'll get a message for every incoming and outgoing USDT transfer.",
			explorer.ChainLabel(c), html.EscapeString(explorer.DisplayAddress(c, addr)))
	case "/list":
		subs, err := b.store.ListSubs(ctx, chatID)
		if err != nil {
			return "⚠️ Temporary error, please try again later."
		}
		if len(subs) == 0 {
			return "You are not watching any address yet. Send /add &lt;address&gt;."
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "📋 Your addresses (%d/%d, %s plan):\n", len(subs), b.cfg.Limit(user.Plan), html.EscapeString(user.Plan))
		for i, s := range subs {
			fmt.Fprintf(&sb, "%d. %s <code>%s</code>\n", i+1, explorer.ChainLabel(s.Chain), html.EscapeString(explorer.DisplayAddress(s.Chain, s.Address)))
		}
		return strings.TrimRight(sb.String(), "\n")
	case "/remove", "/delete":
		if len(args) != 1 {
			return "Usage: /remove &lt;address or number from /list&gt;"
		}
		var c, addr string
		if n, err := strconv.Atoi(args[0]); err == nil {
			subs, err := b.store.ListSubs(ctx, chatID)
			if err != nil {
				return "⚠️ Temporary error, please try again later."
			}
			if n < 1 || n > len(subs) {
				return "❌ No such number, see /list."
			}
			c, addr = subs[n-1].Chain, subs[n-1].Address
		} else {
			c, addr, err = b.parseAddress(args[0])
			if err != nil {
				return "❌ " + html.EscapeString(err.Error())
			}
		}
		switch err := b.store.RemoveSub(ctx, chatID, c, addr); {
		case errors.Is(err, ErrNotFound):
			return "ℹ️ You are not watching this address."
		case err != nil:
			return "⚠️ Temporary error, please try again later."
		}
		b.markDirty()
		return "🗑 Stopped watching <code>" + html.EscapeString(explorer.DisplayAddress(c, addr)) + "</code>."
	case "/plan":
		subs, _ := b.store.ListSubs(ctx, chatID)
		return fmt.Sprintf("Plan: <b>%s</b>\nAddresses: %d/%d\nPremium: up to %d addresses.",
			html.EscapeString(user.Plan), len(subs), b.cfg.Limit(user.Plan), b.cfg.PremiumLimit)
	default:
		return "Unknown command. /help"
	}
}

func (b *Bot) parseAddress(s string) (string, string, error) {
	c, err := chainaddr.Detect(s)
	if err != nil {
		return "", "", errors.New("unrecognised address: send a TRON (T…), TON (UQ…/EQ…) or Ethereum (0x…) address")
	}
	enabled := false
	for _, x := range b.cfg.Chains {
		enabled = enabled || x == c
	}
	if !enabled {
		return "", "", fmt.Errorf("%s is not supported by this bot yet", explorer.ChainLabel(c))
	}
	addr, err := chainaddr.Normalize(c, s)
	if err != nil {
		return "", "", fmt.Errorf("invalid %s address", explorer.ChainLabel(c))
	}
	return c, addr, nil
}

func (b *Bot) sendLoop(ctx context.Context) {
	for {
		b.mu.Lock()
		n := len(b.queue)
		b.mu.Unlock()
		if n == 0 {
			select {
			case <-ctx.Done():
				return
			case <-b.wake:
				continue
			}
		}
		if err := b.limiter.Wait(ctx); err != nil {
			return
		}
		b.sendNext(ctx)
	}
}

// sendNext sends the oldest notification, merged with up to 9 more queued
// for the same chat (keeps a busy address under Telegram's per-chat limit).
func (b *Bot) sendNext(ctx context.Context) {
	b.mu.Lock()
	if len(b.queue) == 0 {
		b.mu.Unlock()
		return
	}
	first := b.queue[0]
	batch := []note{first}
	rest := b.queue[:0:0]
	for _, n := range b.queue[1:] {
		if n.chatID == first.chatID && len(batch) < 10 {
			batch = append(batch, n)
		} else {
			rest = append(rest, n)
		}
	}
	b.queue = rest
	b.mu.Unlock()
	err := b.api.SendMessage(ctx, strconv.FormatInt(first.chatID, 10), format(batch), tgbot.SendOptions{HTML: true, DisablePreview: true})
	switch {
	case tgbot.IsForbidden(err):
		b.log.Info("notify: user blocked the bot, pausing deliveries", "chat_id", first.chatID)
		if err := b.store.SetBlocked(ctx, first.chatID, true); err == nil {
			b.markDirty()
		}
	case err != nil:
		b.log.Warn("notify: delivery failed", "chat_id", first.chatID, "err", err)
	default:
		b.sent.Add(int64(len(batch)))
	}
}

// format renders notifications for one chat.
func format(ns []note) string {
	var sb strings.Builder
	for i, n := range ns {
		if i > 0 {
			sb.WriteString("\n\n")
		}
		t, c := n.t, n.t.ChainName()
		amt := model.HumanizeUnits(t.Amount, t.Decimals, 6) + " " + html.EscapeString(t.Symbol)
		switch n.dir {
		case "in":
			fmt.Fprintf(&sb, "📥 <b>+%s</b> received on %s\n", amt, explorer.ChainLabel(c))
			fmt.Fprintf(&sb, "To: <code>%s</code>\nFrom: <code>%s</code>\n", html.EscapeString(explorer.DisplayAddress(c, t.To)), html.EscapeString(explorer.DisplayAddress(c, t.From)))
		case "out":
			fmt.Fprintf(&sb, "📤 <b>−%s</b> sent on %s\n", amt, explorer.ChainLabel(c))
			fmt.Fprintf(&sb, "From: <code>%s</code>\nTo: <code>%s</code>\n", html.EscapeString(explorer.DisplayAddress(c, t.From)), html.EscapeString(explorer.DisplayAddress(c, t.To)))
		default:
			fmt.Fprintf(&sb, "🔁 <b>%s</b> self-transfer on %s\n", amt, explorer.ChainLabel(c))
			fmt.Fprintf(&sb, "Address: <code>%s</code>\n", html.EscapeString(explorer.DisplayAddress(c, t.To)))
		}
		if t.Memo != "" {
			memo := []rune(t.Memo)
			if len(memo) > 120 {
				memo = append(memo[:120], '…')
			}
			fmt.Fprintf(&sb, "Memo: <code>%s</code>\n", html.EscapeString(string(memo)))
		}
		fmt.Fprintf(&sb, "<a href=\"%s\">View transaction</a> · %s", html.EscapeString(explorer.TxURL(t)), t.BlockTime.UTC().Format("2006-01-02 15:04 UTC"))
	}
	return sb.String()
}
