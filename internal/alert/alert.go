// Package alert posts large USDT transfers (>= a threshold, on every enabled
// chain, independent of the watch list) to a Telegram channel. It is fed
// every decoded transfer of every confirmed block by the listeners, keeps a
// bounded in-memory queue and sends at a steady pace that respects Telegram
// limits (≈20 messages/minute per channel), batching several transfers into
// one message when there is a backlog. Alerts are queued in an outbox: with
// Postgres it is durable (at-least-once across restarts, deduplicated by
// transfer id); without it an in-memory queue bounded by QueueSize drops the
// oldest pending alerts. Observe never waits on Telegram.
package alert

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/explorer"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/outbox"
	"github.com/co-codin/USDT-Tracker/internal/tgbot"
)

// Sender posts a message to a chat (implemented by *tgbot.Client).
type Sender interface {
	SendMessage(ctx context.Context, chatID, text string, o tgbot.SendOptions) error
}

// Config configures the alert channel.
type Config struct {
	ChatID string // channel id or @username (the bot must be a channel admin)
	// Threshold is the minimum amount in token units, e.g. "100000".
	Threshold string
	// Interval is the minimum gap between two messages (default 3s ⇒ 20/min).
	Interval time.Duration
	// MaxPerMessage is how many transfers one message may carry (default 10).
	MaxPerMessage int
	// QueueSize bounds pending alerts (default 1000); the oldest are dropped.
	QueueSize int
	// Footer is an optional HTML line appended to every message (e.g. a link
	// to your project).
	Footer string
	// Outbox stores pending alerts (default: in-memory, bounded by QueueSize).
	Outbox outbox.Store
	// MaxAttempts is how many sends an alert gets before it is given up
	// (default 3; each send already retries transient Telegram errors).
	MaxAttempts int
}

// Stats are cumulative counters.
type Stats struct {
	Queued, Sent, Dropped, Failed, Messages int64
}

// Alerter is safe for concurrent use.
type Alerter struct {
	cfg    Config
	send   Sender
	log    *slog.Logger
	mu     sync.Mutex
	box    outbox.Store
	mem    *outbox.Memory   // non-nil when box is the in-memory default
	thr    map[int]*big.Int // decimals → raw threshold
	stats  Stats
	wake   chan struct{}
	onSent func(status string, n int)
}

// New validates cfg and creates an alerter.
func New(cfg Config, send Sender, log *slog.Logger) (*Alerter, error) {
	if strings.TrimSpace(cfg.ChatID) == "" {
		return nil, fmt.Errorf("alert: chat id is required")
	}
	if cfg.Threshold == "" {
		cfg.Threshold = "100000"
	}
	if _, err := model.ParseUnits(cfg.Threshold, 18); err != nil {
		return nil, fmt.Errorf("alert: threshold: %w", err)
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 3 * time.Second
	}
	if cfg.MaxPerMessage <= 0 {
		cfg.MaxPerMessage = 10
	}
	if cfg.MaxPerMessage > 30 {
		cfg.MaxPerMessage = 30 // keeps messages well below 4096 characters
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1000
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	a := &Alerter{cfg: cfg, send: send, log: log, box: cfg.Outbox, thr: map[int]*big.Int{}, wake: make(chan struct{}, 1)}
	if a.box == nil {
		a.mem = outbox.NewMemory(cfg.QueueSize)
		a.mem.MaxKeys = 100_000
		a.box = a.mem
	}
	return a, nil
}

// OnSent registers a telemetry callback (status: sent|failed|dropped).
func (a *Alerter) OnSent(f func(status string, n int)) { a.onSent = f }

func (a *Alerter) count(status string, n int) {
	if a.onSent != nil && n > 0 {
		a.onSent(status, n)
	}
}

func (a *Alerter) threshold(decimals int) *big.Int {
	t, ok := a.thr[decimals]
	if !ok {
		t, _ = model.ParseUnits(a.cfg.Threshold, decimals)
		if t == nil {
			// Threshold finer than the token's decimals: round up.
			t, _ = model.ParseUnits(strings.Split(a.cfg.Threshold, ".")[0], decimals)
		}
		a.thr[decimals] = t
	}
	return t
}

// Observe enqueues every transfer of b at or above the threshold (keyed by
// transfer id, so a replayed block adds nothing). It never waits on
// Telegram. It matches listener.Tap; an error (outbox unavailable) makes the
// listener retry the block before saving its cursor.
func (a *Alerter) Observe(ctx context.Context, b chain.Block) error {
	a.mu.Lock()
	var items []outbox.Item
	for _, t := range b.Transfers {
		if t.Amount == nil || t.Amount.Cmp(a.threshold(t.Decimals)) < 0 {
			continue
		}
		p, err := json.Marshal(t)
		if err != nil {
			a.mu.Unlock()
			return fmt.Errorf("alert: encode: %w", err)
		}
		items = append(items, outbox.Item{Key: t.Key(), Payload: p})
	}
	a.mu.Unlock()
	if len(items) == 0 {
		return nil
	}
	var before int64
	if a.mem != nil {
		before = a.mem.Dropped()
	}
	added, err := a.box.Enqueue(ctx, outbox.KindAlert, items)
	if err != nil {
		return fmt.Errorf("alert: enqueue: %w", err)
	}
	dropped := 0
	if a.mem != nil {
		dropped = int(a.mem.Dropped() - before)
	}
	a.mu.Lock()
	a.stats.Queued += int64(added)
	a.stats.Dropped += int64(dropped)
	a.mu.Unlock()
	if dropped > 0 {
		a.log.Warn("large-transfer alert queue full, dropped oldest", "dropped", dropped)
		a.count("dropped", dropped)
	}
	if added > 0 {
		select {
		case a.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Stats returns a snapshot of the counters.
func (a *Alerter) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stats
}

// Pending returns the number of queued alerts (0 if the outbox is unreachable).
func (a *Alerter) Pending() int {
	n, err := a.box.Count(context.Background(), outbox.KindAlert)
	if err != nil {
		return 0
	}
	return n
}

// Run sends queued alerts until ctx is done, at most one message per
// Interval. On shutdown it tries to flush for up to flushTimeout.
func (a *Alerter) Run(ctx context.Context, flushTimeout time.Duration) {
	var last time.Time
	for {
		if a.Pending() == 0 {
			select {
			case <-ctx.Done():
				return
			case <-a.wake:
			case <-time.After(5 * time.Second): // durable outbox: pick up rows left by a previous run
			}
			continue
		}
		if wait := a.cfg.Interval - time.Since(last); !last.IsZero() && wait > 0 {
			select {
			case <-ctx.Done():
				a.flush(ctx, flushTimeout)
				return
			case <-time.After(wait):
			}
		}
		if ctx.Err() != nil {
			a.flush(ctx, flushTimeout)
			return
		}
		a.sendNext(ctx)
		last = time.Now()
	}
}

// flush sends what is still queued after ctx was cancelled (shutdown): the
// deadline derives from ctx without its cancellation.
func (a *Alerter) flush(parent context.Context, timeout time.Duration) {
	if timeout <= 0 || a.Pending() == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	for a.Pending() > 0 && ctx.Err() == nil {
		a.sendNext(ctx)
		if err := sleep(ctx, a.cfg.Interval); err != nil {
			return
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// sendNext sends one message with up to MaxPerMessage queued transfers.
func (a *Alerter) sendNext(ctx context.Context) {
	items, err := a.box.Pending(ctx, outbox.KindAlert, a.cfg.MaxPerMessage)
	if err != nil {
		a.log.Warn("large-transfer alert outbox unavailable", "err", err)
		return
	}
	if len(items) == 0 {
		return
	}
	batch := make([]model.Transfer, 0, len(items))
	keys := make([]string, 0, len(items))
	var bad []string
	for _, it := range items {
		var t model.Transfer
		if err := json.Unmarshal(it.Payload, &t); err != nil {
			bad = append(bad, it.Key)
			continue
		}
		batch = append(batch, t)
		keys = append(keys, it.Key)
	}
	if len(bad) > 0 {
		_ = a.box.Done(ctx, outbox.KindAlert, bad, outbox.StatusFailed)
	}
	n := len(batch)
	if n == 0 {
		return
	}
	text := Format(batch, a.cfg.Footer)
	err = a.send.SendMessage(ctx, a.cfg.ChatID, text, tgbot.SendOptions{HTML: true, DisablePreview: true})
	if err != nil {
		a.log.Error("large-transfer alert failed", "transfers", n, "attempt", items[0].Attempts+1, "err", err)
		if items[0].Attempts+1 < a.cfg.MaxAttempts && ctx.Err() == nil {
			_ = a.box.Failed(ctx, outbox.KindAlert, keys)
			return
		}
		_ = a.box.Done(context.WithoutCancel(ctx), outbox.KindAlert, keys, outbox.StatusFailed)
		a.mu.Lock()
		a.stats.Failed += int64(n)
		a.mu.Unlock()
		a.count("failed", n)
		return
	}
	if derr := a.box.Done(context.WithoutCancel(ctx), outbox.KindAlert, keys, outbox.StatusSent); derr != nil {
		a.log.Warn("large-transfer alert sent but not marked done (may repeat after restart)", "err", derr)
	}
	a.mu.Lock()
	a.stats.Sent += int64(n)
	a.stats.Messages++
	a.mu.Unlock()
	a.count("sent", n)
	a.log.Debug("large-transfer alert sent", "transfers", n)
}

// Format renders one Telegram HTML message for transfers (largest first when
// several are batched).
func Format(ts []model.Transfer, footer string) string {
	var sb strings.Builder
	if len(ts) == 1 {
		t := ts[0]
		c := t.ChainName()
		fmt.Fprintf(&sb, "🐋 <b>%s %s</b> on %s\n", model.HumanizeUnits(t.Amount, t.Decimals, 2), html.EscapeString(t.Symbol), explorer.ChainLabel(c))
		fmt.Fprintf(&sb, "From: <a href=\"%s\">%s</a>\n", html.EscapeString(explorer.AddressURL(c, t.From)), html.EscapeString(shortAddr(explorer.DisplayAddress(c, t.From))))
		fmt.Fprintf(&sb, "To: <a href=\"%s\">%s</a>\n", html.EscapeString(explorer.AddressURL(c, t.To)), html.EscapeString(shortAddr(explorer.DisplayAddress(c, t.To))))
		if t.Memo != "" {
			fmt.Fprintf(&sb, "Memo: <code>%s</code>\n", html.EscapeString(truncRunes(t.Memo, 120)))
		}
		fmt.Fprintf(&sb, "<a href=\"%s\">%s</a> · %s", html.EscapeString(explorer.TxURL(t)), explorerName(c), t.BlockTime.UTC().Format("2006-01-02 15:04 UTC"))
	} else {
		sorted := append([]model.Transfer(nil), ts...)
		sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Amount.Cmp(sorted[j].Amount) > 0 })
		fmt.Fprintf(&sb, "🐋 <b>%d large USDT transfers</b>\n", len(sorted))
		for _, t := range sorted {
			c := t.ChainName()
			fmt.Fprintf(&sb, "• <b>%s %s</b> · %s · %s → %s · <a href=\"%s\">tx</a>\n",
				model.HumanizeUnits(t.Amount, t.Decimals, 0), html.EscapeString(t.Symbol), explorer.ChainLabel(c),
				html.EscapeString(shortAddr(explorer.DisplayAddress(c, t.From))), html.EscapeString(shortAddr(explorer.DisplayAddress(c, t.To))),
				html.EscapeString(explorer.TxURL(t)))
		}
	}
	out := strings.TrimRight(sb.String(), "\n")
	if footer != "" {
		out += "\n\n" + footer
	}
	return out
}

func explorerName(c string) string {
	switch c {
	case model.ChainTON:
		return "Tonviewer"
	case model.ChainETH:
		return "Etherscan"
	default:
		return "Tronscan"
	}
}

func shortAddr(a string) string {
	if len(a) <= 14 {
		return a
	}
	return a[:6] + "…" + a[len(a)-4:]
}

func truncRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
