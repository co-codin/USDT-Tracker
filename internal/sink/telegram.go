package sink

import (
	"context"
	"fmt"
	"html"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/httpx"
	"github.com/co-codin/USDT-Tracker/internal/tgbot"

	"golang.org/x/time/rate"

	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/explorer"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
)

// TelegramConfig configures the Telegram alert sink.
type TelegramConfig struct {
	BotToken string
	ChatID   string
	// MinAmount (raw units) further restricts alerts on top of the global
	// filter; nil sends an alert for every matched transfer.
	MinAmount *big.Int
	// MaxPerBlock caps messages per block; extra transfers are summarised.
	MaxPerBlock int
	APIBase     string // default https://api.telegram.org
	ExplorerURL string // default https://tronscan.org/#/transaction/
	// TONExplorerURL is the transaction link prefix for TON transfers
	// (default https://tonviewer.com/transaction/, takes the hex hash).
	TONExplorerURL string
	// ETHExplorerURL is the link prefix for Ethereum transfers
	// (default https://etherscan.io/tx/, takes the 0x hash).
	ETHExplorerURL string
	MaxRetries     int
	Backoff        retry.Backoff
	HTTPClient     *http.Client
}

// Telegram sends human-friendly alerts via the Telegram Bot API.
type Telegram struct {
	cfg TelegramConfig
	bot *tgbot.Client
}

// NewTelegram validates cfg and creates the sink.
func NewTelegram(cfg TelegramConfig) (*Telegram, error) {
	if cfg.BotToken == "" || cfg.ChatID == "" {
		return nil, fmt.Errorf("telegram: bot token and chat id are required")
	}
	if cfg.APIBase == "" {
		cfg.APIBase = "https://api.telegram.org"
	}
	if cfg.ExplorerURL == "" {
		cfg.ExplorerURL = "https://tronscan.org/#/transaction/"
	}
	if cfg.TONExplorerURL == "" {
		cfg.TONExplorerURL = "https://tonviewer.com/transaction/"
	}
	if cfg.ETHExplorerURL == "" {
		cfg.ETHExplorerURL = explorer.ETHTx
	}
	if cfg.MaxPerBlock <= 0 {
		cfg.MaxPerBlock = 10
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = httpx.NewClient(15 * time.Second)
	}
	bot, err := tgbot.New(tgbot.Options{
		Token: cfg.BotToken, APIBase: cfg.APIBase, HTTPClient: hc, MaxRetries: cfg.MaxRetries, Backoff: cfg.Backoff,
		// Telegram allows ~1 message/second per chat.
		Limiter: rate.NewLimiter(rate.Every(1100*time.Millisecond), 3),
	})
	if err != nil {
		return nil, fmt.Errorf("telegram: %w", err)
	}
	return &Telegram{cfg: cfg, bot: bot}, nil
}

// Name implements Sink.
func (t *Telegram) Name() string { return "telegram" }

// Send implements Sink.
func (t *Telegram) Send(ctx context.Context, b Batch) error {
	var alerts []model.Transfer
	for _, tr := range b.Transfers {
		if t.cfg.MinAmount == nil || tr.Amount.Cmp(t.cfg.MinAmount) >= 0 || tr.HasReason(model.ReasonIncoming) || tr.HasReason(model.ReasonOutgoing) {
			alerts = append(alerts, tr)
		}
	}
	if len(alerts) == 0 {
		return nil
	}
	shown := alerts
	if len(shown) > t.cfg.MaxPerBlock {
		shown = alerts[:t.cfg.MaxPerBlock]
	}
	for _, tr := range shown {
		link := t.cfg.ExplorerURL
		switch tr.ChainName() {
		case model.ChainTON:
			link = t.cfg.TONExplorerURL
		case model.ChainETH:
			link = t.cfg.ETHExplorerURL
		}
		if err := t.sendMessage(ctx, FormatTelegram(tr, link)); err != nil {
			return err
		}
	}
	if rest := len(alerts) - len(shown); rest > 0 {
		sum := new(big.Int)
		for _, tr := range alerts[len(shown):] {
			sum.Add(sum, tr.Amount)
		}
		msg := fmt.Sprintf("…and <b>%d</b> more %s transfers in block %d (total %s %s)",
			rest, html.EscapeString(alerts[0].Symbol), b.BlockNumber,
			model.HumanizeUnits(sum, alerts[0].Decimals, 2), html.EscapeString(alerts[0].Symbol))
		return t.sendMessage(ctx, msg)
	}
	return nil
}

// FormatTelegram renders an HTML alert message for a transfer.
func FormatTelegram(tr model.Transfer, txPrefix string) string {
	title := "💸 " + tr.Symbol + " transfer"
	switch {
	case tr.HasReason(model.ReasonIncoming):
		title = "📥 Incoming " + tr.Symbol
	case tr.HasReason(model.ReasonOutgoing):
		title = "📤 Outgoing " + tr.Symbol
	case tr.HasReason(model.ReasonLarge):
		title = "🐋 Large " + tr.Symbol + " transfer"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "<b>%s</b>\n", html.EscapeString(title))
	fmt.Fprintf(&sb, "💵 <b>%s %s</b>\n", model.HumanizeUnits(tr.Amount, tr.Decimals, 2), html.EscapeString(tr.Symbol))
	from, to, blockWord := tr.From, tr.To, "Block"
	switch tr.ChainName() {
	case model.ChainTON: // show the wallet-style UQ… form
		from, to, blockWord = ton.FriendlyFromRaw(from), ton.FriendlyFromRaw(to), "TON masterchain block"
	case model.ChainETH: // EIP-55 checksummed, as on etherscan
		from, to, blockWord = explorer.DisplayAddress(model.ChainETH, from), explorer.DisplayAddress(model.ChainETH, to), "Ethereum block"
	}
	fmt.Fprintf(&sb, "From: <code>%s</code>\n", html.EscapeString(from))
	fmt.Fprintf(&sb, "To:   <code>%s</code>\n", html.EscapeString(to))
	if tr.Memo != "" {
		fmt.Fprintf(&sb, "Memo: <code>%s</code>\n", html.EscapeString(truncRunes(tr.Memo, 200)))
	}
	fmt.Fprintf(&sb, "%s: %d · %s\n", blockWord, tr.BlockNumber, tr.BlockTime.UTC().Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(&sb, "Tx: <a href=\"%s%s\">%s…%s</a>", html.EscapeString(txPrefix), html.EscapeString(tr.TxID), html.EscapeString(short(tr.TxID, 8)), html.EscapeString(tail(tr.TxID, 6)))
	return sb.String()
}

func truncRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func short(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func (t *Telegram) sendMessage(ctx context.Context, text string) error {
	return t.bot.SendMessage(ctx, t.cfg.ChatID, text, tgbot.SendOptions{HTML: true, DisablePreview: true})
}

// Close implements Sink.
func (t *Telegram) Close() error { return nil }
