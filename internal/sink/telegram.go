package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/co-codin/tron-usdt-listener/internal/model"
	"github.com/co-codin/tron-usdt-listener/internal/retry"
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
	MaxRetries  int
	Backoff     retry.Backoff
	HTTPClient  *http.Client
}

// Telegram sends human-friendly alerts via the Telegram Bot API.
type Telegram struct {
	cfg     TelegramConfig
	hc      *http.Client
	limiter *rate.Limiter
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
	if cfg.MaxPerBlock <= 0 {
		cfg.MaxPerBlock = 10
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	// Telegram allows ~1 message/second per chat.
	return &Telegram{cfg: cfg, hc: hc, limiter: rate.NewLimiter(rate.Every(1100*time.Millisecond), 3)}, nil
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
		if err := t.sendMessage(ctx, FormatTelegram(tr, t.cfg.ExplorerURL)); err != nil {
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
func FormatTelegram(tr model.Transfer, explorer string) string {
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
	fmt.Fprintf(&sb, "From: <code>%s</code>\n", html.EscapeString(tr.From))
	fmt.Fprintf(&sb, "To:   <code>%s</code>\n", html.EscapeString(tr.To))
	fmt.Fprintf(&sb, "Block: %d · %s\n", tr.BlockNumber, tr.BlockTime.UTC().Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(&sb, "Tx: <a href=\"%s%s\">%s…%s</a>", html.EscapeString(explorer), html.EscapeString(tr.TxID), html.EscapeString(short(tr.TxID, 8)), html.EscapeString(tail(tr.TxID, 6)))
	return sb.String()
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

type tgResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
	ErrorCode   int    `json:"error_code"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

func (t *Telegram) sendMessage(ctx context.Context, text string) error {
	body, err := json.Marshal(map[string]any{
		"chat_id":                  t.cfg.ChatID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	})
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(t.cfg.APIBase, "/") + "/bot" + t.cfg.BotToken + "/sendMessage"
	return retry.Do(ctx, t.cfg.MaxRetries, t.cfg.Backoff, func(ctx context.Context) error {
		if err := t.limiter.Wait(ctx); err != nil {
			return retry.Permanent(err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return retry.Permanent(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := t.hc.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return retry.Permanent(ctx.Err())
			}
			// Never include the URL (it contains the bot token) in errors.
			var ue *url.Error
			if errors.As(err, &ue) {
				err = ue.Err
			}
			return fmt.Errorf("telegram: request failed: %w", err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var r tgResponse
		_ = json.Unmarshal(raw, &r)
		if resp.StatusCode == http.StatusOK && r.OK {
			return nil
		}
		err = fmt.Errorf("telegram: HTTP %d: %s", resp.StatusCode, r.Description)
		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			return retryAfterErr{err: err, d: time.Duration(r.Parameters.RetryAfter) * time.Second}
		case resp.StatusCode >= 500:
			return err
		default:
			return retry.Permanent(err)
		}
	}, nil)
}

// Close implements Sink.
func (t *Telegram) Close() error { return nil }
