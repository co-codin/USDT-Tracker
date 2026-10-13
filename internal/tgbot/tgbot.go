// Package tgbot is a small Telegram Bot API client (sendMessage and
// getUpdates long polling) shared by the large-transfer alert channel and
// the self-service notify bot. The bot token is part of every URL, so errors
// never include URLs.
package tgbot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/httpx"
	"golang.org/x/time/rate"

	"github.com/co-codin/USDT-Tracker/internal/retry"
)

// DefaultAPIBase is the public Bot API endpoint.
const DefaultAPIBase = "https://api.telegram.org"

// MaxMessageLen is Telegram's limit for a text message (characters).
const MaxMessageLen = 4096

// Options configures a Client.
type Options struct {
	Token      string
	APIBase    string // default https://api.telegram.org
	HTTPClient *http.Client
	MaxRetries int // per call, default 5
	Backoff    retry.Backoff
	// Limiter, if set, throttles every call (e.g. Telegram's ~1 msg/s per chat).
	Limiter *rate.Limiter
}

// Client calls the Bot API.
type Client struct {
	base     string
	attempts int
	caller   httpx.Caller
}

// New creates a client.
func New(o Options) (*Client, error) {
	if strings.TrimSpace(o.Token) == "" {
		return nil, errors.New("tgbot: bot token is required")
	}
	if o.APIBase == "" {
		o.APIBase = DefaultAPIBase
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 5
	}
	hc := o.HTTPClient
	if hc == nil {
		hc = httpx.NewClient(70 * time.Second) // > long-poll timeout
	}
	return &Client{
		base:     strings.TrimRight(o.APIBase, "/") + "/bot" + o.Token,
		attempts: o.MaxRetries,
		// ScrubErrors: the URL contains the bot token and must never be logged.
		caller: httpx.Caller{Name: "telegram", HTTP: hc, Limiter: o.Limiter, Backoff: o.Backoff, ScrubErrors: true},
	}, nil
}

// APIError is an unsuccessful Bot API response.
type APIError struct {
	Method      string
	StatusCode  int
	Code        int
	Description string
	retryAfter  time.Duration
}

func (e *APIError) Error() string {
	return fmt.Sprintf("telegram %s: HTTP %d: %s", e.Method, e.StatusCode, e.Description)
}

// RetryAfter implements retry.AfterHint (Telegram's parameters.retry_after).
func (e *APIError) RetryAfter() time.Duration { return e.retryAfter }

// Forbidden reports whether the bot was blocked / removed from the chat.
func (e *APIError) Forbidden() bool { return e.StatusCode == http.StatusForbidden }

// IsForbidden reports whether err is a 403 (user blocked the bot, bot kicked).
func IsForbidden(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Forbidden()
}

type response struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	Description string          `json:"description"`
	ErrorCode   int             `json:"error_code"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

// Message is the subset of a Telegram message we use.
type Message struct {
	MessageID int64 `json:"message_id"`
	Chat      struct {
		ID   int64  `json:"id"`
		Type string `json:"type"`
	} `json:"chat"`
	From *struct {
		ID           int64  `json:"id"`
		Username     string `json:"username"`
		LanguageCode string `json:"language_code"` // IETF tag of the user's client, e.g. "zh-hans", "ru"
	} `json:"from"`
	Text string `json:"text"`
}

// Update is a getUpdates item.
type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message"`
}

// SendOptions tweaks SendMessage.
type SendOptions struct {
	HTML           bool
	DisablePreview bool
}

// SendMessage posts text to chatID (numeric id or @channelusername).
func (c *Client) SendMessage(ctx context.Context, chatID, text string, o SendOptions) error {
	p := map[string]any{"chat_id": chatID, "text": text, "disable_web_page_preview": o.DisablePreview}
	if o.HTML {
		p["parse_mode"] = "HTML"
	}
	return c.call(ctx, "sendMessage", p, nil, c.attempts)
}

// GetUpdates long-polls for new messages after offset (exclusive id + 1).
func (c *Client) GetUpdates(ctx context.Context, offset int64, timeout time.Duration) ([]Update, error) {
	var out []Update
	p := map[string]any{"offset": offset, "timeout": int(timeout / time.Second), "allowed_updates": []string{"message"}}
	err := c.call(ctx, "getUpdates", p, &out, 1)
	return out, err
}

func (c *Client) call(ctx context.Context, method string, params, out any, attempts int) error {
	body, err := json.Marshal(params)
	if err != nil {
		return err
	}
	cl := c.caller
	cl.Attempts = attempts
	return cl.Do(ctx, method, func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/"+method, bytes.NewReader(body))
		if err != nil {
			return nil, errors.New("tgbot: invalid api base")
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}, func(resp *httpx.Response) error {
		var r response
		_ = json.Unmarshal(resp.Body, &r)
		if resp.StatusCode == http.StatusOK && r.OK {
			if out != nil {
				if err := json.Unmarshal(r.Result, out); err != nil {
					return fmt.Errorf("telegram %s: decode: %w", method, err)
				}
			}
			return nil
		}
		ae := &APIError{Method: method, StatusCode: resp.StatusCode, Code: r.ErrorCode, Description: r.Description}
		if r.Parameters.RetryAfter > 0 {
			ae.retryAfter = time.Duration(r.Parameters.RetryAfter) * time.Second
		}
		return httpx.Classify(resp.StatusCode, ae, 0)
	})
}
