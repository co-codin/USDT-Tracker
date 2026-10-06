package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/co-codin/tron-usdt-listener/internal/model"
	"github.com/co-codin/tron-usdt-listener/internal/retry"
	"github.com/co-codin/tron-usdt-listener/pkg/webhooksig"
)

// Webhook header names (signature headers live in pkg/webhooksig).
const (
	HeaderEvent    = "X-Webhook-Event"
	HeaderDelivery = "X-Webhook-Delivery"
	WebhookEvent   = "trc20.transfers"
)

// WebhookConfig configures the webhook sink.
type WebhookConfig struct {
	URL        string
	Secret     string // HMAC-SHA256 key; when empty requests are unsigned
	Timeout    time.Duration
	MaxRetries int
	Headers    map[string]string
	Backoff    retry.Backoff
	HTTPClient *http.Client
	Now        func() time.Time
}

// WebhookPayload is the JSON body POSTed to the webhook URL (one per block).
type WebhookPayload struct {
	Event       string           `json:"event"`
	DeliveryID  string           `json:"delivery_id"`
	BlockNumber int64            `json:"block_number"`
	BlockTime   time.Time        `json:"block_time"`
	Transfers   []model.Transfer `json:"transfers"`
}

// Webhook POSTs a signed JSON payload per block with retries.
type Webhook struct {
	cfg WebhookConfig
	hc  *http.Client
}

// NewWebhook validates cfg and creates the sink.
func NewWebhook(cfg WebhookConfig) (*Webhook, error) {
	if !strings.HasPrefix(cfg.URL, "http://") && !strings.HasPrefix(cfg.URL, "https://") {
		return nil, fmt.Errorf("webhook: invalid url %q", cfg.URL)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: cfg.Timeout}
	}
	return &Webhook{cfg: cfg, hc: hc}, nil
}

// Name implements Sink.
func (w *Webhook) Name() string { return "webhook" }

// DeliveryID is deterministic for a given set of transfers so that receivers
// can recognise redeliveries.
func DeliveryID(b Batch) string {
	h := sha256.New()
	for _, t := range b.Transfers {
		h.Write([]byte(t.Key()))
		h.Write([]byte{'\n'})
	}
	return strconv.FormatInt(b.BlockNumber, 10) + "-" + hex.EncodeToString(h.Sum(nil))[:16]
}

// Send implements Sink.
func (w *Webhook) Send(ctx context.Context, b Batch) error {
	p := WebhookPayload{
		Event:       WebhookEvent,
		DeliveryID:  DeliveryID(b),
		BlockNumber: b.BlockNumber,
		BlockTime:   b.BlockTime.UTC(),
		Transfers:   b.Transfers,
	}
	body, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return retry.Do(ctx, w.cfg.MaxRetries, w.cfg.Backoff, func(ctx context.Context) error {
		return w.post(ctx, p.DeliveryID, body)
	}, nil)
}

func (w *Webhook) post(ctx context.Context, deliveryID string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return retry.Permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tron-usdt-listener")
	req.Header.Set(HeaderEvent, WebhookEvent)
	req.Header.Set(HeaderDelivery, deliveryID)
	for k, v := range w.cfg.Headers {
		req.Header.Set(k, v)
	}
	if w.cfg.Secret != "" {
		ts := w.cfg.Now().Unix()
		req.Header.Set(webhooksig.HeaderTimestamp, strconv.FormatInt(ts, 10))
		req.Header.Set(webhooksig.HeaderSignature, webhooksig.Sign([]byte(w.cfg.Secret), ts, body))
	}
	resp, err := w.hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return retry.Permanent(ctx.Err())
		}
		return err
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	herr := fmt.Errorf("webhook: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500 {
		if ra := parseRetryAfterSeconds(resp.Header.Get("Retry-After")); ra > 0 {
			return retryAfterErr{err: herr, d: ra}
		}
		return herr
	}
	return retry.Permanent(herr)
}

// Close implements Sink.
func (w *Webhook) Close() error { return nil }

type retryAfterErr struct {
	err error
	d   time.Duration
}

func (e retryAfterErr) Error() string             { return e.err.Error() }
func (e retryAfterErr) Unwrap() error             { return e.err }
func (e retryAfterErr) RetryAfter() time.Duration { return e.d }

var _ retry.AfterHint = retryAfterErr{}

func parseRetryAfterSeconds(v string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}
