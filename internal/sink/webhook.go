package sink

import (
	"github.com/co-codin/USDT-Tracker/internal/httpx"

	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
	"github.com/co-codin/USDT-Tracker/pkg/webhooksig"
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
		return nil, fmt.Errorf("webhook: invalid url %s (want http:// or https://)", httpx.RedactURL(cfg.URL))
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
		hc = httpx.NewClient(cfg.Timeout)
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
	return w.caller().Do(ctx, httpx.RedactURL(w.cfg.URL), func(ctx context.Context) (*http.Request, error) {
		return w.request(ctx, p.DeliveryID, body)
	}, func(r *httpx.Response) error {
		if r.OK() {
			return nil
		}
		herr := fmt.Errorf("webhook: HTTP %d: %s", r.StatusCode, httpx.Truncate(string(r.Body), 512))
		return httpx.Classify(r.StatusCode, herr, r.RetryAfter())
	})
}

func (w *Webhook) caller() *httpx.Caller {
	// ScrubErrors: the URL may carry a token in its path or query.
	return &httpx.Caller{Name: "webhook", HTTP: w.hc, Attempts: w.cfg.MaxRetries, Backoff: w.cfg.Backoff, BodyLimit: 512, ScrubErrors: true}
}

func (w *Webhook) request(ctx context.Context, deliveryID string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "usdt-tracker")
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
	return req, nil
}

// Close implements Sink.
func (w *Webhook) Close() error { return nil }
