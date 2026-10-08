package tron

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/co-codin/USDT-Tracker/internal/retry"
)

// DefaultBaseURL is the public TronGrid mainnet endpoint.
const DefaultBaseURL = "https://api.trongrid.io"

// maxResponseBytes guards against unexpectedly large responses.
const maxResponseBytes = 64 << 20

// Observer receives per-request telemetry (implemented by the metrics package).
type Observer interface {
	ObserveRequest(endpoint, status string, d time.Duration)
	ObserveRetry(endpoint, reason string)
}

// Options configures a Client.
type Options struct {
	BaseURL    string        // default https://api.trongrid.io
	APIKey     string        // optional TRON-PRO-API-KEY
	Timeout    time.Duration // per HTTP request, default 15s
	RPS        float64       // client-side rate limit, default 3 req/s
	Burst      int           // limiter burst, default 1
	MaxRetries int           // attempts per call, default 8 (<=0 means default)
	Backoff    retry.Backoff
	UserAgent  string
	HTTPClient *http.Client
	Observer   Observer
	Logger     *slog.Logger
}

// Client is a minimal, read-only TRON HTTP API client. It only calls query
// endpoints; it never signs or broadcasts transactions.
type Client struct {
	base     string
	apiKey   string
	ua       string
	hc       *http.Client
	limiter  *rate.Limiter
	attempts int
	backoff  retry.Backoff
	obs      Observer
	log      *slog.Logger
}

// NewClient creates a client with sane defaults.
func NewClient(o Options) *Client {
	if o.BaseURL == "" {
		o.BaseURL = DefaultBaseURL
	}
	if o.Timeout <= 0 {
		o.Timeout = 15 * time.Second
	}
	if o.RPS <= 0 {
		o.RPS = 3
	}
	if o.Burst <= 0 {
		o.Burst = 1
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 8
	}
	if o.UserAgent == "" {
		o.UserAgent = "tron-usdt-listener"
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	hc := o.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: o.Timeout}
	}
	return &Client{
		base:     strings.TrimRight(o.BaseURL, "/"),
		apiKey:   o.APIKey,
		ua:       o.UserAgent,
		hc:       hc,
		limiter:  rate.NewLimiter(rate.Limit(o.RPS), o.Burst),
		attempts: o.MaxRetries,
		backoff:  o.Backoff,
		obs:      o.Observer,
		log:      o.Logger,
	}
}

// HTTPError is returned for non-2xx responses.
type HTTPError struct {
	Endpoint   string
	StatusCode int
	Body       string
	retryAfter time.Duration
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("tron %s: HTTP %d: %s", e.Endpoint, e.StatusCode, e.Body)
}

// RetryAfter implements retry.AfterHint.
func (e *HTTPError) RetryAfter() time.Duration { return e.retryAfter }

// RateLimited reports whether the error is a rate-limit response.
func (e *HTTPError) RateLimited() bool {
	return e.StatusCode == http.StatusTooManyRequests ||
		(e.StatusCode == http.StatusForbidden && isRateLimitBody(e.Body))
}

// APIError is returned when the node answers 200 with {"Error": "..."}.
type APIError struct {
	Endpoint string
	Message  string
}

func (e *APIError) Error() string { return fmt.Sprintf("tron %s: %s", e.Endpoint, e.Message) }

func isRateLimitBody(b string) bool {
	l := strings.ToLower(b)
	return strings.Contains(l, "frequency") || strings.Contains(l, "rate limit") || strings.Contains(l, "exceed")
}

// NowBlock returns the latest block header.
//
// It uses /wallet/getblock with detail=false (header only, a few hundred
// bytes) and falls back to /wallet/getnowblock on nodes that lack it.
func (c *Client) NowBlock(ctx context.Context) (BlockHeader, error) {
	var resp blockResponse
	err := c.post(ctx, "/wallet/getblock", map[string]any{"detail": false}, &resp)
	var he *HTTPError
	if errors.As(err, &he) && (he.StatusCode == http.StatusNotFound || he.StatusCode == http.StatusMethodNotAllowed) {
		err = c.post(ctx, "/wallet/getnowblock", map[string]any{}, &resp)
	}
	if err != nil {
		return BlockHeader{}, err
	}
	if resp.BlockHeader.RawData.Number == 0 {
		return BlockHeader{}, errors.New("tron: empty head block response")
	}
	return resp.header(), nil
}

// BlockByNum returns a block header and its transaction count. It returns
// ErrBlockNotFound when the node does not have the block (yet).
func (c *Client) BlockByNum(ctx context.Context, num int64) (Block, error) {
	var resp blockResponse
	if err := c.post(ctx, "/wallet/getblockbynum", map[string]any{"num": num}, &resp); err != nil {
		return Block{}, err
	}
	if resp.BlockID == "" || resp.BlockHeader.RawData.Number != num {
		return Block{}, fmt.Errorf("%w: %d", ErrBlockNotFound, num)
	}
	return Block{BlockHeader: resp.header(), TxCount: len(resp.Transactions)}, nil
}

// ErrBlockNotFound means the node does not (yet) know the requested block.
var ErrBlockNotFound = errors.New("tron: block not found")

// TransactionInfoByBlockNum returns execution info (incl. event logs) for
// every transaction in the block. Note: nodes return an empty list both for
// empty blocks and for blocks they don't have yet; callers must disambiguate
// (see trc20.Source).
func (c *Client) TransactionInfoByBlockNum(ctx context.Context, num int64) ([]TransactionInfo, error) {
	var raw json.RawMessage
	if err := c.post(ctx, "/wallet/gettransactioninfobyblocknum", map[string]any{"num": num}, &raw); err != nil {
		return nil, err
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] == '{' { // "{}" is returned by some nodes for empty blocks
		return nil, nil
	}
	var infos []TransactionInfo
	if err := json.Unmarshal(raw, &infos); err != nil {
		return nil, fmt.Errorf("tron: decode transaction infos for block %d: %w", num, err)
	}
	return infos, nil
}

// post performs a rate-limited JSON POST with retries and exponential backoff.
func (c *Client) post(ctx context.Context, path string, reqBody, out any) error {
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	return retry.Do(ctx, c.attempts, c.backoff, func(ctx context.Context) error {
		return c.doOnce(ctx, path, payload, out)
	}, func(attempt int, delay time.Duration, err error) {
		reason := "error"
		var he *HTTPError
		if errors.As(err, &he) {
			reason = strconv.Itoa(he.StatusCode)
			if he.RateLimited() {
				reason = "rate_limited"
			}
		}
		if c.obs != nil {
			c.obs.ObserveRetry(path, reason)
		}
		c.log.Warn("tron api retry", "endpoint", path, "attempt", attempt, "delay", delay.String(), "err", err)
	})
}

func (c *Client) doOnce(ctx context.Context, path string, payload []byte, out any) error {
	if err := c.limiter.Wait(ctx); err != nil {
		return retry.Permanent(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(payload))
	if err != nil {
		return retry.Permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.ua)
	if c.apiKey != "" {
		req.Header.Set("TRON-PRO-API-KEY", c.apiKey)
	}

	start := time.Now()
	resp, err := c.hc.Do(req)
	if err != nil {
		c.observe(path, "network_error", start)
		if ctx.Err() != nil {
			return retry.Permanent(ctx.Err())
		}
		return fmt.Errorf("tron %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	c.observe(path, strconv.Itoa(resp.StatusCode), start)
	if err != nil {
		return fmt.Errorf("tron %s: read body: %w", path, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		he := &HTTPError{Endpoint: path, StatusCode: resp.StatusCode, Body: truncate(string(body), 300)}
		he.retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		switch {
		case he.RateLimited():
			if he.retryAfter == 0 && resp.StatusCode == http.StatusForbidden {
				// TronGrid temporarily bans keys/IPs that exceed the limit.
				he.retryAfter = 30 * time.Second
			}
			return he
		case resp.StatusCode >= 500, resp.StatusCode == http.StatusRequestTimeout:
			return he
		default:
			return retry.Permanent(he)
		}
	}

	var apiErr struct {
		Error string `json:"Error"`
	}
	if len(body) > 0 && body[0] == '{' && json.Unmarshal(body, &apiErr) == nil && apiErr.Error != "" {
		return &APIError{Endpoint: path, Message: truncate(apiErr.Error, 300)}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("tron %s: decode response: %w", path, err)
	}
	return nil
}

func (c *Client) observe(path, status string, start time.Time) {
	if c.obs != nil {
		c.obs.ObserveRequest(path, status, time.Since(start))
	}
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
