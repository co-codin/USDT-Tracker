package ton

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/httpx"

	"golang.org/x/time/rate"

	"github.com/co-codin/USDT-Tracker/internal/retry"
)

// DefaultBaseURL is the public toncenter v3 (TON Index) mainnet endpoint.
const DefaultBaseURL = "https://toncenter.com/api/v3"

// MaxPageSize is the largest page toncenter accepts for list endpoints.
const MaxPageSize = 1000

const maxResponseBytes = 64 << 20

// ErrBlockNotFound means the indexer does not (yet) have the block.
var ErrBlockNotFound = errors.New("ton: block not found")

// Observer receives per-request telemetry (implemented by the metrics package).
type Observer interface {
	ObserveRequest(endpoint, status string, d time.Duration)
	ObserveRetry(endpoint, reason string)
}

// Options configures a Client.
type Options struct {
	BaseURL    string        // default https://toncenter.com/api/v3
	APIKey     string        // optional toncenter API key (X-API-Key)
	Timeout    time.Duration // per HTTP request, default 15s
	RPS        float64       // client-side rate limit, default 1 req/s (keyless toncenter limit)
	Burst      int           // limiter burst, default 1
	MaxRetries int           // attempts per call, default 8
	Backoff    retry.Backoff
	UserAgent  string
	HTTPClient *http.Client
	Observer   Observer
	Logger     *slog.Logger
}

// Client is a minimal, read-only toncenter v3 client (GET requests only).
type Client struct {
	base   string
	apiKey string
	ua     string
	caller *httpx.Caller
	obs    Observer
	log    *slog.Logger
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
		o.RPS = 1
	}
	if o.Burst <= 0 {
		o.Burst = 1
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 8
	}
	if o.UserAgent == "" {
		o.UserAgent = "usdt-tracker"
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	hc := o.HTTPClient
	if hc == nil {
		hc = httpx.NewClient(o.Timeout)
	}
	c := &Client{base: strings.TrimRight(o.BaseURL, "/"), apiKey: o.APIKey, ua: o.UserAgent, obs: o.Observer, log: o.Logger}
	c.caller = &httpx.Caller{
		Name: "ton",
		HTTP: hc, Limiter: rate.NewLimiter(rate.Limit(o.RPS), o.Burst), Attempts: o.MaxRetries,
		Backoff: o.Backoff, BodyLimit: maxResponseBytes, OnRetry: c.onRetry, ScrubErrors: true,
	}
	if o.Observer != nil {
		c.caller.Observe = o.Observer.ObserveRequest
	}
	return c
}

// HTTPError is returned for non-2xx responses.
type HTTPError struct {
	Endpoint   string
	StatusCode int
	Body       string
	retryAfter time.Duration
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("ton %s: HTTP %d: %s", e.Endpoint, e.StatusCode, e.Body)
}

// RetryAfter implements retry.AfterHint.
func (e *HTTPError) RetryAfter() time.Duration { return e.retryAfter }

// MasterchainInfo returns the first and last indexed masterchain blocks.
func (c *Client) MasterchainInfo(ctx context.Context) (MasterchainInfo, error) {
	var out MasterchainInfo
	if err := c.get(ctx, "/masterchainInfo", nil, &out); err != nil {
		return MasterchainInfo{}, err
	}
	if out.Last.Seqno <= 0 {
		return MasterchainInfo{}, errors.New("ton: empty masterchainInfo response")
	}
	return out, nil
}

// MasterchainBlockShards returns the masterchain block seqno together with
// the shard blocks it committed. ErrBlockNotFound if not indexed (yet).
func (c *Client) MasterchainBlockShards(ctx context.Context, seqno int64) ([]BlockRef, error) {
	var out BlocksResponse
	err := c.get(ctx, "/masterchainBlockShards", url.Values{"seqno": {strconv.FormatInt(seqno, 10)}}, &out)
	var he *HTTPError
	if errors.As(err, &he) && he.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: masterchain %d", ErrBlockNotFound, seqno)
	}
	if err != nil {
		return nil, err
	}
	return out.Blocks, nil
}

// JettonTransfers returns one page of transfers of jetton master with
// transaction lt in [startLt, endLt], ascending.
func (c *Client) JettonTransfers(ctx context.Context, master string, startLt, endLt uint64, limit, offset int) ([]JettonTransfer, error) {
	q := url.Values{
		"jetton_master": {master},
		"start_lt":      {strconv.FormatUint(startLt, 10)},
		"end_lt":        {strconv.FormatUint(endLt, 10)},
		"limit":         {strconv.Itoa(limit)},
		"offset":        {strconv.Itoa(offset)},
		"sort":          {"asc"},
	}
	var out JettonTransfersResponse
	if err := c.get(ctx, "/jetton/transfers", q, &out); err != nil {
		return nil, err
	}
	return out.JettonTransfers, nil
}

func (c *Client) get(ctx context.Context, path string, q url.Values, out any) error {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return c.caller.Do(ctx, path, func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", c.ua)
		if c.apiKey != "" {
			req.Header.Set("X-API-Key", c.apiKey)
		}
		return req, nil
	}, func(r *httpx.Response) error {
		if !r.OK() {
			he := &HTTPError{Endpoint: path, StatusCode: r.StatusCode, Body: truncate(string(r.Body), 300), retryAfter: r.RetryAfter()}
			if r.StatusCode == http.StatusTooManyRequests && he.retryAfter == 0 {
				he.retryAfter = time.Second
			}
			return httpx.Classify(r.StatusCode, he, 0)
		}
		if err := json.Unmarshal(r.Body, out); err != nil {
			return fmt.Errorf("ton %s: decode response: %w", path, err)
		}
		return nil
	})
}

func (c *Client) onRetry(path string, attempt int, delay time.Duration, err error) {
	code := 0
	var he *HTTPError
	if errors.As(err, &he) {
		code = he.StatusCode
	}
	if c.obs != nil {
		c.obs.ObserveRetry(path, httpx.RetryReason(code, false))
	}
	c.log.Warn("ton api retry", "endpoint", path, "attempt", attempt, "delay", delay.String(), "err", err)
}

func truncate(s string, n int) string { return httpx.Truncate(s, n) }
