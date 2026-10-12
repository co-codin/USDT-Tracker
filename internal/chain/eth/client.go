package eth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/co-codin/USDT-Tracker/internal/retry"
)

// DefaultRPCURL is a free public mainnet endpoint without an API key. It is
// rate limited and keeps only recent state; use your own node or a provider
// URL (Infura/Alchemy/QuickNode…) in production via ETH_RPC_URL.
const DefaultRPCURL = "https://ethereum-rpc.publicnode.com"

const maxResponseBytes = 128 << 20

// ErrRangeTooLarge means the node refused an eth_getLogs range (too many
// results or blocks); the caller should split the range.
var ErrRangeTooLarge = errors.New("eth: log range too large")

// Observer receives per-request telemetry (implemented by the metrics package).
type Observer interface {
	ObserveRequest(endpoint, status string, d time.Duration)
	ObserveRetry(endpoint, reason string)
}

// Options configures a Client.
type Options struct {
	URL        string // JSON-RPC endpoint; may embed an API key, so it is never logged
	Timeout    time.Duration
	RPS        float64
	Burst      int
	MaxRetries int
	Backoff    retry.Backoff
	UserAgent  string
	HTTPClient *http.Client
	Observer   Observer
	Logger     *slog.Logger
}

// Client is a read-only Ethereum JSON-RPC client.
type Client struct {
	url      string
	ua       string
	hc       *http.Client
	limiter  *rate.Limiter
	attempts int
	backoff  retry.Backoff
	obs      Observer
	log      *slog.Logger
	id       atomic.Int64
}

// NewClient creates a client with sane defaults.
func NewClient(o Options) *Client {
	if o.URL == "" {
		o.URL = DefaultRPCURL
	}
	if o.Timeout <= 0 {
		o.Timeout = 20 * time.Second
	}
	if o.RPS <= 0 {
		o.RPS = 5
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
		hc = &http.Client{Timeout: o.Timeout}
	}
	return &Client{
		url: o.URL, ua: o.UserAgent, hc: hc, limiter: rate.NewLimiter(rate.Limit(o.RPS), o.Burst),
		attempts: o.MaxRetries, backoff: o.Backoff, obs: o.Observer, log: o.Logger,
	}
}

// RedactURL returns scheme://host of an RPC URL, hiding paths, query strings
// and userinfo that commonly carry provider API keys.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<invalid url>"
	}
	r := u.Scheme + "://" + u.Host
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.User != nil {
		r += "/***"
	}
	return r
}

// HTTPError is a non-2xx HTTP response.
type HTTPError struct {
	Method     string
	StatusCode int
	Body       string
	retryAfter time.Duration
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("eth %s: HTTP %d: %s", e.Method, e.StatusCode, e.Body)
}

// RetryAfter implements retry.AfterHint.
func (e *HTTPError) RetryAfter() time.Duration { return e.retryAfter }

// RPCError is a JSON-RPC error object.
type RPCError struct {
	Method  string
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("eth %s: rpc error %d: %s", e.Method, e.Code, e.Message)
}

// Log is an eth_getLogs entry.
type Log struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	BlockHash        string   `json:"blockHash"`
	BlockTimestamp   string   `json:"blockTimestamp"` // optional (newer nodes)
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// LogFilter is the eth_getLogs filter object.
type LogFilter struct {
	FromBlock int64
	ToBlock   int64
	Address   string
	Topics    []string // topic0 only is enough for us
}

// BlockNumber returns the latest block number.
func (c *Client) BlockNumber(ctx context.Context) (int64, error) {
	var hexNum string
	if err := c.call(ctx, "eth_blockNumber", []any{}, &hexNum); err != nil {
		return 0, err
	}
	n, err := ParseQuantity(hexNum)
	if err != nil {
		return 0, fmt.Errorf("eth_blockNumber: %w", err)
	}
	return n, nil
}

// GetLogs calls eth_getLogs. It returns ErrRangeTooLarge (wrapped) when the
// node rejects the range so the caller can split it.
func (c *Client) GetLogs(ctx context.Context, f LogFilter) ([]Log, error) {
	topics := make([]any, len(f.Topics))
	for i, t := range f.Topics {
		topics[i] = t
	}
	params := []any{map[string]any{
		"fromBlock": Quantity(f.FromBlock),
		"toBlock":   Quantity(f.ToBlock),
		"address":   f.Address,
		"topics":    topics,
	}}
	var out []Log
	if err := c.call(ctx, "eth_getLogs", params, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// BlockTime returns the timestamp of block num (eth_getBlockByNumber, no txs).
func (c *Client) BlockTime(ctx context.Context, num int64) (time.Time, error) {
	var hdr *struct {
		Timestamp string `json:"timestamp"`
	}
	if err := c.call(ctx, "eth_getBlockByNumber", []any{Quantity(num), false}, &hdr); err != nil {
		return time.Time{}, err
	}
	if hdr == nil {
		return time.Time{}, fmt.Errorf("eth_getBlockByNumber %d: block not found", num)
	}
	ts, err := ParseQuantity(hdr.Timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("eth_getBlockByNumber %d: %w", num, err)
	}
	return time.Unix(ts, 0).UTC(), nil
}

// Quantity encodes n as a JSON-RPC hex quantity.
func Quantity(n int64) string { return "0x" + strconv.FormatInt(n, 16) }

// ParseQuantity decodes a JSON-RPC hex quantity.
func ParseQuantity(s string) (int64, error) {
	if !strings.HasPrefix(s, "0x") || len(s) < 3 {
		return 0, fmt.Errorf("invalid quantity %q", s)
	}
	n, err := strconv.ParseInt(s[2:], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid quantity %q", s)
	}
	return n, nil
}

func (c *Client) call(ctx context.Context, method string, params []any, out any) error {
	return retry.Do(ctx, c.attempts, c.backoff, func(ctx context.Context) error {
		return c.doOnce(ctx, method, params, out)
	}, func(attempt int, delay time.Duration, err error) {
		reason := "error"
		var he *HTTPError
		var re *RPCError
		switch {
		case errors.As(err, &he) && he.StatusCode == http.StatusTooManyRequests:
			reason = "rate_limited"
		case errors.As(err, &he):
			reason = strconv.Itoa(he.StatusCode)
		case errors.As(err, &re):
			reason = "rpc_" + strconv.Itoa(re.Code)
		}
		if c.obs != nil {
			c.obs.ObserveRetry(method, reason)
		}
		c.log.Warn("eth rpc retry", "method", method, "attempt", attempt, "delay", delay.String(), "err", err)
	})
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
}

func (c *Client) doOnce(ctx context.Context, method string, params []any, out any) error {
	if err := c.limiter.Wait(ctx); err != nil {
		return retry.Permanent(err)
	}
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: c.id.Add(1), Method: method, Params: params})
	if err != nil {
		return retry.Permanent(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return retry.Permanent(errors.New("eth: invalid rpc url"))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.ua)
	start := time.Now()
	resp, err := c.hc.Do(req)
	if err != nil {
		c.observe(method, "network_error", start)
		if ctx.Err() != nil {
			return retry.Permanent(ctx.Err())
		}
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // the URL may contain an API key: keep it out of logs
		}
		return fmt.Errorf("eth %s: %w", method, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	c.observe(method, strconv.Itoa(resp.StatusCode), start)
	if err != nil {
		return fmt.Errorf("eth %s: read body: %w", method, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		he := &HTTPError{Method: method, StatusCode: resp.StatusCode, Body: truncate(string(raw), 300)}
		he.retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			if he.retryAfter == 0 {
				he.retryAfter = time.Second
			}
			return he
		case resp.StatusCode == http.StatusRequestEntityTooLarge && method == "eth_getLogs":
			return retry.Permanent(fmt.Errorf("%w: %w", ErrRangeTooLarge, he))
		case resp.StatusCode >= 500, resp.StatusCode == http.StatusRequestTimeout:
			return he
		default:
			return retry.Permanent(he)
		}
	}
	var r rpcResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("eth %s: decode response: %w", method, err)
	}
	if r.Error != nil {
		r.Error.Method = method
		return classifyRPCError(method, r.Error)
	}
	if err := json.Unmarshal(r.Result, out); err != nil {
		return fmt.Errorf("eth %s: decode result: %w", method, err)
	}
	return nil
}

// classifyRPCError maps provider-specific JSON-RPC errors: range/result
// limits → ErrRangeTooLarge (split), rate limits → retry, the rest permanent.
func classifyRPCError(method string, e *RPCError) error {
	msg := strings.ToLower(e.Message)
	if method == "eth_getLogs" {
		for _, s := range []string{"range", "too many", "limit exceeded", "response size", "query returned more than", "max results", "block range"} {
			if strings.Contains(msg, s) && !strings.Contains(msg, "rate") {
				return retry.Permanent(fmt.Errorf("%w: %w", ErrRangeTooLarge, e))
			}
		}
	}
	switch {
	case e.Code == 429, e.Code == -32005, strings.Contains(msg, "rate limit"), strings.Contains(msg, "too many requests"),
		strings.Contains(msg, "capacity"), strings.Contains(msg, "timeout"), strings.Contains(msg, "header not found"),
		e.Code == -32000 && strings.Contains(msg, "unknown block"), e.Code == -32603:
		return e
	default:
		return retry.Permanent(e)
	}
}

func (c *Client) observe(method, status string, start time.Time) {
	if c.obs != nil {
		c.obs.ObserveRequest(method, status, time.Since(start))
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
