package eth

import (
	"github.com/co-codin/USDT-Tracker/internal/httpx"

	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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
	url    string
	ua     string
	caller *httpx.Caller
	obs    Observer
	log    *slog.Logger
	id     atomic.Int64
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
		hc = httpx.NewClient(o.Timeout)
	}
	c := &Client{url: o.URL, ua: o.UserAgent, obs: o.Observer, log: o.Logger}
	c.caller = &httpx.Caller{
		Name: "eth", HTTP: hc, Limiter: rate.NewLimiter(rate.Limit(o.RPS), o.Burst), Attempts: o.MaxRetries,
		Backoff: o.Backoff, BodyLimit: maxResponseBytes, OnRetry: c.onRetry, ScrubErrors: true,
	}
	if o.Observer != nil {
		c.caller.Observe = o.Observer.ObserveRequest
	}
	return c
}

// RedactURL returns scheme://host of an RPC URL, hiding paths, query strings
// and userinfo that commonly carry provider API keys.
func RedactURL(raw string) string { return httpx.RedactURL(raw) }

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

// Header is the subset of an eth_getBlockByNumber result we use.
type Header struct {
	Number     string `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
	Timestamp  string `json:"timestamp"`
}

// HeadTag selects which block eth_getBlockByNumber treats as the head.
type HeadTag string

// Head tags. "latest" is the newest block (use confirmations); "safe" and
// "finalized" are the post-merge consensus checkpoints (finalized blocks
// cannot be reorged without slashing ~1/3 of stake).
const (
	HeadLatest    HeadTag = "latest"
	HeadSafe      HeadTag = "safe"
	HeadFinalized HeadTag = "finalized"
)

// HeaderByTag returns the block number of a head tag (latest/safe/finalized).
func (c *Client) HeaderByTag(ctx context.Context, tag HeadTag) (int64, error) {
	if tag == "" || tag == HeadLatest {
		return c.BlockNumber(ctx)
	}
	var hdr *Header
	if err := c.call(ctx, "eth_getBlockByNumber", []any{string(tag), false}, &hdr); err != nil {
		return 0, err
	}
	if hdr == nil {
		return 0, fmt.Errorf("eth_getBlockByNumber %s: block not found", tag)
	}
	return ParseQuantity(hdr.Number)
}

// HeaderByNumber returns the header of block num, or nil if the node does
// not have it.
func (c *Client) HeaderByNumber(ctx context.Context, num int64) (*Header, error) {
	var hdr *Header
	if err := c.call(ctx, "eth_getBlockByNumber", []any{Quantity(num), false}, &hdr); err != nil {
		return nil, err
	}
	return hdr, nil
}

// ErrBatchUnsupported means the endpoint rejected a JSON-RPC batch request.
var ErrBatchUnsupported = errors.New("eth: JSON-RPC batch requests not supported")

// LogsWithHeaders fetches eth_getLogs for f and the headers of every block in
// [f.FromBlock, f.ToBlock] in ONE JSON-RPC batch, so both answers come from
// the same backend. A nil header means that backend does not have the block
// yet (a lagging node); callers must then treat the empty log result as
// untrustworthy. Returns ErrBatchUnsupported (permanent) when the endpoint
// refuses batches.
func (c *Client) LogsWithHeaders(ctx context.Context, f LogFilter) ([]Log, []*Header, error) {
	body, decode, err := c.batchRequest(f)
	if err != nil {
		return nil, nil, err
	}
	var logs []Log
	var hdrs []*Header
	err = c.post(ctx, "eth_getLogs+headers", body, func(raw []byte) error {
		var derr error
		logs, hdrs, derr = decode(raw)
		return derr
	})
	return logs, hdrs, err
}

func (c *Client) batchRequest(f LogFilter) ([]byte, func([]byte) ([]Log, []*Header, error), error) {
	topics := make([]any, len(f.Topics))
	for i, t := range f.Topics {
		topics[i] = t
	}
	n := int(f.ToBlock - f.FromBlock + 1)
	reqs := make([]rpcRequest, 0, n+1)
	logsID := c.id.Add(1)
	reqs = append(reqs, rpcRequest{JSONRPC: "2.0", ID: logsID, Method: "eth_getLogs", Params: []any{map[string]any{
		"fromBlock": Quantity(f.FromBlock), "toBlock": Quantity(f.ToBlock), "address": f.Address, "topics": topics,
	}}})
	ids := make(map[int64]int, n) // request id -> header index
	for i := 0; i < n; i++ {
		id := c.id.Add(1)
		ids[id] = i
		reqs = append(reqs, rpcRequest{JSONRPC: "2.0", ID: id, Method: "eth_getBlockByNumber", Params: []any{Quantity(f.FromBlock + int64(i)), false}})
	}
	body, err := json.Marshal(reqs)
	if err != nil {
		return nil, nil, err
	}
	return body, func(raw []byte) ([]Log, []*Header, error) { return decodeBatch(raw, f, n, logsID, ids) }, nil
}

func decodeBatch(raw []byte, f LogFilter, n int, logsID int64, ids map[int64]int) ([]Log, []*Header, error) {
	var resps []rpcResponse
	if err := json.Unmarshal(raw, &resps); err != nil {
		var single rpcResponse
		if json.Unmarshal(raw, &single) == nil && single.Error != nil {
			single.Error.Method = "batch"
			if cerr := classifyRPCError("batch", single.Error); !retry.IsPermanent(cerr) {
				return nil, nil, cerr
			}
			return nil, nil, retry.Permanent(fmt.Errorf("%w: %w", ErrBatchUnsupported, single.Error))
		}
		return nil, nil, fmt.Errorf("eth batch: decode response: %w", err)
	}
	var logs []Log
	gotLogs := false
	hdrs := make([]*Header, n)
	seen := make([]bool, n)
	for _, r := range resps {
		var id int64
		if err := json.Unmarshal(r.ID, &id); err != nil {
			return nil, nil, fmt.Errorf("eth batch: bad response id %s", r.ID)
		}
		method := "eth_getBlockByNumber"
		if id == logsID {
			method = "eth_getLogs"
		}
		if r.Error != nil {
			r.Error.Method = method
			return nil, nil, classifyRPCError(method, r.Error)
		}
		if id == logsID {
			if err := json.Unmarshal(r.Result, &logs); err != nil {
				return nil, nil, fmt.Errorf("eth_getLogs: decode result: %w", err)
			}
			gotLogs = true
			continue
		}
		i, ok := ids[id]
		if !ok {
			return nil, nil, fmt.Errorf("eth batch: unexpected response id %d", id)
		}
		if err := json.Unmarshal(r.Result, &hdrs[i]); err != nil {
			return nil, nil, fmt.Errorf("eth_getBlockByNumber: decode result: %w", err)
		}
		seen[i] = true
	}
	if !gotLogs {
		return nil, nil, fmt.Errorf("eth batch: eth_getLogs response missing")
	}
	for i, ok := range seen {
		if !ok {
			return nil, nil, fmt.Errorf("eth batch: header %d response missing", f.FromBlock+int64(i))
		}
	}
	return logs, hdrs, nil
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
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: c.id.Add(1), Method: method, Params: params})
	if err != nil {
		return err
	}
	return c.post(ctx, method, body, func(raw []byte) error {
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
	})
}

func (c *Client) onRetry(method string, attempt int, delay time.Duration, err error) {
	var he *HTTPError
	var re *RPCError
	reason := "error"
	switch {
	case errors.As(err, &he):
		reason = httpx.RetryReason(he.StatusCode, false)
	case errors.As(err, &re):
		reason = "rpc_" + strconv.Itoa(re.Code)
	}
	if c.obs != nil {
		c.obs.ObserveRetry(method, reason)
	}
	c.log.Warn("eth rpc retry", "method", method, "attempt", attempt, "delay", delay.String(), "err", err)
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
}

// post sends one JSON-RPC HTTP request (single call or batch) with retries
// and hands each 2xx body to decode. label names the call in errors and metrics.
func (c *Client) post(ctx context.Context, label string, body []byte, decode func([]byte) error) error {
	return c.caller.Do(ctx, label, func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
		if err != nil {
			return nil, errors.New("eth: invalid rpc url")
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", c.ua)
		return req, nil
	}, func(r *httpx.Response) error {
		if !r.OK() {
			he := &HTTPError{Method: label, StatusCode: r.StatusCode, Body: truncate(string(r.Body), 300), retryAfter: r.RetryAfter()}
			switch {
			case r.StatusCode == http.StatusTooManyRequests && he.retryAfter == 0:
				he.retryAfter = time.Second
			case r.StatusCode == http.StatusRequestEntityTooLarge && strings.HasPrefix(label, "eth_getLogs"):
				return retry.Permanent(fmt.Errorf("%w: %w", ErrRangeTooLarge, he))
			}
			return httpx.Classify(r.StatusCode, he, 0)
		}
		return decode(r.Body)
	})
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

func truncate(s string, n int) string { return httpx.Truncate(s, n) }
