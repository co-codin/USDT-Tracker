// Package tontest provides an in-process mock of the toncenter v3 API (the
// subset used by the listener) serving a deterministic synthetic chain with
// two workchain-0 shards whose lt ranges overlap across masterchain blocks,
// plus fault injection (HTTP status codes, 429 with Retry-After).
package tontest

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Synthetic raw addresses (uppercase hex, like toncenter returns them).
var (
	USDTMasterRaw = "0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE"
	OtherMaster   = "0:" + strings.Repeat("AB", 32)
	Alice         = raw(0x11)
	Bob           = raw(0x22)
	Carol         = raw(0x33)
	walletA       = "0:0" + strings.Repeat("A", 63) // first bit 0 → shard 4000…
	walletB       = "0:F" + strings.Repeat("B", 63) // first bit 1 → shard c000…
)

func raw(b byte) string { return "0:" + strings.ToUpper(hex.EncodeToString(bytesOf(b))) }

func bytesOf(b byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = b
	}
	return out
}

// Lower returns the canonical (lowercase) raw form used by the listener.
func Lower(s string) string { return strings.ToLower(s) }

// GenesisTime is the time of masterchain block 0.
var GenesisTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Expected is one USD₮ transfer the listener must report.
type Expected struct {
	Key    string // "ton:<hex hash>:0"
	Block  int64
	From   string // canonical raw
	To     string
	Amount *big.Int
	Memo   string
}

type item struct {
	mc      int64
	master  string
	wallet  string
	from    string
	to      string
	amount  string
	lt      uint64
	hash    string
	aborted bool
	memo    string
}

// Server is a mock toncenter v3 API.
type Server struct {
	*httptest.Server

	mu       sync.Mutex
	head     int64
	items    []item
	faults   map[string][]int // path -> status codes returned before succeeding
	APIKey   string           // when set, requests without X-API-Key get 401
	requests map[string]int
}

// New starts a server with masterchain head at head.
func New(head int64) *Server {
	s := &Server{head: head, faults: map[string][]int{}, requests: map[string]int{}}
	for n := int64(1); n <= head+10; n++ {
		s.items = append(s.items, genItems(n)...)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/masterchainInfo", s.wrap(s.info))
	mux.HandleFunc("GET /api/v3/masterchainBlockShards", s.wrap(s.shards))
	mux.HandleFunc("GET /api/v3/jetton/transfers", s.wrap(s.transfers))
	s.Server = httptest.NewServer(mux)
	return s
}

// BaseURL returns the API base URL (…/api/v3).
func (s *Server) BaseURL() string { return s.URL + "/api/v3" }

// SetHead moves the chain head.
func (s *Server) SetHead(h int64) { s.mu.Lock(); s.head = h; s.mu.Unlock() }

// Fail makes the next len(codes) requests to path ("/masterchainInfo",
// "/masterchainBlockShards", "/jetton/transfers") return those statuses.
func (s *Server) Fail(path string, codes ...int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[path] = append(s.faults[path], codes...)
}

// Requests returns how many requests path received.
func (s *Server) Requests(path string) int { s.mu.Lock(); defer s.mu.Unlock(); return s.requests[path] }

func ltBase(n int64) uint64 { return uint64(n) * 1000 } //nolint:gosec // test data

// shard A: [n*1000, n*1000+500]; shard B: [n*1000+200, n*1000+1100] (overlaps A of n+1).
func ranges(n int64) (aLo, aHi, bLo, bHi uint64) {
	b := ltBase(n)
	return b, b + 500, b + 200, b + 1100
}

func hashOf(n int64, i int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("ton-%d-%d", n, i)))
	return base64.StdEncoding.EncodeToString(h[:])
}

func genItems(n int64) []item {
	aLo, _, _, bHi := ranges(n)
	var out []item
	if n%3 == 0 {
		out = append(out, item{mc: n, master: USDTMasterRaw, wallet: walletA, from: Alice, to: Bob,
			amount: strconv.FormatInt(n*1_000_000, 10), lt: aLo + 10, hash: hashOf(n, 0), memo: fmt.Sprintf("inv-%d", n)})
	}
	if n%5 == 0 { // near the end of shard B's range: inside shard A's range of n+1 too
		out = append(out, item{mc: n, master: USDTMasterRaw, wallet: walletB, from: Carol, to: Alice,
			amount: "1500000", lt: bHi - 50, hash: hashOf(n, 1)})
	}
	if n%7 == 0 { // other jetton and aborted USD₮ transfer: must be ignored
		out = append(out, item{mc: n, master: OtherMaster, wallet: walletA, from: Bob, to: Carol, amount: "1", lt: aLo + 20, hash: hashOf(n, 2)})
		out = append(out, item{mc: n, master: USDTMasterRaw, wallet: walletA, from: Bob, to: Carol, amount: "2", lt: aLo + 30, hash: hashOf(n, 3), aborted: true})
	}
	return out
}

// AddBurst adds k extra USD₮ transfers to masterchain block n (shard A),
// `perLt` of them sharing each logical time, e.g. an airdrop. Keep
// k/perLt below 400 so every lt stays inside shard A's range.
func (s *Server) AddBurst(n int64, k, perLt int) {
	aLo, _, _, _ := ranges(n)
	if perLt < 1 {
		perLt = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < k; i++ {
		s.items = append(s.items, item{mc: n, master: USDTMasterRaw, wallet: walletA, from: Alice, to: Bob,
			amount: strconv.Itoa(i + 1), lt: aLo + 100 + uint64(i/perLt), hash: hashOf(n, 1000+i)}) //nolint:gosec // test data
	}
}

// Expected returns the USD₮ transfers in masterchain blocks [from, to].
func (s *Server) Expected(from, to int64) []Expected {
	var out []Expected
	for _, it := range s.items {
		if it.mc < from || it.mc > to || it.master != USDTMasterRaw || it.aborted {
			continue
		}
		b, _ := base64.StdEncoding.DecodeString(it.hash)
		amt, _ := new(big.Int).SetString(it.amount, 10)
		out = append(out, Expected{Key: "ton:" + hex.EncodeToString(b) + ":0", Block: it.mc,
			From: Lower(it.from), To: Lower(it.to), Amount: amt, Memo: it.memo})
	}
	return out
}

func (s *Server) wrap(h func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/v3")
		s.mu.Lock()
		s.requests[path]++
		var code int
		if q := s.faults[path]; len(q) > 0 {
			code, s.faults[path] = q[0], q[1:]
		}
		key := s.APIKey
		s.mu.Unlock()
		if key != "" && r.Header.Get("X-API-Key") != key {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "API key does not exist"})
			return
		}
		if code != 0 {
			if code == http.StatusTooManyRequests {
				w.Header().Set("Retry-After", "0")
			}
			writeJSON(w, code, map[string]string{"error": "injected"})
			return
		}
		h(w, r)
	}
}

func block(wc int32, shard string, seqno int64, lo, hi uint64, utime int64) map[string]any {
	return map[string]any{"workchain": wc, "shard": shard, "seqno": seqno,
		"gen_utime": strconv.FormatInt(utime, 10), "start_lt": strconv.FormatUint(lo, 10),
		"end_lt": strconv.FormatUint(hi, 10), "tx_count": 1}
}

func utime(n int64) int64 { return GenesisTime.Unix() + n }

func (s *Server) info(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	h := s.head
	s.mu.Unlock()
	writeJSON(w, 200, map[string]any{
		"last":  block(-1, "8000000000000000", h, ltBase(h), ltBase(h)+5, utime(h)),
		"first": block(-1, "8000000000000000", 1, 1, 5, utime(1)),
	})
}

func (s *Server) shards(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.ParseInt(r.URL.Query().Get("seqno"), 10, 64)
	s.mu.Lock()
	h := s.head
	s.mu.Unlock()
	if err != nil || n < 1 || n > h {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "blocks not found"})
		return
	}
	aLo, aHi, bLo, bHi := ranges(n)
	writeJSON(w, 200, map[string]any{"blocks": []any{
		block(-1, "8000000000000000", n, ltBase(n)+1, ltBase(n)+3, utime(n)),
		block(0, "4000000000000000", n*2, aLo, aHi, utime(n)),
		block(0, "c000000000000000", n*2+1, bLo, bHi, utime(n)),
	}})
}

func (s *Server) transfers(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	lo, _ := strconv.ParseUint(q.Get("start_lt"), 10, 64)
	hi, _ := strconv.ParseUint(q.Get("end_lt"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	if limit <= 0 {
		limit = 10
	}
	master := q.Get("jetton_master")
	if master != "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs" && !strings.EqualFold(master, USDTMasterRaw) {
		writeJSON(w, 200, map[string]any{"jetton_transfers": []any{}, "address_book": map[string]any{}})
		return
	}
	s.mu.Lock()
	var sel []item
	for _, it := range s.items {
		if it.lt >= lo && it.lt <= hi && it.mc <= s.head {
			sel = append(sel, it)
		}
	}
	s.mu.Unlock()
	sort.Slice(sel, func(i, j int) bool { return sel[i].lt < sel[j].lt })
	if offset > len(sel) {
		offset = len(sel)
	}
	sel = sel[offset:min(len(sel), offset+limit)]
	out := make([]map[string]any, 0, len(sel))
	for _, it := range sel {
		var dec any
		if it.memo != "" {
			dec = map[string]any{"@type": "text_comment", "comment": it.memo, "type": "text_comment"}
		}
		out = append(out, map[string]any{
			"query_id": "0", "source": it.from, "destination": it.to, "amount": it.amount,
			"source_wallet": it.wallet, "jetton_master": it.master, "transaction_hash": it.hash,
			"transaction_lt": strconv.FormatUint(it.lt, 10), "transaction_now": utime(it.mc),
			"transaction_aborted": it.aborted, "response_destination": it.from, "custom_payload": nil,
			"forward_ton_amount": "1", "forward_payload": nil, "decoded_forward_payload": dec, "trace_id": it.hash,
		})
	}
	writeJSON(w, 200, map[string]any{"jetton_transfers": out, "address_book": map[string]any{}, "metadata": map[string]any{}})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
