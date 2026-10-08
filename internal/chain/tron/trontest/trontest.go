// Package trontest provides an in-process mock of the TRON full-node HTTP API
// (the subset used by the listener) for integration tests. It serves a
// deterministic synthetic chain and supports fault injection (HTTP errors,
// rate limiting, lagging-node empty responses).
package trontest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"
)

// Well-known addresses used by the synthetic chain (hex without 0x41 prefix).
const (
	USDTHex  = "a614f803b6fd780986a42c78ec9c7f77e6ded13c" // TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t
	USDCHex  = "3487b63d30b5b2c87fb7ffa8bcfade38eaac1abe" // another TRC-20 (must be ignored)
	RouterEx = "39dd12a54e2bab7c82aa14a1e158b34263d2d510" // some contract emitting non-Transfer logs

	AliceHex = "f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07" // TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx
	BobHex   = "7452f02038a6039b730c7ec929a3380ff1b4a6e7" // TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv
	CarolHex = "2f6b52ce0076d4a0245cd73f255279237dd60dff" // TEHwL3F2kpkJExYAZYziHGLFR2CR8Bq3bo
	DaveHex  = "76b5c8429b78a38643e5ff9a94b4ca10c1efd867" // TLntW9Z59LYY5KEi9cmwk3PKjQga828ird

	Alice = "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"
	Bob   = "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"
	Carol = "TEHwL3F2kpkJExYAZYziHGLFR2CR8Bq3bo"
	Dave  = "TLntW9Z59LYY5KEi9cmwk3PKjQga828ird"

	transferTopic = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

// GenesisTime is the timestamp of block 0 of the synthetic chain.
var GenesisTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Expected describes one USDT transfer the synthetic chain contains.
type Expected struct {
	Key    string // "<tx_id>:<log_index>"
	Block  int64
	From   string
	To     string
	Amount *big.Int // raw units
}

// Server is a mock TRON HTTP API.
type Server struct {
	*httptest.Server

	mu         sync.Mutex
	head       int64
	faults     map[int64][]int // tx-info status codes to return before succeeding
	emptyOnce  map[int64]bool  // return [] once (simulates a lagging backend)
	txInfoReqs map[int64]int
	maxReq     int64
}

// NewServer starts a mock node whose head is head.
func NewServer(head int64) *Server {
	s := &Server{head: head, faults: map[int64][]int{}, emptyOnce: map[int64]bool{}, txInfoReqs: map[int64]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /wallet/getblock", s.getBlock)
	mux.HandleFunc("POST /wallet/getblockbynum", s.getBlockByNum)
	mux.HandleFunc("POST /wallet/gettransactioninfobyblocknum", s.getTxInfo)
	s.Server = httptest.NewServer(mux)
	return s
}

// SetHead moves the chain head.
func (s *Server) SetHead(h int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.head = h
}

// FailBlock makes the next len(codes) tx-info requests for block n return
// the given HTTP status codes (429 responses carry "Retry-After: 0").
func (s *Server) FailBlock(n int64, codes ...int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[n] = append(s.faults[n], codes...)
}

// EmptyOnce makes the next tx-info request for block n return [] although the
// block has transactions (lagging node behind a load balancer).
func (s *Server) EmptyOnce(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emptyOnce[n] = true
}

// MaxRequestedBlock returns the highest block whose tx info was requested.
func (s *Server) MaxRequestedBlock() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxReq
}

// TxInfoRequests returns how many tx-info requests block n received.
func (s *Server) TxInfoRequests(n int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.txInfoReqs[n]
}

// IsEmptyBlock reports whether the synthetic block n has no transactions
// (every 10th block, to exercise the empty-block path).
func IsEmptyBlock(n int64) bool { return n%10 == 7 }

// TxID returns the deterministic id of transaction i in block n.
func TxID(n int64, i int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("block-%d-tx-%d", n, i)))
	return hex.EncodeToString(h[:])
}

// ExpectedTransfers lists the USDT transfers contained in blocks [from, to].
func ExpectedTransfers(from, to int64) []Expected {
	var out []Expected
	for n := from; n <= to; n++ {
		if IsEmptyBlock(n) {
			continue
		}
		out = append(out,
			Expected{TxID(n, 0) + ":0", n, Alice, Bob, smallAmount(n)},
			Expected{TxID(n, 1) + ":0", n, Carol, Dave, largeAmount(n)},
			Expected{TxID(n, 1) + ":2", n, Dave, Alice, big.NewInt(1)},
		)
	}
	return out
}

// smallAmount is n + 0.5 USDT; largeAmount is 100k + n USDT.
func smallAmount(n int64) *big.Int { return big.NewInt(n*1_000_000 + 500_000) }
func largeAmount(n int64) *big.Int {
	return new(big.Int).Mul(big.NewInt(100_000+n), big.NewInt(1_000_000))
}

type log struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

type txInfo struct {
	ID              string         `json:"id"`
	BlockNumber     int64          `json:"blockNumber"`
	BlockTimeStamp  int64          `json:"blockTimeStamp"`
	ContractAddress string         `json:"contract_address,omitempty"`
	Receipt         map[string]any `json:"receipt"`
	Log             []log          `json:"log,omitempty"`
}

func topicAddr(h string) string { return "000000000000000000000000" + h }
func word(v *big.Int) string    { return fmt.Sprintf("%064x", v) }

func transfer(contract, from, to string, amount *big.Int) log {
	return log{Address: contract, Topics: []string{transferTopic, topicAddr(from), topicAddr(to)}, Data: word(amount)}
}

// blockTxs builds the synthetic transactions of block n:
//
//	tx0: plain USDT transfer Alice -> Bob (small)
//	tx1: "swap": USDT Carol -> Dave (large), router event, USDT Dave -> Alice (1 unit), USDC transfer
//	tx2: reverted USDT transfer (must be ignored)
//	tx3: USDC-only transfer (must be ignored)
func blockTxs(n int64) []txInfo {
	if IsEmptyBlock(n) {
		return nil
	}
	ts := blockTime(n).UnixMilli()
	ok := map[string]any{"result": "SUCCESS", "energy_usage_total": 64285}
	return []txInfo{
		{ID: TxID(n, 0), BlockNumber: n, BlockTimeStamp: ts, ContractAddress: "41" + USDTHex, Receipt: ok,
			Log: []log{transfer(USDTHex, AliceHex, BobHex, smallAmount(n))}},
		{ID: TxID(n, 1), BlockNumber: n, BlockTimeStamp: ts, ContractAddress: "41" + RouterEx, Receipt: ok,
			Log: []log{
				transfer(USDTHex, CarolHex, DaveHex, largeAmount(n)),
				{Address: RouterEx, Topics: []string{"fe6f7f85" + "00000000000000000000000000000000000000000000000000000000"}, Data: word(big.NewInt(103))},
				transfer(USDTHex, DaveHex, AliceHex, big.NewInt(1)),
				transfer(USDCHex, AliceHex, BobHex, big.NewInt(5)),
			}},
		{ID: TxID(n, 2), BlockNumber: n, BlockTimeStamp: ts, ContractAddress: "41" + USDTHex,
			Receipt: map[string]any{"result": "REVERT"},
			Log:     []log{transfer(USDTHex, AliceHex, BobHex, big.NewInt(999))}},
		{ID: TxID(n, 3), BlockNumber: n, BlockTimeStamp: ts, ContractAddress: "41" + USDCHex, Receipt: ok,
			Log: []log{transfer(USDCHex, BobHex, CarolHex, big.NewInt(7))}},
	}
}

func blockTime(n int64) time.Time { return GenesisTime.Add(time.Duration(n) * 3 * time.Second) }

func readNum(r *http.Request) (int64, bool) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return 0, false
	}
	switch v := body["num"].(type) {
	case float64:
		return int64(v), true
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	}
	return 0, false
}

func blockJSON(n int64, txCount int) map[string]any {
	txs := make([]map[string]string, txCount)
	for i := range txs {
		txs[i] = map[string]string{"txID": TxID(n, i)}
	}
	return map[string]any{
		"blockID": fmt.Sprintf("%016x%048x", n, n),
		"block_header": map[string]any{"raw_data": map[string]any{
			"number": n, "timestamp": blockTime(n).UnixMilli(),
		}},
		"transactions": txs,
	}
}

func (s *Server) getBlock(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	head := s.head
	s.mu.Unlock()
	b := blockJSON(head, 0)
	delete(b, "transactions")
	writeJSON(w, b)
}

func (s *Server) getBlockByNum(w http.ResponseWriter, r *http.Request) {
	n, ok := readNum(r)
	if !ok {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	head := s.head
	s.mu.Unlock()
	if n > head {
		writeJSON(w, map[string]any{})
		return
	}
	writeJSON(w, blockJSON(n, len(blockTxs(n))))
}

func (s *Server) getTxInfo(w http.ResponseWriter, r *http.Request) {
	n, ok := readNum(r)
	if !ok {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.txInfoReqs[n]++
	if n > s.maxReq {
		s.maxReq = n
	}
	head := s.head
	var code int
	if f := s.faults[n]; len(f) > 0 {
		code, s.faults[n] = f[0], f[1:]
	}
	empty := s.emptyOnce[n]
	delete(s.emptyOnce, n)
	s.mu.Unlock()

	switch {
	case code == http.StatusTooManyRequests:
		w.Header().Set("Retry-After", "0")
		http.Error(w, `{"Error":"rate limited"}`, code)
	case code != 0:
		http.Error(w, "injected failure", code)
	case n > head || empty:
		writeJSON(w, []any{})
	default:
		txs := blockTxs(n)
		if txs == nil {
			writeJSON(w, []any{})
			return
		}
		writeJSON(w, txs)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
