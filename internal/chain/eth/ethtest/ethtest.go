// Package ethtest provides an in-process mock Ethereum JSON-RPC node serving
// a deterministic synthetic USDT chain: every block n carries (n % 3) USDT
// Transfer logs plus one log of another contract, with fault injection
// (HTTP 429 with Retry-After, max eth_getLogs range).
package ethtest

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Synthetic addresses (lowercase).
var (
	USDT  = "0xdac17f958d2ee523a2206206994597c13d831ec7"
	Other = "0x" + strings.Repeat("ee", 20)
	Alice = "0x" + strings.Repeat("11", 20)
	Bob   = "0x" + strings.Repeat("22", 20)
)

const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// GenesisTime is the timestamp of block 0; blocks are 12 s apart.
const GenesisTime = 1_700_000_000

// Node is a mock JSON-RPC node.
type Node struct {
	*httptest.Server
	Head          atomic.Int64
	MaxRange      int64 // eth_getLogs ranges wider than this fail ("block range too large"); 0 = unlimited
	OmitTimestamp bool  // omit blockTimestamp from logs (older nodes)
	mu            sync.Mutex
	fail429       int
	calls         map[string]int
	ranges        [][2]int64
}

// New starts a node with the given head.
func New(head int64) *Node {
	n := &Node{calls: map[string]int{}}
	n.Head.Store(head)
	n.Server = httptest.NewServer(http.HandlerFunc(n.serve))
	return n
}

// Fail429 makes the next k requests return HTTP 429 with Retry-After: 0.
func (n *Node) Fail429(k int) { n.mu.Lock(); n.fail429 = k; n.mu.Unlock() }

// Calls returns how many times method was called.
func (n *Node) Calls(method string) int { n.mu.Lock(); defer n.mu.Unlock(); return n.calls[method] }

// Ranges returns the eth_getLogs ranges requested so far.
func (n *Node) Ranges() [][2]int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([][2]int64(nil), n.ranges...)
}

// Amount is the raw amount of the i-th USDT transfer in block b.
func Amount(b int64, i int) *big.Int {
	return new(big.Int).Mul(big.NewInt(b*10+int64(i)+1), big.NewInt(1_000_000))
}

// TxHash is the synthetic hash of transfer i in block b.
func TxHash(b int64, i int) string { return fmt.Sprintf("0x%060x%04x", b, i) }

func topic(addr string) string {
	return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(addr, "0x")
}

type req struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

func (n *Node) serve(w http.ResponseWriter, r *http.Request) {
	var q req
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	n.calls[q.Method]++
	if n.fail429 > 0 {
		n.fail429--
		n.mu.Unlock()
		w.Header().Set("Retry-After", "0")
		http.Error(w, "slow down", http.StatusTooManyRequests)
		return
	}
	n.mu.Unlock()
	reply := func(result any, rpcErr map[string]any) {
		out := map[string]any{"jsonrpc": "2.0", "id": q.ID}
		if rpcErr != nil {
			out["error"] = rpcErr
		} else {
			out["result"] = result
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
	switch q.Method {
	case "eth_blockNumber":
		reply(hexq(n.Head.Load()), nil)
	case "eth_getBlockByNumber":
		var num string
		_ = json.Unmarshal(q.Params[0], &num)
		b, _ := strconv.ParseInt(strings.TrimPrefix(num, "0x"), 16, 64)
		if b > n.Head.Load() {
			reply(nil, nil)
			return
		}
		reply(map[string]any{"number": num, "timestamp": hexq(GenesisTime + 12*b)}, nil)
	case "eth_getLogs":
		var f struct {
			FromBlock string   `json:"fromBlock"`
			ToBlock   string   `json:"toBlock"`
			Address   string   `json:"address"`
			Topics    []string `json:"topics"`
		}
		_ = json.Unmarshal(q.Params[0], &f)
		from, _ := strconv.ParseInt(strings.TrimPrefix(f.FromBlock, "0x"), 16, 64)
		to, _ := strconv.ParseInt(strings.TrimPrefix(f.ToBlock, "0x"), 16, 64)
		n.mu.Lock()
		n.ranges = append(n.ranges, [2]int64{from, to})
		n.mu.Unlock()
		if n.MaxRange > 0 && to-from+1 > n.MaxRange {
			reply(nil, map[string]any{"code": -32000, "message": "block range too large"})
			return
		}
		logs := []map[string]any{}
		for b := from; b <= to && b <= n.Head.Load(); b++ {
			for i := 0; i < int(b%3); i++ {
				if strings.ToLower(f.Address) != USDT {
					continue
				}
				src, dst := Alice, Bob
				if i == 1 {
					src, dst = Bob, Alice
				}
				lg := map[string]any{
					"address": USDT, "topics": []string{transferTopic, topic(src), topic(dst)},
					"data": fmt.Sprintf("0x%064x", Amount(b, i)), "blockNumber": hexq(b),
					"blockHash": fmt.Sprintf("0x%064x", b), "transactionHash": TxHash(b, i),
					"transactionIndex": hexq(int64(i)), "logIndex": hexq(int64(i * 2)), "removed": false,
				}
				if !n.OmitTimestamp {
					lg["blockTimestamp"] = hexq(GenesisTime + 12*b)
				}
				logs = append(logs, lg)
			}
		}
		reply(logs, nil)
	default:
		reply(nil, map[string]any{"code": -32601, "message": "method not found"})
	}
}

func hexq(n int64) string { return "0x" + strconv.FormatInt(n, 16) }
