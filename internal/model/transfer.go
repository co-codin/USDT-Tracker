// Package model contains the domain types shared across the listener.
package model

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"
)

// Reasons attached to a transfer by the filter.
const (
	ReasonAll      = "all"
	ReasonIncoming = "watch_incoming"
	ReasonOutgoing = "watch_outgoing"
	ReasonLarge    = "large_transfer"
)

// Supported chains.
const (
	ChainTron = "tron"
	ChainTON  = "ton"
	ChainETH  = "eth"
)

// Transfer is a decoded token transfer: a TRC-20 Transfer(address,address,uint256)
// event on TRON or a jetton transfer on TON.
type Transfer struct {
	Chain       string    // "tron" (default when empty), "ton" or "eth"
	TxID        string    // transaction hash: hex without 0x on TRON/TON, 0x-prefixed lowercase on Ethereum
	LogIndex    int       // position of the log inside the transaction's log list
	BlockNumber int64     // block height
	BlockTime   time.Time // block timestamp (UTC)
	Contract    string    // token contract: base58check (T...) on TRON, jetton master raw address on TON, lowercase 0x on Ethereum
	Symbol      string    // token symbol, e.g. USDT
	Decimals    int       // token decimals, 6 for USDT
	From        string    // sender: base58check on TRON, raw "0:<hex>" owner address on TON, lowercase 0x on Ethereum
	To          string    // recipient (same formats as From)
	Amount      *big.Int  // raw amount in base units
	Memo        string    // TON only: text comment of the transfer (forward payload), often an invoice id
	Reasons     []string  // why the filter matched (set by filter.Filter)
}

// ChainName returns the chain, defaulting to "tron".
func (t Transfer) ChainName() string {
	if t.Chain == "" {
		return ChainTron
	}
	return t.Chain
}

// Key is the idempotency / dedupe key of a transfer: "<tx_id>:<log_index>"
// on TRON (unchanged for compatibility) and "ton:<tx_hash>:<index>" on TON.
func (t Transfer) Key() string {
	k := t.TxID + ":" + strconv.Itoa(t.LogIndex)
	if c := t.ChainName(); c != ChainTron {
		k = c + ":" + k
	}
	return k
}

// AmountDecimal returns the human readable amount, e.g. "1250.5".
func (t Transfer) AmountDecimal() string {
	return FormatUnits(t.Amount, t.Decimals)
}

// HasReason reports whether the filter tagged the transfer with reason r.
func (t Transfer) HasReason(r string) bool {
	for _, x := range t.Reasons {
		if x == r {
			return true
		}
	}
	return false
}

type transferJSON struct {
	ID          string    `json:"id"`
	Chain       string    `json:"chain"`
	TxID        string    `json:"tx_id"`
	LogIndex    int       `json:"log_index"`
	BlockNumber int64     `json:"block_number"`
	BlockTime   time.Time `json:"block_time"`
	Contract    string    `json:"contract"`
	Symbol      string    `json:"symbol"`
	Decimals    int       `json:"decimals"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	Amount      string    `json:"amount"`
	AmountRaw   string    `json:"amount_raw"`
	Memo        string    `json:"memo,omitempty"`
	Reasons     []string  `json:"reasons,omitempty"`
}

// MarshalJSON renders the stable public JSON shape used by the webhook sink.
func (t Transfer) MarshalJSON() ([]byte, error) {
	raw := "0"
	if t.Amount != nil {
		raw = t.Amount.String()
	}
	return json.Marshal(transferJSON{
		ID:          t.Key(),
		Chain:       t.ChainName(),
		TxID:        t.TxID,
		LogIndex:    t.LogIndex,
		BlockNumber: t.BlockNumber,
		BlockTime:   t.BlockTime.UTC(),
		Contract:    t.Contract,
		Symbol:      t.Symbol,
		Decimals:    t.Decimals,
		From:        t.From,
		To:          t.To,
		Amount:      t.AmountDecimal(),
		AmountRaw:   raw,
		Memo:        t.Memo,
		Reasons:     t.Reasons,
	})
}

// UnmarshalJSON parses the public JSON shape (useful for webhook consumers).
func (t *Transfer) UnmarshalJSON(b []byte) error {
	var j transferJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	amt, ok := new(big.Int).SetString(j.AmountRaw, 10)
	if !ok {
		return fmt.Errorf("invalid amount_raw %q", j.AmountRaw)
	}
	*t = Transfer{
		Chain: j.Chain, Memo: j.Memo,
		TxID: j.TxID, LogIndex: j.LogIndex, BlockNumber: j.BlockNumber, BlockTime: j.BlockTime,
		Contract: j.Contract, Symbol: j.Symbol, Decimals: j.Decimals, From: j.From, To: j.To,
		Amount: amt, Reasons: j.Reasons,
	}
	return nil
}
