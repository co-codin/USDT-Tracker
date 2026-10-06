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

// Transfer is a decoded TRC-20 Transfer(address,address,uint256) event.
type Transfer struct {
	TxID        string    // transaction hash (hex, no 0x)
	LogIndex    int       // position of the log inside the transaction's log list
	BlockNumber int64     // block height
	BlockTime   time.Time // block timestamp (UTC)
	Contract    string    // token contract, base58check (T...)
	Symbol      string    // token symbol, e.g. USDT
	Decimals    int       // token decimals, 6 for USDT
	From        string    // sender, base58check
	To          string    // recipient, base58check
	Amount      *big.Int  // raw amount in base units
	Reasons     []string  // why the filter matched (set by filter.Filter)
}

// Key is the idempotency / dedupe key of a transfer: "<tx_id>:<log_index>".
func (t Transfer) Key() string {
	return t.TxID + ":" + strconv.Itoa(t.LogIndex)
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
		TxID: j.TxID, LogIndex: j.LogIndex, BlockNumber: j.BlockNumber, BlockTime: j.BlockTime,
		Contract: j.Contract, Symbol: j.Symbol, Decimals: j.Decimals, From: j.From, To: j.To,
		Amount: amt, Reasons: j.Reasons,
	}
	return nil
}
