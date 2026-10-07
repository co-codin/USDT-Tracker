// Package decoder turns raw TRON event logs into TRC-20 Transfer events.
package decoder

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/tron"
)

// TransferTopic is keccak256("Transfer(address,address,uint256)").
const TransferTopic = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// ErrMalformedLog is returned for logs emitted by the token contract with the
// Transfer topic that cannot be decoded.
var ErrMalformedLog = errors.New("malformed Transfer log")

// Token describes the TRC-20 contract being watched.
type Token struct {
	Contract tron.Address
	Symbol   string
	Decimals int
}

// Decoder decodes Transfer events of a single TRC-20 token.
type Decoder struct {
	token       Token
	contractHex string // 40-char lowercase hex as it appears in log.address
	contractB58 string
}

// New creates a decoder for the given token.
func New(t Token) *Decoder {
	return &Decoder{token: t, contractHex: t.Contract.EVMHex(), contractB58: t.Contract.String()}
}

// Token returns the token the decoder is bound to.
func (d *Decoder) Token() Token { return d.token }

// DecodeBlock decodes all Transfer events from a block's transaction infos.
// Failed transactions are skipped. Malformed logs are skipped and returned as
// errors so that one bad log never stalls the pipeline.
func (d *Decoder) DecodeBlock(infos []tron.TransactionInfo) ([]model.Transfer, []error) {
	var (
		out  []model.Transfer
		errs []error
	)
	for _, info := range infos {
		ts, es := d.DecodeTx(info)
		out = append(out, ts...)
		errs = append(errs, es...)
	}
	return out, errs
}

// DecodeTx decodes all Transfer events of a single transaction.
func (d *Decoder) DecodeTx(info tron.TransactionInfo) ([]model.Transfer, []error) {
	if !info.Succeeded() || len(info.Log) == 0 {
		return nil, nil
	}
	var (
		out  []model.Transfer
		errs []error
	)
	for i, l := range info.Log {
		t, ok, err := d.DecodeLog(l)
		if err != nil {
			errs = append(errs, fmt.Errorf("tx %s log %d: %w", info.ID, i, err))
			continue
		}
		if !ok {
			continue
		}
		t.TxID = strings.ToLower(info.ID)
		t.LogIndex = i
		t.BlockNumber = info.BlockNumber
		t.BlockTime = info.BlockTime()
		out = append(out, t)
	}
	return out, errs
}

// DecodeLog decodes a single log. ok is false when the log is not a Transfer
// event of the watched token. TxID/LogIndex/Block fields are left empty.
func (d *Decoder) DecodeLog(l tron.Log) (t model.Transfer, ok bool, err error) {
	if normalizeLogAddress(l.Address) != d.contractHex {
		return t, false, nil
	}
	if len(l.Topics) == 0 || strings.ToLower(strings.TrimPrefix(l.Topics[0], "0x")) != TransferTopic {
		return t, false, nil
	}
	if len(l.Topics) != 3 {
		return t, false, fmt.Errorf("%w: expected 3 topics, got %d", ErrMalformedLog, len(l.Topics))
	}
	from, err := tron.AddressFromTopic(l.Topics[1])
	if err != nil {
		return t, false, fmt.Errorf("%w: from: %w", ErrMalformedLog, err)
	}
	to, err := tron.AddressFromTopic(l.Topics[2])
	if err != nil {
		return t, false, fmt.Errorf("%w: to: %w", ErrMalformedLog, err)
	}
	amount, err := DecodeUint256(l.Data)
	if err != nil {
		return t, false, fmt.Errorf("%w: amount: %w", ErrMalformedLog, err)
	}
	return model.Transfer{
		Contract: d.contractB58,
		Symbol:   d.token.Symbol,
		Decimals: d.token.Decimals,
		From:     from.String(),
		To:       to.String(),
		Amount:   amount,
	}, true, nil
}

// DecodeUint256 decodes the first ABI word of hex data as an unsigned integer.
func DecodeUint256(data string) (*big.Int, error) {
	data = strings.TrimPrefix(data, "0x")
	if len(data) < 64 {
		return nil, fmt.Errorf("data too short: %d hex chars", len(data))
	}
	b, err := hex.DecodeString(data[:64])
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(b), nil
}

// normalizeLogAddress converts "41…"/"0x…"/"…" forms into 40-char lowercase hex.
func normalizeLogAddress(a string) string {
	a = strings.ToLower(strings.TrimPrefix(a, "0x"))
	if len(a) == 42 && strings.HasPrefix(a, "41") {
		a = a[2:]
	}
	return a
}
