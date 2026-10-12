package ton

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Uint64String decodes a JSON string or number holding a uint64 (toncenter
// encodes logical times and utimes as strings).
type Uint64String uint64

// UnmarshalJSON implements json.Unmarshaler.
func (u *Uint64String) UnmarshalJSON(b []byte) error {
	var s string
	if len(b) > 0 && b[0] == '"' {
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
	} else {
		s = string(b)
	}
	if s == "" || s == "null" {
		*u = 0
		return nil
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("ton: bad integer %q", s)
	}
	*u = Uint64String(n)
	return nil
}

// MasterchainInfo is the /api/v3/masterchainInfo response.
type MasterchainInfo struct {
	Last  BlockRef `json:"last"`
	First BlockRef `json:"first"`
}

// BlockRef is the subset of a block header used by the listener.
type BlockRef struct {
	Workchain int32        `json:"workchain"`
	Shard     string       `json:"shard"`
	Seqno     int64        `json:"seqno"`
	GenUtime  Uint64String `json:"gen_utime"`
	StartLt   Uint64String `json:"start_lt"`
	EndLt     Uint64String `json:"end_lt"`
	TxCount   int          `json:"tx_count"`
}

// Time returns the block generation time (UTC).
func (b BlockRef) Time() time.Time { return time.Unix(int64(b.GenUtime), 0).UTC() } //nolint:gosec // G115: unix seconds fit

// BlocksResponse is the /api/v3/masterchainBlockShards response.
type BlocksResponse struct {
	Blocks []BlockRef `json:"blocks"`
}

// JettonTransfer is one item of /api/v3/jetton/transfers.
type JettonTransfer struct {
	QueryID            string          `json:"query_id"`
	Source             string          `json:"source"`      // sender owner (raw)
	Destination        string          `json:"destination"` // recipient owner (raw)
	Amount             string          `json:"amount"`
	SourceWallet       string          `json:"source_wallet"` // sender's jetton wallet (raw); the tx runs on it
	JettonMaster       string          `json:"jetton_master"`
	TransactionHash    string          `json:"transaction_hash"` // base64
	TransactionLt      Uint64String    `json:"transaction_lt"`
	TransactionNow     int64           `json:"transaction_now"`
	TransactionAborted bool            `json:"transaction_aborted"`
	TraceID            string          `json:"trace_id"`
	DecodedForward     *DecodedPayload `json:"decoded_forward_payload"`
}

// DecodedPayload is toncenter's decoded forward payload (only text comments are used).
type DecodedPayload struct {
	Type    string `json:"type"`
	Comment string `json:"comment"`
}

// Comment returns the text comment attached to the transfer (memo), if any.
func (t JettonTransfer) Comment() string {
	if t.DecodedForward != nil && t.DecodedForward.Type == "text_comment" {
		return t.DecodedForward.Comment
	}
	return ""
}

// JettonTransfersResponse is the /api/v3/jetton/transfers response.
type JettonTransfersResponse struct {
	JettonTransfers []JettonTransfer `json:"jetton_transfers"`
}

// HashHex converts a base64 (std or url) or hex transaction hash to lowercase hex.
func HashHex(h string) (string, error) {
	if len(h) == 64 {
		if b, err := hex.DecodeString(h); err == nil {
			return hex.EncodeToString(b), nil
		}
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(h); err == nil && len(b) == 32 {
			return hex.EncodeToString(b), nil
		}
	}
	return "", fmt.Errorf("ton: bad transaction hash %q", h)
}
