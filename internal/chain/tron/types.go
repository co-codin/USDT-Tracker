package tron

import (
	"encoding/json"
	"time"
)

// BlockHeader is the subset of a block header the listener needs.
type BlockHeader struct {
	ID        string
	Number    int64
	Timestamp time.Time
}

// Block is a block header plus its transaction count.
type Block struct {
	BlockHeader
	TxCount int
}

type blockResponse struct {
	BlockID     string `json:"blockID"`
	BlockHeader struct {
		RawData struct {
			Number    int64 `json:"number"`
			Timestamp int64 `json:"timestamp"`
		} `json:"raw_data"`
	} `json:"block_header"`
	Transactions []json.RawMessage `json:"transactions"`
}

func (b blockResponse) header() BlockHeader {
	return BlockHeader{
		ID:        b.BlockID,
		Number:    b.BlockHeader.RawData.Number,
		Timestamp: time.UnixMilli(b.BlockHeader.RawData.Timestamp).UTC(),
	}
}

// TransactionInfo is the response item of /wallet/gettransactioninfobyblocknum.
type TransactionInfo struct {
	ID              string `json:"id"`
	BlockNumber     int64  `json:"blockNumber"`
	BlockTimeStamp  int64  `json:"blockTimeStamp"`
	ContractAddress string `json:"contract_address"`
	// Result is "FAILED" for failed transactions (absent on success).
	Result  string  `json:"result"`
	Receipt Receipt `json:"receipt"`
	Log     []Log   `json:"log"`
}

// Receipt holds the execution result of a smart contract call.
type Receipt struct {
	// Result is "SUCCESS" for successful contract calls, e.g. "REVERT" otherwise.
	Result string `json:"result"`
}

// Succeeded reports whether the transaction executed successfully.
func (t TransactionInfo) Succeeded() bool {
	if t.Result == "FAILED" {
		return false
	}
	return t.Receipt.Result == "" || t.Receipt.Result == "SUCCESS"
}

// BlockTime returns the block timestamp as time.Time (UTC).
func (t TransactionInfo) BlockTime() time.Time {
	return time.UnixMilli(t.BlockTimeStamp).UTC()
}

// Log is an EVM-style event log as returned by the TRON HTTP API. Address is
// a 40-char hex account id (no 0x41 prefix); topics and data are hex strings.
type Log struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}
