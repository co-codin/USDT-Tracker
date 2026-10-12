// Package eth is a minimal, read-only Ethereum JSON-RPC client plus address
// helpers. It never signs anything and never handles private keys: it only
// calls eth_blockNumber and eth_getLogs.
package eth

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/sha3"
)

// USDTContract is Tether USD (ERC-20, 6 decimals) on Ethereum mainnet.
const USDTContract = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

// TransferTopic is keccak256("Transfer(address,address,uint256)").
const TransferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// ErrInvalidAddress is returned for malformed Ethereum addresses.
var ErrInvalidAddress = errors.New("invalid ethereum address")

// Keccak256 returns the legacy Keccak-256 hash used by Ethereum.
func Keccak256(b []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(b)
	return h.Sum(nil)
}

// NormalizeAddress validates a 0x-prefixed 20-byte hex address and returns
// its canonical lowercase form. All-lowercase and all-uppercase hex are
// accepted as is; mixed case must carry a valid EIP-55 checksum.
func NormalizeAddress(s string) (string, error) {
	s = strings.TrimSpace(s)
	if len(s) != 42 || (s[:2] != "0x" && s[:2] != "0X") {
		return "", fmt.Errorf("%w %q: want 0x followed by 40 hex characters", ErrInvalidAddress, s)
	}
	body := s[2:]
	if _, err := hex.DecodeString(body); err != nil {
		return "", fmt.Errorf("%w %q: not hex", ErrInvalidAddress, s)
	}
	lower := strings.ToLower(body)
	if body != lower && body != strings.ToUpper(body) {
		if Checksum("0x"+lower) != "0x"+body {
			return "", fmt.Errorf("%w %q: bad EIP-55 checksum (use the address exactly as shown on etherscan, or all lowercase)", ErrInvalidAddress, s)
		}
	}
	return "0x" + lower, nil
}

// IsAddress reports whether s is a valid Ethereum address.
func IsAddress(s string) bool {
	_, err := NormalizeAddress(s)
	return err == nil
}

// Checksum returns the EIP-55 mixed-case form of a valid address (the input
// is returned unchanged if it is not a valid address).
func Checksum(s string) string {
	if len(s) != 42 {
		return s
	}
	lower := strings.ToLower(s[2:])
	if _, err := hex.DecodeString(lower); err != nil {
		return s
	}
	h := hex.EncodeToString(Keccak256([]byte(lower)))
	out := []byte(lower)
	for i, c := range out {
		if c >= 'a' && c <= 'f' && h[i] >= '8' {
			out[i] = c - 32
		}
	}
	return "0x" + string(out)
}

// TopicAddress decodes an address from a 32-byte topic ("0x" + 64 hex,
// left-padded with zeros) into canonical lowercase form.
func TopicAddress(topic string) (string, error) {
	t := strings.ToLower(strings.TrimPrefix(topic, "0x"))
	if len(t) != 64 {
		return "", fmt.Errorf("topic %q: want 32 bytes", topic)
	}
	if strings.Trim(t[:24], "0") != "" {
		return "", fmt.Errorf("topic %q: not an address (non-zero padding)", topic)
	}
	if _, err := hex.DecodeString(t); err != nil {
		return "", fmt.Errorf("topic %q: not hex", topic)
	}
	return "0x" + t[24:], nil
}
