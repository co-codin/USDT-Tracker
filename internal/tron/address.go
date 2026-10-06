// Package tron contains TRON primitives (addresses, base58check) and a small,
// read-only client for the TRON full-node HTTP API (TronGrid compatible).
package tron

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// AddressPrefix is the first byte of every TRON mainnet address (0x41).
const AddressPrefix byte = 0x41

// AddressLen is the length of a raw TRON address (prefix + 20 bytes).
const AddressLen = 21

// Address is a raw 21-byte TRON address (0x41 || 20-byte account id).
type Address [AddressLen]byte

var (
	// ErrInvalidAddress is returned for malformed addresses.
	ErrInvalidAddress = errors.New("invalid TRON address")
	// ErrChecksum is returned when a base58check checksum does not match.
	ErrChecksum = errors.New("invalid base58check checksum")
)

// AddressFromBytes builds an address from a 20-byte EVM-style account id or a
// 21-byte TRON address starting with 0x41.
func AddressFromBytes(b []byte) (Address, error) {
	var a Address
	switch {
	case len(b) == 20:
		a[0] = AddressPrefix
		copy(a[1:], b)
	case len(b) == AddressLen && b[0] == AddressPrefix:
		copy(a[:], b)
	default:
		return a, fmt.Errorf("%w: unexpected length %d", ErrInvalidAddress, len(b))
	}
	return a, nil
}

// AddressFromHex parses "41…" (42 hex chars) or a 40-char EVM-style hex
// address, with or without a 0x prefix.
func AddressFromHex(s string) (Address, error) {
	s = strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(s), "0x"), "0X")
	b, err := hex.DecodeString(s)
	if err != nil {
		return Address{}, fmt.Errorf("%w: %w", ErrInvalidAddress, err)
	}
	return AddressFromBytes(b)
}

// AddressFromTopic extracts an address from a 32-byte ABI-encoded log topic
// (64 hex chars, the address is right-aligned in the last 20 bytes).
func AddressFromTopic(topic string) (Address, error) {
	topic = strings.TrimPrefix(topic, "0x")
	if len(topic) != 64 {
		return Address{}, fmt.Errorf("%w: topic length %d", ErrInvalidAddress, len(topic))
	}
	if strings.Trim(topic[:24], "0") != "" {
		return Address{}, fmt.Errorf("%w: topic has non-zero padding", ErrInvalidAddress)
	}
	return AddressFromHex(topic[24:])
}

// ParseAddress accepts a base58check ("T…") or hex ("41…") address.
func ParseAddress(s string) (Address, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "T") && len(s) == 34 {
		return AddressFromBase58(s)
	}
	return AddressFromHex(s)
}

// AddressFromBase58 decodes a base58check TRON address ("T…").
func AddressFromBase58(s string) (Address, error) {
	raw, err := base58Decode(s)
	if err != nil {
		return Address{}, fmt.Errorf("%w: %w", ErrInvalidAddress, err)
	}
	if len(raw) != AddressLen+4 {
		return Address{}, fmt.Errorf("%w: decoded length %d", ErrInvalidAddress, len(raw))
	}
	payload, sum := raw[:AddressLen], raw[AddressLen:]
	if !bytes.Equal(checksum(payload), sum) {
		return Address{}, ErrChecksum
	}
	return AddressFromBytes(payload)
}

// String returns the base58check representation ("T…").
func (a Address) String() string {
	buf := make([]byte, 0, AddressLen+4)
	buf = append(buf, a[:]...)
	buf = append(buf, checksum(a[:])...)
	return base58Encode(buf)
}

// Hex returns the 42-char hex representation starting with "41".
func (a Address) Hex() string { return hex.EncodeToString(a[:]) }

// EVMHex returns the 40-char hex account id without the 0x41 prefix, which is
// the form used in event log "address" fields.
func (a Address) EVMHex() string { return hex.EncodeToString(a[1:]) }

// IsZero reports whether the account id is all zeros (mint/burn address).
func (a Address) IsZero() bool {
	for _, b := range a[1:] {
		if b != 0 {
			return false
		}
	}
	return true
}

func checksum(b []byte) []byte {
	h1 := sha256.Sum256(b)
	h2 := sha256.Sum256(h1[:])
	return h2[:4]
}
