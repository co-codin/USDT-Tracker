// Package ton is a minimal, read-only client for the TON Index HTTP API
// (toncenter v3) plus TON address helpers. It never signs or sends messages.
package ton

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// USDTMaster is the official Tether USD₮ jetton master on TON mainnet
// (https://tether.to/en/supported-protocols, toncenter metadata "Tether USD").
const USDTMaster = "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs"

// ErrInvalidAddress is returned for malformed TON addresses.
var ErrInvalidAddress = errors.New("ton: invalid address")

// Address is a TON standard address (workchain + 256-bit account id).
type Address struct {
	Workchain int32
	Hash      [32]byte
}

const (
	tagBounceable    = 0x11
	tagNonBounceable = 0x51
	tagTestOnly      = 0x80
)

// ParseAddress accepts the raw form ("0:83df…", any hex case) and the 48-char
// user-friendly form (base64 or base64url, bounceable or not, with a valid
// CRC16 checksum).
func ParseAddress(s string) (Address, error) {
	s = strings.TrimSpace(s)
	if wc, h, ok := strings.Cut(s, ":"); ok {
		n, err := strconv.ParseInt(wc, 10, 32)
		if err != nil {
			return Address{}, fmt.Errorf("%w: bad workchain %q", ErrInvalidAddress, wc)
		}
		b, err := hex.DecodeString(h)
		if err != nil || len(b) != 32 {
			return Address{}, fmt.Errorf("%w: want 64 hex characters after the workchain", ErrInvalidAddress)
		}
		var a Address
		a.Workchain = int32(n)
		copy(a.Hash[:], b)
		return a, nil
	}
	if len(s) != 48 {
		return Address{}, fmt.Errorf("%w: want raw \"wc:hex\" or a 48-character user-friendly address", ErrInvalidAddress)
	}
	std := strings.NewReplacer("-", "+", "_", "/").Replace(s)
	b, err := base64.StdEncoding.DecodeString(std)
	if err != nil || len(b) != 36 {
		return Address{}, fmt.Errorf("%w: bad base64", ErrInvalidAddress)
	}
	if crc16(b[:34]) != binary.BigEndian.Uint16(b[34:]) {
		return Address{}, fmt.Errorf("%w: checksum mismatch", ErrInvalidAddress)
	}
	if tag := b[0] &^ tagTestOnly; tag != tagBounceable && tag != tagNonBounceable {
		return Address{}, fmt.Errorf("%w: unknown tag 0x%02x", ErrInvalidAddress, b[0])
	}
	var a Address
	a.Workchain = int32(int8(b[1])) //nolint:gosec // G115: the workchain byte is a signed int8 by definition
	copy(a.Hash[:], b[2:34])
	return a, nil
}

// Raw returns the canonical raw form "wc:hex" with lowercase hex. This is
// the form used for matching and storage.
func (a Address) Raw() string {
	return strconv.FormatInt(int64(a.Workchain), 10) + ":" + hex.EncodeToString(a.Hash[:])
}

// String implements fmt.Stringer (raw form).
func (a Address) String() string { return a.Raw() }

// Friendly returns the user-friendly base64url form. Wallets show
// non-bounceable ("UQ…") addresses for user wallets and bounceable ("EQ…")
// for contracts.
func (a Address) Friendly(bounceable, testnet bool) string {
	b := make([]byte, 36)
	b[0] = tagNonBounceable
	if bounceable {
		b[0] = tagBounceable
	}
	if testnet {
		b[0] |= tagTestOnly
	}
	b[1] = byte(int8(a.Workchain)) //nolint:gosec // G115: standard workchains are -1 and 0
	copy(b[2:34], a.Hash[:])
	binary.BigEndian.PutUint16(b[34:], crc16(b[:34]))
	return base64.URLEncoding.EncodeToString(b)
}

// NormalizeAddress parses s and returns its canonical raw form.
func NormalizeAddress(s string) (string, error) {
	a, err := ParseAddress(s)
	if err != nil {
		return "", err
	}
	return a.Raw(), nil
}

// FriendlyFromRaw converts a canonical raw address to the non-bounceable
// user-friendly form for display; invalid input is returned unchanged.
func FriendlyFromRaw(raw string) string {
	a, err := ParseAddress(raw)
	if err != nil {
		return raw
	}
	return a.Friendly(false, false)
}

// crc16 is CRC-16/XMODEM (poly 0x1021, init 0), as used by TON addresses.
func crc16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc ^= uint16(b) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
