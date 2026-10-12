// Package chainaddr validates and canonicalises addresses for every
// supported chain, so the filter, config and admin API agree on one form:
// base58check ("T…") on TRON, raw lowercase "wc:hex" on TON and lowercase
// "0x…" hex on Ethereum.
package chainaddr

import (
	"fmt"
	"strings"

	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// Chains lists the supported chain names.
var Chains = []string{model.ChainTron, model.ChainTON, model.ChainETH}

// Valid reports whether c is a supported chain name.
func Valid(c string) bool {
	return c == model.ChainTron || c == model.ChainTON || c == model.ChainETH
}

// Normalize validates s for chain c (empty = tron) and returns its canonical form.
// TRON accepts base58 or hex; TON accepts raw or user-friendly forms;
// Ethereum accepts lowercase/uppercase hex or a valid EIP-55 checksum.
func Normalize(c, s string) (string, error) {
	s = strings.TrimSpace(s)
	switch c {
	case "", model.ChainTron:
		a, err := tron.ParseAddress(s)
		if err != nil {
			return "", err
		}
		return a.String(), nil
	case model.ChainTON:
		return ton.NormalizeAddress(s)
	case model.ChainETH:
		return eth.NormalizeAddress(s)
	default:
		return "", fmt.Errorf("unknown chain %q (want tron|ton|eth)", c)
	}
}

// Detect guesses the chain of an address: TRON base58 starts with "T" and is
// 34 characters long; "0x" + 40 hex is Ethereum; everything that parses as
// a TON address is TON.
func Detect(s string) (string, error) {
	s = strings.TrimSpace(s)
	if len(s) == 42 && (strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X")) {
		if _, err := eth.NormalizeAddress(s); err != nil {
			return "", err
		}
		return model.ChainETH, nil
	}
	if strings.HasPrefix(s, "T") && len(s) == 34 {
		return model.ChainTron, nil
	}
	if _, err := ton.ParseAddress(s); err == nil {
		return model.ChainTON, nil
	}
	if _, err := tron.ParseAddress(s); err == nil {
		return model.ChainTron, nil
	}
	return "", fmt.Errorf("unrecognised address %q: want a TRON base58 address (T…) a TON address (EQ…/UQ… or 0:<hex>) or an Ethereum address (0x…)", s)
}
