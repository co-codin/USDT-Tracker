package model

import (
	"fmt"
	"math/big"
	"strings"
)

// FormatUnits converts a raw integer amount into a decimal string with the
// given number of decimals, trimming trailing zeros: 1500000 (6) -> "1.5".
func FormatUnits(v *big.Int, decimals int) string {
	if v == nil {
		return "0"
	}
	neg := v.Sign() < 0
	s := new(big.Int).Abs(v).String()
	if decimals > 0 {
		if len(s) <= decimals {
			s = strings.Repeat("0", decimals-len(s)+1) + s
		}
		intPart, frac := s[:len(s)-decimals], strings.TrimRight(s[len(s)-decimals:], "0")
		s = intPart
		if frac != "" {
			s += "." + frac
		}
	}
	if neg {
		s = "-" + s
	}
	return s
}

// ParseUnits parses a non-negative decimal string ("1000", "0.5", "1_000.25")
// into raw base units. Underscores and commas are ignored as digit separators.
func ParseUnits(s string, decimals int) (*big.Int, error) {
	clean := strings.NewReplacer("_", "", ",", "", " ", "").Replace(strings.TrimSpace(s))
	if clean == "" {
		return new(big.Int), nil
	}
	intPart, frac, hasDot := strings.Cut(clean, ".")
	if hasDot && strings.Contains(frac, ".") {
		return nil, fmt.Errorf("invalid amount %q", s)
	}
	if len(frac) > decimals {
		return nil, fmt.Errorf("amount %q has more than %d decimals", s, decimals)
	}
	if intPart == "" {
		intPart = "0"
	}
	digits := intPart + frac + strings.Repeat("0", decimals-len(frac))
	for _, r := range digits {
		if r < '0' || r > '9' {
			return nil, fmt.Errorf("invalid amount %q", s)
		}
	}
	v, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount %q", s)
	}
	return v, nil
}

// HumanizeUnits formats an amount with thousands separators and at most
// maxFrac fractional digits: 1250000500000 (6) -> "1,250,000.5".
func HumanizeUnits(v *big.Int, decimals, maxFrac int) string {
	s := FormatUnits(v, decimals)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	intPart, frac, _ := strings.Cut(s, ".")
	if len(frac) > maxFrac {
		frac = strings.TrimRight(frac[:maxFrac], "0")
	}
	var b strings.Builder
	for i, r := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	out := b.String()
	if frac != "" {
		out += "." + frac
	}
	if neg {
		out = "-" + out
	}
	return out
}
