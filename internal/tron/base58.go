package tron

import (
	"errors"
	"strings"
)

const b58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

var b58Index = func() [256]int16 {
	var idx [256]int16
	for i := range idx {
		idx[i] = -1
	}
	for i := 0; i < len(b58Alphabet); i++ {
		idx[b58Alphabet[i]] = int16(i)
	}
	return idx
}()

// base58Encode encodes bytes using the Bitcoin base58 alphabet.
func base58Encode(in []byte) string {
	zeros := 0
	for zeros < len(in) && in[zeros] == 0 {
		zeros++
	}
	// log(256)/log(58) ≈ 1.37
	out := make([]byte, (len(in)-zeros)*138/100+1)
	high := len(out) - 1
	for _, b := range in[zeros:] {
		carry := int(b)
		j := len(out) - 1
		for ; j > high || carry != 0; j-- {
			carry += 256 * int(out[j])
			out[j] = byte(carry % 58) //nolint:gosec // G115: value is < 58
			carry /= 58
		}
		high = j
	}
	i := 0
	for i < len(out) && out[i] == 0 {
		i++
	}
	var sb strings.Builder
	sb.Grow(zeros + len(out) - i)
	for k := 0; k < zeros; k++ {
		sb.WriteByte(b58Alphabet[0])
	}
	for ; i < len(out); i++ {
		sb.WriteByte(b58Alphabet[out[i]])
	}
	return sb.String()
}

// base58Decode decodes a Bitcoin-alphabet base58 string.
func base58Decode(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.New("empty base58 string")
	}
	zeros := 0
	for zeros < len(s) && s[zeros] == b58Alphabet[0] {
		zeros++
	}
	out := make([]byte, (len(s)-zeros)*733/1000+1) // log(58)/log(256) ≈ 0.733
	high := len(out) - 1
	for i := zeros; i < len(s); i++ {
		v := b58Index[s[i]]
		if v < 0 {
			return nil, errors.New("invalid base58 character")
		}
		carry := int(v)
		j := len(out) - 1
		for ; j > high || carry != 0; j-- {
			carry += 58 * int(out[j])
			out[j] = byte(carry % 256) //nolint:gosec // G115: value is < 256
			carry /= 256
		}
		high = j
	}
	i := 0
	for i < len(out) && out[i] == 0 {
		i++
	}
	res := make([]byte, zeros+len(out)-i)
	copy(res[zeros:], out[i:])
	return res, nil
}
