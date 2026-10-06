// Package webhooksig signs and verifies webhook payloads sent by
// tron-usdt-listener. Consumers can import it to verify deliveries:
//
//	err := webhooksig.Verify(secret, r.Header.Get(webhooksig.HeaderTimestamp),
//		r.Header.Get(webhooksig.HeaderSignature), body, 5*time.Minute, time.Now())
//
// The signature is HMAC-SHA256 over "<timestamp>.<raw body>", hex encoded and
// prefixed with "sha256=". Including the timestamp prevents replay attacks.
package webhooksig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Header names used by the webhook sink.
const (
	HeaderSignature = "X-Signature-256"
	HeaderTimestamp = "X-Signature-Timestamp"
	prefix          = "sha256="
)

// Errors returned by Verify.
var (
	ErrMissing     = errors.New("webhooksig: missing signature or timestamp")
	ErrBadFormat   = errors.New("webhooksig: malformed signature or timestamp")
	ErrExpired     = errors.New("webhooksig: timestamp outside tolerance")
	ErrMismatch    = errors.New("webhooksig: signature mismatch")
	ErrEmptySecret = errors.New("webhooksig: empty secret")
)

// Sign returns the signature header value for body at timestamp ts.
func Sign(secret []byte, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return prefix + hex.EncodeToString(mac.Sum(nil))
}

// Verify checks a signature header in constant time. tolerance <= 0 disables
// the timestamp freshness check.
func Verify(secret []byte, tsHeader, sigHeader string, body []byte, tolerance time.Duration, now time.Time) error {
	if len(secret) == 0 {
		return ErrEmptySecret
	}
	if tsHeader == "" || sigHeader == "" {
		return ErrMissing
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(tsHeader), 10, 64)
	if err != nil {
		return ErrBadFormat
	}
	if !strings.HasPrefix(sigHeader, prefix) {
		return ErrBadFormat
	}
	got, err := hex.DecodeString(strings.TrimPrefix(sigHeader, prefix))
	if err != nil {
		return ErrBadFormat
	}
	if tolerance > 0 {
		d := now.Sub(time.Unix(ts, 0))
		if d < 0 {
			d = -d
		}
		if d > tolerance {
			return ErrExpired
		}
	}
	want, _ := hex.DecodeString(strings.TrimPrefix(Sign(secret, ts, body), prefix))
	if !hmac.Equal(got, want) {
		return ErrMismatch
	}
	return nil
}
