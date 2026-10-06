package webhooksig

import (
	"errors"
	"testing"
	"time"
)

func TestSignKnownVector(t *testing.T) {
	// printf '1700000000.{"a":1}' | openssl dgst -sha256 -hmac secret
	got := Sign([]byte("secret"), 1700000000, []byte(`{"a":1}`))
	want := "sha256=49f24e537407743fa4a0242bb63b94b9a47ee99cbbe071ccd8a22550ae411686"
	if got != want {
		t.Fatalf("Sign = %s, want %s", got, want)
	}
}

func TestVerify(t *testing.T) {
	secret := []byte("s3cr3t")
	body := []byte(`{"event":"trc20.transfers"}`)
	now := time.Unix(1_800_000_000, 0)
	ts := now.Unix()
	sig := Sign(secret, ts, body)
	tsStr := "1800000000"

	if err := Verify(secret, tsStr, sig, body, 5*time.Minute, now); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	cases := []struct {
		name   string
		secret []byte
		ts     string
		sig    string
		body   []byte
		now    time.Time
		want   error
	}{
		{"tampered body", secret, tsStr, sig, []byte(`{"event":"x"}`), now, ErrMismatch},
		{"wrong secret", []byte("other"), tsStr, sig, body, now, ErrMismatch},
		{"timestamp changed", secret, "1800000001", sig, body, now, ErrMismatch},
		{"expired", secret, tsStr, sig, body, now.Add(10 * time.Minute), ErrExpired},
		{"missing", secret, "", sig, body, now, ErrMissing},
		{"no prefix", secret, tsStr, sig[7:], body, now, ErrBadFormat},
		{"bad hex", secret, tsStr, "sha256=zz", body, now, ErrBadFormat},
		{"empty secret", nil, tsStr, sig, body, now, ErrEmptySecret},
	}
	for _, c := range cases {
		if err := Verify(c.secret, c.ts, c.sig, c.body, 5*time.Minute, c.now); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}
