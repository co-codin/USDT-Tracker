package tron

import (
	"bytes"
	"errors"
	"testing"
)

func TestAddressVectors(t *testing.T) {
	tests := []struct {
		name, hex, b58 string
	}{
		{"usdt contract", "41a614f803b6fd780986a42c78ec9c7f77e6ded13c", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"},
		{"zero address", "410000000000000000000000000000000000000000", "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"},
		// Verified against tronscan for tx 3e5cb8d1… (block 86883900).
		{"real sender", "41f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07", "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"},
		{"real recipient", "417452f02038a6039b730c7ec929a3380ff1b4a6e7", "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := AddressFromHex(tt.hex)
			if err != nil {
				t.Fatal(err)
			}
			if got := a.String(); got != tt.b58 {
				t.Fatalf("String() = %s, want %s", got, tt.b58)
			}
			b, err := AddressFromBase58(tt.b58)
			if err != nil {
				t.Fatal(err)
			}
			if b != a {
				t.Fatalf("base58 round trip mismatch: %s vs %s", b.Hex(), a.Hex())
			}
			if b.Hex() != tt.hex {
				t.Fatalf("Hex() = %s, want %s", b.Hex(), tt.hex)
			}
		})
	}
}

func TestAddressFromTopic(t *testing.T) {
	a, err := AddressFromTopic("000000000000000000000000f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07")
	if err != nil {
		t.Fatal(err)
	}
	if a.String() != "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx" {
		t.Fatalf("got %s", a)
	}
	if _, err := AddressFromTopic("ff0000000000000000000000f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07"); err == nil {
		t.Fatal("expected error for non-zero padding")
	}
	if _, err := AddressFromTopic("f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07"); err == nil {
		t.Fatal("expected error for short topic")
	}
}

func TestParseAddressForms(t *testing.T) {
	want := "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
	for _, in := range []string{
		want,
		"41a614f803b6fd780986a42c78ec9c7f77e6ded13c",
		"0x41a614f803b6fd780986a42c78ec9c7f77e6ded13c",
		"a614f803b6fd780986a42c78ec9c7f77e6ded13c",
		"  " + want + " ",
	} {
		a, err := ParseAddress(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if a.String() != want {
			t.Fatalf("%q: got %s", in, a)
		}
	}
}

func TestAddressErrors(t *testing.T) {
	// Last character changed -> checksum mismatch.
	if _, err := AddressFromBase58("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6u"); !errors.Is(err, ErrChecksum) {
		t.Fatalf("want ErrChecksum, got %v", err)
	}
	for _, bad := range []string{"", "T0OIl", "42a614f803b6fd780986a42c78ec9c7f77e6ded13c", "zz", "41a614"} {
		if _, err := ParseAddress(bad); err == nil {
			t.Fatalf("%q: expected error", bad)
		}
	}
}

func TestBase58RoundTrip(t *testing.T) {
	for _, in := range [][]byte{{0}, {0, 0, 1}, {1, 2, 3, 4, 5}, bytes.Repeat([]byte{0xff}, 25), {}} {
		enc := base58Encode(in)
		if len(in) == 0 {
			if enc != "" {
				t.Fatalf("empty input encoded to %q", enc)
			}
			continue
		}
		dec, err := base58Decode(enc)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(dec, in) {
			t.Fatalf("round trip %x -> %s -> %x", in, enc, dec)
		}
	}
	if base58Encode([]byte("hello world")) != "StV1DL6CwTryKyV" {
		t.Fatal("unexpected base58 for 'hello world'")
	}
}
