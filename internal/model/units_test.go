package model

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"
)

func TestFormatUnits(t *testing.T) {
	tests := []struct {
		raw  string
		dec  int
		want string
	}{
		{"0", 6, "0"},
		{"1", 6, "0.000001"},
		{"1500000", 6, "1.5"},
		{"400000000", 6, "400"},
		{"123456789", 6, "123.456789"},
		{"1000000000000000", 6, "1000000000"},
		{"-2500000", 6, "-2.5"},
		{"42", 0, "42"},
	}
	for _, tt := range tests {
		v, _ := new(big.Int).SetString(tt.raw, 10)
		if got := FormatUnits(v, tt.dec); got != tt.want {
			t.Errorf("FormatUnits(%s, %d) = %s, want %s", tt.raw, tt.dec, got, tt.want)
		}
	}
	if FormatUnits(nil, 6) != "0" {
		t.Error("nil should format as 0")
	}
}

func TestParseUnits(t *testing.T) {
	ok := map[string]string{
		"": "0", "0": "0", "1": "1000000", "0.5": "500000", ".25": "250000",
		"100000": "100000000000", "1_000_000": "1000000000000", "1,250.75": "1250750000", "0.000001": "1",
	}
	for in, want := range ok {
		v, err := ParseUnits(in, 6)
		if err != nil {
			t.Fatalf("ParseUnits(%q): %v", in, err)
		}
		if v.String() != want {
			t.Errorf("ParseUnits(%q) = %s, want %s", in, v, want)
		}
	}
	for _, bad := range []string{"abc", "1.0000001", "-5", "1.2.3", "1e6"} {
		if _, err := ParseUnits(bad, 6); err == nil {
			t.Errorf("ParseUnits(%q): expected error", bad)
		}
	}
}

func TestHumanizeUnits(t *testing.T) {
	v, _ := new(big.Int).SetString("1250000505000", 10)
	if got := HumanizeUnits(v, 6, 2); got != "1,250,000.5" {
		t.Fatalf("got %s", got)
	}
	if got := HumanizeUnits(big.NewInt(999000000), 6, 2); got != "999" {
		t.Fatalf("got %s", got)
	}
}

func TestTransferJSONRoundTrip(t *testing.T) {
	in := Transfer{
		TxID: "abc", LogIndex: 2, BlockNumber: 10, BlockTime: time.Unix(1700000000, 0).UTC(),
		Contract: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", Symbol: "USDT", Decimals: 6,
		From: "TA", To: "TB", Amount: big.NewInt(1500000), Reasons: []string{ReasonLarge},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m["id"] != "abc:2" || m["amount"] != "1.5" || m["amount_raw"] != "1500000" {
		t.Fatalf("unexpected json %s", b)
	}
	var out Transfer
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Key() != in.Key() || out.Amount.Cmp(in.Amount) != 0 || !out.HasReason(ReasonLarge) {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}
