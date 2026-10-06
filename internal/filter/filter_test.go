package filter

import (
	"math/big"
	"reflect"
	"testing"

	"github.com/co-codin/tron-usdt-listener/internal/model"
)

const (
	alice = "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"
	bob   = "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"
	carol = "TEHwL3F2kpkJExYAZYziHGLFR2CR8Bq3bo"
	dave  = "TGph8bRfN2rA1BtnX6haStVGFBmhvj9NpE"
)

func usdt(n int64) *big.Int { return new(big.Int).Mul(big.NewInt(n), big.NewInt(1_000_000)) }

func tr(from, to string, amount int64) model.Transfer {
	return model.Transfer{From: from, To: to, Amount: usdt(amount), Decimals: 6}
}

func TestFilterMatch(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		in      model.Transfer
		ok      bool
		reasons []string
	}{
		{"no rules matches everything", Config{}, tr(carol, dave, 1), true, []string{model.ReasonAll}},
		{"incoming to watched", Config{WatchAddresses: []string{bob}}, tr(alice, bob, 5), true, []string{model.ReasonIncoming}},
		{"outgoing from watched", Config{WatchAddresses: []string{alice}}, tr(alice, bob, 5), true, []string{model.ReasonOutgoing}},
		{"unrelated not matched", Config{WatchAddresses: []string{alice}}, tr(carol, dave, 5), false, nil},
		{"direction incoming ignores outgoing", Config{WatchAddresses: []string{alice}, Direction: Incoming}, tr(alice, bob, 5), false, nil},
		{"direction outgoing ignores incoming", Config{WatchAddresses: []string{bob}, Direction: Outgoing}, tr(alice, bob, 5), false, nil},
		{"self transfer both reasons", Config{WatchAddresses: []string{alice}}, tr(alice, alice, 5), true, []string{model.ReasonIncoming, model.ReasonOutgoing}},
		{"hex watch address", Config{WatchAddresses: []string{"417452f02038a6039b730c7ec929a3380ff1b4a6e7"}}, tr(alice, bob, 5), true, []string{model.ReasonIncoming}},
		{"large at threshold", Config{MinAmount: usdt(100_000)}, tr(carol, dave, 100_000), true, []string{model.ReasonLarge}},
		{"below threshold", Config{MinAmount: usdt(100_000)}, tr(carol, dave, 99_999), false, nil},
		{"any: watched small", Config{WatchAddresses: []string{alice}, MinAmount: usdt(1000)}, tr(alice, bob, 5), true, []string{model.ReasonOutgoing}},
		{"any: unwatched large", Config{WatchAddresses: []string{alice}, MinAmount: usdt(1000)}, tr(carol, dave, 5000), true, []string{model.ReasonLarge}},
		{"all: watched small rejected", Config{WatchAddresses: []string{alice}, MinAmount: usdt(1000), Mode: All}, tr(alice, bob, 5), false, nil},
		{"all: unwatched large rejected", Config{WatchAddresses: []string{alice}, MinAmount: usdt(1000), Mode: All}, tr(carol, dave, 5000), false, nil},
		{"all: watched large", Config{WatchAddresses: []string{alice}, MinAmount: usdt(1000), Mode: All}, tr(carol, alice, 5000), true, []string{model.ReasonIncoming, model.ReasonLarge}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := New(tt.cfg)
			if err != nil {
				t.Fatal(err)
			}
			reasons, ok := f.Match(tt.in)
			if ok != tt.ok || !reflect.DeepEqual(reasons, tt.reasons) {
				t.Fatalf("Match = %v %v, want %v %v", reasons, ok, tt.reasons, tt.ok)
			}
		})
	}
}

func TestFilterApply(t *testing.T) {
	f, err := New(Config{WatchAddresses: []string{bob}, MinAmount: usdt(1000)})
	if err != nil {
		t.Fatal(err)
	}
	out := f.Apply([]model.Transfer{tr(alice, bob, 1), tr(carol, dave, 1), tr(carol, dave, 2000)})
	if len(out) != 2 || !out[0].HasReason(model.ReasonIncoming) || !out[1].HasReason(model.ReasonLarge) {
		t.Fatalf("unexpected output %+v", out)
	}
}

func TestFilterValidation(t *testing.T) {
	for _, cfg := range []Config{
		{WatchAddresses: []string{"not-an-address"}},
		{Direction: "sideways"},
		{Mode: "some"},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("expected error for %+v", cfg)
		}
	}
}
