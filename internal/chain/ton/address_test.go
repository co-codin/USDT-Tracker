package ton

import (
	"errors"
	"testing"
)

// Pairs taken from a real toncenter v3 address_book response.
var addrPairs = []struct {
	raw, friendly string
	bounceable    bool
}{
	{"0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE", "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs", true},
	{"0:9DB2CAE853CF2DB870EA06D1E8286F202CD9AD689DFC74B98195520D06495859", "UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg", false},
	{"0:3D473A725599D277EE2D4F28093320027F120D704DD541B8DEC2986FAECD7F17", "EQA9RzpyVZnSd-4tTygJMyACfxINcE3VQbjewphvrs1_F12c", true},
	{"0:6FC4B8AE3A3C8BBF09A0EE6DD07BE32AABC0B661D017A82F5D28675C9BD7AB81", "UQBvxLiuOjyLvwmg7m3Qe-Mqq8C2YdAXqC9dKGdcm9ergbiQ", false},
}

func TestParseAddressRoundTrip(t *testing.T) {
	for _, p := range addrPairs {
		a, err := ParseAddress(p.raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := a.Friendly(p.bounceable, false); got != p.friendly {
			t.Errorf("Friendly(%s) = %s, want %s", p.raw, got, p.friendly)
		}
		b, err := ParseAddress(p.friendly)
		if err != nil {
			t.Fatalf("parse %s: %v", p.friendly, err)
		}
		if b != a {
			t.Errorf("friendly and raw parse differently for %s", p.friendly)
		}
		if b.Raw() != toLower(p.raw) {
			t.Errorf("Raw() = %s", b.Raw())
		}
		// the other flavour (bounceable vs not) and the std-base64 spelling decode to the same address
		c, err := ParseAddress(a.Friendly(!p.bounceable, false))
		if err != nil || c != a {
			t.Errorf("other flavour: %v", err)
		}
	}
	std := "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id/sDs"
	if a, err := ParseAddress(std); err != nil || a.Raw() != toLower(addrPairs[0].raw) {
		t.Errorf("std base64: %v", err)
	}
	if n, _ := NormalizeAddress(USDTMaster); n != "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe" {
		t.Errorf("USDTMaster raw = %s", n)
	}
	if FriendlyFromRaw(toLower(addrPairs[1].raw)) != addrPairs[1].friendly {
		t.Error("FriendlyFromRaw")
	}
	if FriendlyFromRaw("garbage") != "garbage" {
		t.Error("FriendlyFromRaw garbage")
	}
}

func TestParseAddressTestnetAndMasterchain(t *testing.T) {
	a, _ := ParseAddress("-1:" + "3333333333333333333333333333333333333333333333333333333333333333")
	f := a.Friendly(true, true)
	b, err := ParseAddress(f)
	if err != nil || b != a || b.Workchain != -1 {
		t.Fatalf("testnet masterchain round trip: %v %v", b, err)
	}
}

func TestParseAddressInvalid(t *testing.T) {
	bad := []string{
		"", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "0:abc", "x:" + "00", "0:zz" + "00",
		"EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDt", // checksum
		"EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sD!",
		"AACxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs",
	}
	for _, s := range bad {
		if _, err := ParseAddress(s); !errors.Is(err, ErrInvalidAddress) {
			t.Errorf("ParseAddress(%q) err = %v", s, err)
		}
	}
}

func TestShard(t *testing.T) {
	var lo, hi [32]byte
	hi[0] = 0xF0
	full, _ := ParseShard("8000000000000000")
	left, _ := ParseShard("4000000000000000")
	right, _ := ParseShard("c000000000000000")
	if !ShardContains(full, lo) || !ShardContains(full, hi) {
		t.Error("full shard must contain everything")
	}
	if !ShardContains(left, lo) || ShardContains(left, hi) || ShardContains(right, lo) || !ShardContains(right, hi) {
		t.Error("split shard prefix match")
	}
	neg, err := ParseShard("-9223372036854775808")
	if err != nil || neg != full {
		t.Errorf("signed shard: %x %v", neg, err)
	}
	for _, s := range []string{"", "0", "xyz", "-abc"} {
		if _, err := ParseShard(s); err == nil {
			t.Errorf("ParseShard(%q) should fail", s)
		}
	}
}

func TestHashHex(t *testing.T) {
	h, err := HashHex("VAo5uCmcxdAjnu+ivkJoRFomK4l6bNlw8fsqxOcK7xk=")
	if err != nil || len(h) != 64 {
		t.Fatal(h, err)
	}
	if h2, _ := HashHex(h); h2 != h {
		t.Error("hex passthrough")
	}
	if h3, _ := HashHex("VAo5uCmcxdAjnu-ivkJoRFomK4l6bNlw8fsqxOcK7xk="); h3 != h {
		t.Error("url base64")
	}
	if _, err := HashHex("short"); err == nil {
		t.Error("want error")
	}
}

func toLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'F' {
			b[i] = c + 32
		}
	}
	return string(b)
}
