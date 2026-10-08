package trc20

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
)

func usdtDecoder(t *testing.T) *Decoder {
	t.Helper()
	c, err := tron.ParseAddress("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
	if err != nil {
		t.Fatal(err)
	}
	return New(Token{Contract: c, Symbol: "USDT", Decimals: 6})
}

func loadFixture(t *testing.T) []tron.TransactionInfo {
	t.Helper()
	b, err := os.ReadFile("testdata/block_86883900_sample.json")
	if err != nil {
		t.Fatal(err)
	}
	var infos []tron.TransactionInfo
	if err := json.Unmarshal(b, &infos); err != nil {
		t.Fatal(err)
	}
	return infos
}

// Expected values were cross-checked against tronscan's trc20TransferInfo.
func TestDecodeRealMainnetBlock(t *testing.T) {
	transfers, errs := usdtDecoder(t).DecodeBlock(loadFixture(t))
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	type exp struct {
		tx       string
		logIndex int
		from, to string
		raw, amt string
	}
	want := []exp{
		{"3e5cb8d14a2063cdcd35510db2510c64da5df29b1293634ffe2895ce822c5d82", 0, "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx", "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv", "400000000", "400"},
		{"932db1149f2ef2fa3c252b4397b729601f478ff5434b634f4c3d08f94247590a", 0, "TXtEs6t2oUWQsNos7m68gbHdE9Q5n6x2oN", "TYVWGh8XkmU49Hi9PkGAZXiiJPB3J5zJZy", "25000000000", "25000"},
		{"590f6e3d62ca9d298a42edeaee14ee961e496821802e5d5ce647941e28598993", 0, "TEHwL3F2kpkJExYAZYziHGLFR2CR8Bq3bo", "TLntW9Z59LYY5KEi9cmwk3PKjQga828ird", "1500000", "1.5"},
		{"590f6e3d62ca9d298a42edeaee14ee961e496821802e5d5ce647941e28598993", 1, "TEHwL3F2kpkJExYAZYziHGLFR2CR8Bq3bo", "TGph8bRfN2rA1BtnX6haStVGFBmhvj9NpE", "21000000000", "21000"},
	}
	got := map[string]exp{}
	for _, tr := range transfers {
		got[tr.Key()] = exp{tr.TxID, tr.LogIndex, tr.From, tr.To, tr.Amount.String(), tr.AmountDecimal()}
		if tr.BlockNumber != 86883900 {
			t.Errorf("%s: block = %d", tr.Key(), tr.BlockNumber)
		}
		if !tr.BlockTime.Equal(time.UnixMilli(1791324426000)) {
			t.Errorf("%s: block time = %s", tr.Key(), tr.BlockTime)
		}
		if tr.Contract != "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t" || tr.Symbol != "USDT" || tr.Decimals != 6 {
			t.Errorf("%s: token fields wrong: %+v", tr.Key(), tr)
		}
	}
	if len(transfers) != len(want) {
		t.Fatalf("got %d transfers, want %d", len(transfers), len(want))
	}
	for _, w := range want {
		g, ok := got[w.tx+":"+strconv.Itoa(w.logIndex)]
		if !ok {
			t.Fatalf("missing transfer %s:%d", w.tx, w.logIndex)
		}
		if g != w {
			t.Errorf("transfer %s:%d\n got %+v\nwant %+v", w.tx, w.logIndex, g, w)
		}
	}
}

func TestDecodeSkipsIrrelevantAndFailed(t *testing.T) {
	d := usdtDecoder(t)
	transferLog := tron.Log{
		Address: "a614f803b6fd780986a42c78ec9c7f77e6ded13c",
		Topics: []string{TransferTopic,
			"000000000000000000000000f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07",
			"0000000000000000000000007452f02038a6039b730c7ec929a3380ff1b4a6e7"},
		Data: "0000000000000000000000000000000000000000000000000000000017d78400",
	}
	otherToken := transferLog
	otherToken.Address = "3487b63d30b5b2c87fb7ffa8bcfade38eaac1abe" // USDC
	approval := transferLog
	approval.Topics = append([]string{"8c5be1e5ebec7d5bd14f71427e1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"}, transferLog.Topics[1:]...)

	reverted := tron.TransactionInfo{ID: "aa", BlockNumber: 1, Receipt: tron.Receipt{Result: "REVERT"}, Log: []tron.Log{transferLog}}
	if ts, _ := d.DecodeTx(reverted); len(ts) != 0 {
		t.Fatal("reverted tx must be skipped")
	}
	failed := tron.TransactionInfo{ID: "ab", BlockNumber: 1, Result: "FAILED", Log: []tron.Log{transferLog}}
	if ts, _ := d.DecodeTx(failed); len(ts) != 0 {
		t.Fatal("failed tx must be skipped")
	}
	mixed := tron.TransactionInfo{ID: "AC", BlockNumber: 1, Receipt: tron.Receipt{Result: "SUCCESS"},
		Log: []tron.Log{otherToken, approval, transferLog}}
	ts, errs := d.DecodeTx(mixed)
	if len(errs) != 0 || len(ts) != 1 {
		t.Fatalf("got %d transfers, errs %v", len(ts), errs)
	}
	if ts[0].LogIndex != 2 || ts[0].TxID != "ac" || ts[0].AmountDecimal() != "400" {
		t.Fatalf("unexpected transfer %+v", ts[0])
	}

	// The log address may also appear 41-prefixed.
	prefixed := transferLog
	prefixed.Address = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	if _, ok, err := d.DecodeLog(prefixed); !ok || err != nil {
		t.Fatalf("41-prefixed address not accepted: ok=%v err=%v", ok, err)
	}
}

func TestDecodeMalformed(t *testing.T) {
	d := usdtDecoder(t)
	base := tron.Log{Address: "a614f803b6fd780986a42c78ec9c7f77e6ded13c", Topics: []string{TransferTopic,
		"000000000000000000000000f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07",
		"0000000000000000000000007452f02038a6039b730c7ec929a3380ff1b4a6e7"}, Data: "00"}
	_, ok, err := d.DecodeLog(base)
	if ok || !errors.Is(err, ErrMalformedLog) {
		t.Fatalf("short data: ok=%v err=%v", ok, err)
	}
	twoTopics := base
	twoTopics.Topics = base.Topics[:2]
	if _, _, err := d.DecodeLog(twoTopics); !errors.Is(err, ErrMalformedLog) {
		t.Fatalf("missing topic: %v", err)
	}
	info := tron.TransactionInfo{ID: "ff", Log: []tron.Log{base}}
	ts, errs := d.DecodeTx(info)
	if len(ts) != 0 || len(errs) != 1 {
		t.Fatalf("want 1 error and no transfers, got %d/%d", len(ts), len(errs))
	}
}

func TestDecodeUint256(t *testing.T) {
	v, err := DecodeUint256("0x00000000000000000000000000000000000000000000000000000004e3b29200")
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "21000000000" {
		t.Fatalf("got %s", v)
	}
	maxWord := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	v, _ = DecodeUint256(maxWord)
	if v.BitLen() != 256 {
		t.Fatal("uint256 max not decoded")
	}
}
