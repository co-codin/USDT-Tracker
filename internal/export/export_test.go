package export

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	ethA = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"
	ethB = "0xfb6916095ca1df60bb79ce92ce3ea74c37c5d359"
	tok  = "0123456789abcdef-token"
)

type fakeSrc struct {
	rows []Row
	got  Query
	err  error
}

func (f *fakeSrc) StreamTransfers(_ context.Context, q Query, fn func(Row) error) error {
	f.got = q
	for _, r := range f.rows {
		if err := fn(r); err != nil {
			return err
		}
	}
	return f.err
}

func row(from, to, memo string, raw int64) Row {
	return Row{Time: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), Chain: "eth", TxID: "0xabc", LogIndex: 3,
		From: from, To: to, AmountRaw: big.NewInt(raw), Decimals: 6, Symbol: "USDT", Memo: memo}
}

func TestParseQuery(t *testing.T) {
	q, err := ParseQuery("", "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed", "2026-10-01", "2026-10-31")
	if err != nil {
		t.Fatal(err)
	}
	if q.Chain != "eth" || q.Address != ethA || !q.From.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) ||
		!q.To.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("%+v", q)
	}
	q, err = ParseQuery("tron", "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv", "2026-10-01T10:00:00+02:00", "")
	if err != nil || q.From.Hour() != 8 || !q.To.IsZero() {
		t.Fatalf("%+v %v", q, err)
	}
	for _, bad := range [][4]string{
		{"", "", "", ""}, {"sol", ethA, "", ""}, {"ton", ethA, "", ""}, {"", ethA, "yesterday", ""},
		{"", ethA, "2026-10-05", "2026-10-01"},
	} {
		if _, err := ParseQuery(bad[0], bad[1], bad[2], bad[3]); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}

func TestWriteEscapingAndDirection(t *testing.T) {
	src := &fakeSrc{rows: []Row{
		row(ethB, ethA, `inv "42", paid`+"\nline2", 1_250_000),
		row(ethA, ethB, "=HYPERLINK(\"http://evil\")", 1),
		row(ethA, ethA, "", 100_000_000_000),
	}}
	var buf bytes.Buffer
	n, err := Write(context.Background(), &buf, src, Query{Chain: "eth", Address: ethA}, Options{})
	if err != nil || n != 3 {
		t.Fatal(n, err)
	}
	recs, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(recs[0], ",") != strings.Join(Header, ",") || len(recs) != 4 {
		t.Fatalf("%v", recs)
	}
	r := recs[1]
	if r[0] != "2026-10-01T12:00:00Z" || r[1] != "eth" || r[6] != "in" || r[7] != "1.25" || r[9] != "inv \"42\", paid\nline2" ||
		r[4] != "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359" || r[10] != "https://etherscan.io/tx/0xabc" {
		t.Fatalf("%q", r)
	}
	if recs[2][6] != "out" || recs[2][7] != "0.000001" || !strings.HasPrefix(recs[2][9], "'=") {
		t.Fatalf("%q", recs[2])
	}
	if recs[3][6] != "self" || recs[3][7] != "100000" {
		t.Fatalf("%q", recs[3])
	}
}

func TestHandler(t *testing.T) {
	src := &fakeSrc{rows: []Row{row(ethB, ethA, "", 5_000_000)}}
	h := NewHandler(tok, src, nil)
	do := func(auth, q string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/v1/export.csv?"+q, nil)
		if auth != "" {
			req.Header.Set("Authorization", "Bearer "+auth)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	if w := do("", "address="+ethA); w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code)
	}
	if w := do("wrong-token-xxxxxxxx", "address="+ethA); w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code)
	}
	if w := do(tok, "address=nope"); w.Code != http.StatusBadRequest {
		t.Fatal(w.Code)
	}
	w := do(tok, "address="+ethA+"&chain=eth&from=2026-10-01&to=2026-10-31")
	if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/csv") ||
		!strings.Contains(w.Header().Get("Content-Disposition"), "usdt-eth-0x5aAeb") || !strings.Contains(w.Body.String(), ",in,5,USDT,") {
		t.Fatalf("%d %v %s", w.Code, w.Header(), w.Body.String())
	}
	if src.got.Address != ethA || src.got.To.Day() != 1 {
		t.Fatalf("%+v", src.got)
	}
	if w := NewHandler("", src, nil); w.enabled {
		t.Fatal("empty token must disable")
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/v1/export.csv?address="+ethA, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	NewHandler(tok, nil, nil).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatal(rec.Code)
	}
	req = httptest.NewRequest("POST", "/v1/export.csv", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatal(rec.Code)
	}
}

func TestWriteSourceError(t *testing.T) {
	src := &fakeSrc{rows: []Row{row(ethB, ethA, "", 1)}, err: errors.New("boom")}
	if _, err := Write(context.Background(), &bytes.Buffer{}, src, Query{Address: ethA}, Options{}); err == nil {
		t.Fatal("error swallowed")
	}
}

func TestSafeFormulaInjection(t *testing.T) {
	for in, want := range map[string]string{
		"":          "",
		"order 42":  "order 42",
		"=1+1":      "'=1+1",
		" =1+1":     "' =1+1",
		"\n=1":      "'\n=1",
		"＝1":        "'＝1",
		"|cmd":      "'|cmd",
		"%0A":       "'%0A",
		"\t@x":      "'\t@x",
		"-5":        "'-5",
		"a\r\nb":    "a\r\nb",
		"Ref#CwZT6": "Ref#CwZT6",
	} {
		if got := safe(in); got != want {
			t.Errorf("safe(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHandlerJSONErrorsAndBOM(t *testing.T) {
	src := &fakeSrc{rows: []Row{row(ethB, ethA, "备注", 1)}}
	h := NewHandler(tok, src, nil)
	get := func(q string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/v1/export.csv?"+q, nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	w := get("address=nope")
	if w.Code != 400 || w.Header().Get("Content-Type") != "application/json" || !strings.Contains(w.Body.String(), `"code":"invalid_request"`) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if w := get("address=" + ethA + "&bom=x"); w.Code != 400 {
		t.Fatal(w.Code)
	}
	w = get("address=" + ethA + "&bom=1")
	if w.Code != 200 || !strings.HasPrefix(w.Body.String(), BOM+"time_utc,") {
		t.Fatalf("%q", w.Body.String()[:20])
	}
	if w := get("address=" + ethA); strings.HasPrefix(w.Body.String(), BOM) {
		t.Fatal("BOM without bom=1")
	}
}
