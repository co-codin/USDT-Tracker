package sink

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
)

func TestFormatTelegram(t *testing.T) {
	tr := sampleBatch().Transfers[0]
	tr.Amount = big.NewInt(1_250_000_500_000)
	msg := FormatTelegram(tr, "https://tronscan.org/#/transaction/")
	for _, want := range []string{
		"📥 Incoming USDT",
		"1,250,000.5 USDT",
		"<code>TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx</code>",
		"Block: 86883900 · 2026-10-06 22:07:06 UTC",
		`href="https://tronscan.org/#/transaction/3e5cb8d14a2063cdcd35510db2510c64da5df29b1293634ffe2895ce822c5d82"`,
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message missing %q:\n%s", want, msg)
		}
	}
}

func TestTelegramSendMinAmountAndCap(t *testing.T) {
	var msgs []string
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/botTOKEN/sendMessage" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["chat_id"] != "42" || body["parse_mode"] != "HTML" {
			t.Errorf("unexpected body %v", body)
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":0}}`))
			return
		}
		msgs = append(msgs, body["text"].(string))
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	tg, err := NewTelegram(TelegramConfig{
		BotToken: "TOKEN", ChatID: "42", APIBase: srv.URL, MaxPerBlock: 2,
		MinAmount: big.NewInt(1_000_000_000), // 1000 USDT
		Backoff:   retry.Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := sampleBatch().Transfers[0]
	base.Reasons = []string{model.ReasonLarge}
	mk := func(i int, amount int64) model.Transfer {
		t := base
		t.LogIndex = i
		t.Amount = big.NewInt(amount)
		return t
	}
	b := Batch{BlockNumber: 1, Transfers: []model.Transfer{
		mk(0, 5_000_000),     // below telegram min -> skipped
		mk(1, 2_000_000_000), // alert
		mk(2, 3_000_000_000), // alert
		mk(3, 4_000_000_000), // over cap -> summarised
	}}
	if err := tg.Send(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want 3 (2 alerts + summary): %q", len(msgs), msgs)
	}
	if !strings.Contains(msgs[2], "<b>1</b> more USDT transfers") || !strings.Contains(msgs[2], "4,000 USDT") {
		t.Fatalf("unexpected summary: %s", msgs[2])
	}
}

func TestTelegramRequiresCredentials(t *testing.T) {
	if _, err := NewTelegram(TelegramConfig{ChatID: "1"}); err == nil {
		t.Fatal("expected error without token")
	}
}
