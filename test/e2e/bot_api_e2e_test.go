//go:build e2e

package e2e

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trontest"
	"github.com/co-codin/USDT-Tracker/internal/tgbot/tgtest"
)

// Admin API (auth, add/remove, pagination), CSV export over HTTP, the
// large-transfer alerts channel and the notify bot (/start /add /list
// /remove, i18n, free-plan limit) against mock Telegram servers, all on
// the real binary with real Postgres.
func TestAdminAPIExportAlertsNotifyBot(t *testing.T) {
	db := newPG(t)
	node := trontest.NewServer(106)
	defer node.Close()
	alerts := tgtest.New("ALERTTOKEN")
	defer alerts.Close()
	nb := tgtest.New("NOTIFYTOKEN")
	defer nb.Close()

	cfgPath := writeConfig(t, `
chains:
  tron: {rps: 1000, confirmations: 5, start_block: 100, poll_interval: 20ms}
sinks:
  stdout: {enabled: false}
  postgres: {enabled: true}
alerts:
  large_transfers: {enabled: true, bot_token: ALERTTOKEN, chat_id: "@whales", threshold: "0.000001", interval: 500ms, api_base: "`+alerts.URL+`"}
notify_bot: {enabled: true, bot_token: NOTIFYTOKEN, free_limit: 2, min_amount: "0", poll_timeout: 1s, api_base: "`+nb.URL+`"}
`)
	env := map[string]string{"TRON_API_URL": node.URL, "DATABASE_URL": db.DSN, "HTTP_ADDR": freeAddr(t), "API_TOKEN": apiToken}
	p := start(t, env, "-config", cfgPath)

	// --- admin API ---
	req, _ := http.NewRequest("GET", "http://"+p.addr+"/v1/addresses", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 401 {
		t.Fatalf("bad token: %v %v", resp, err)
	}
	for _, a := range []string{trontest.Alice, trontest.Bob, trontest.Carol} {
		if code, b := p.api("POST", "/v1/addresses", map[string]string{"chain": "tron", "address": a}); code/100 != 2 {
			t.Fatalf("add %s: %d %s", a, code, b)
		}
	}
	if code, _ := p.api("POST", "/v1/addresses", map[string]string{"chain": "ton", "address": "0:" + strings.Repeat("a", 64)}); code != 400 {
		t.Fatalf("disabled chain should be 400, got %d", code)
	}
	type page struct {
		Addresses []struct{ Address string } `json:"addresses"`
		Next      string                     `json:"next_cursor"`
	}
	var p1, p2 page
	_, b := p.api("GET", "/v1/addresses?limit=2", nil)
	_ = json.Unmarshal(b, &p1)
	if len(p1.Addresses) != 2 || p1.Next == "" {
		t.Fatalf("page 1: %s", b)
	}
	_, b = p.api("GET", "/v1/addresses?limit=2&cursor="+p1.Next, nil)
	_ = json.Unmarshal(b, &p2)
	if len(p2.Addresses) != 1 || p2.Next != "" {
		t.Fatalf("page 2: %s", b)
	}
	if code, b := p.api("DELETE", "/v1/addresses/"+trontest.Carol, nil); code/100 != 2 {
		t.Fatalf("delete: %d %s", code, b)
	}
	_, b = p.api("GET", "/v1/addresses", nil)
	if strings.Contains(string(b), trontest.Carol) {
		t.Fatalf("Carol still listed: %s", b)
	}

	// --- notify bot conversations ---
	const en, ru = 1001, 1002
	nb.UserSays(en, "/start")
	nb.UserSays(en, "/add "+trontest.Alice)
	nb.UserSays(en, "/add "+trontest.Dave)
	nb.UserSays(en, "/add "+trontest.Bob) // over free_limit 2
	nb.UserSays(en, "/remove "+trontest.Dave)
	nb.UserSays(en, "/help") // 6th command inside the burst of 5: rate-limited, no reply
	nb.UserSaysLang(ru, "/add "+trontest.Bob, "ru")
	eventually(t, 15*time.Second, "first replies", func() bool { return len(nb.SentTo("1001")) >= 5 && len(nb.SentTo("1002")) >= 1 })
	time.Sleep(3500 * time.Millisecond) // per-chat bucket refills at 20/min
	if n := len(nb.SentTo("1001")); n != 5 {
		t.Fatalf("rate limit: %d replies to 6 commands, want 5", n)
	}
	nb.UserSays(en, "/list")
	eventually(t, 15*time.Second, "bot replies", func() bool { return len(nb.SentTo("1001")) >= 6 && len(nb.SentTo("1002")) >= 1 })
	r := nb.SentTo("1001")
	checks := []string{"/add", "Watching TRON", "Watching TRON", "plan allows 2", "Stopped watching", "Your addresses (1/2"}
	for i, want := range checks {
		if !strings.Contains(r[i].Text, want) {
			t.Fatalf("reply %d = %q, want %q", i, r[i].Text, want)
		}
	}
	if !strings.Contains(nb.SentTo("1002")[0].Text, "Отслеживаю адрес TRON") {
		t.Fatalf("ru reply: %q", nb.SentTo("1002")[0].Text)
	}

	// --- new blocks → alerts + notifications ---
	before := len(nb.SentTo("1001"))
	node.SetHead(160)
	eventually(t, 30*time.Second, "cursor 155", func() bool { return db.cursor("default") >= 155 })
	eventually(t, 20*time.Second, "alerts + notifications", func() bool {
		return len(alerts.SentTo("@whales")) > 0 && len(nb.SentTo("1001")) > before && len(nb.SentTo("1002")) > 1
	})
	time.Sleep(500 * time.Millisecond)
	seen := map[string]bool{}
	for _, m := range nb.SentTo("1001")[before:] {
		if seen[m.Text] {
			t.Fatalf("duplicate notification: %q", m.Text)
		}
		seen[m.Text] = true
	}

	// --- CSV export over HTTP ---
	var aliceRows int
	for _, e := range trontest.ExpectedTransfers(102, 155) { // 100–101 may pass before the API adds land
		if e.From == trontest.Alice || e.To == trontest.Alice {
			aliceRows++
		}
	}
	creq, _ := http.NewRequest("GET", "http://"+p.addr+"/v1/export.csv?chain=tron&address="+trontest.Alice, nil)
	creq.Header.Set("Authorization", "Bearer "+apiToken)
	resp, err := http.DefaultClient.Do(creq)
	if err != nil || resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/csv") {
		t.Fatalf("export: %v %v", resp, err)
	}
	rows, err := csv.NewReader(resp.Body).ReadAll()
	resp.Body.Close()
	if err != nil || len(rows) == 0 || rows[0][0] != "time_utc" {
		t.Fatalf("csv: %v %v", rows, err)
	}
	if aliceRows == 0 || len(rows)-1 < aliceRows {
		t.Fatalf("csv rows %d, want ≥ %d (Alice transfers in stored range)", len(rows)-1, aliceRows)
	}
	if code := p.stop(syscall.SIGTERM); code != 0 {
		t.Fatalf("exit %d", code)
	}
	t.Logf("alerts %d, notifications en=%d ru=%d, csv rows %d", len(alerts.SentTo("@whales")), len(nb.SentTo("1001"))-before, len(nb.SentTo("1002"))-1, len(rows)-1)
}
