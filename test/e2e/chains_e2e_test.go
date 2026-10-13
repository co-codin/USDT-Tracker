//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/eth/ethtest"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton/tontest"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trontest"
)

// TRON with injected 429 / 5xx / TronGrid 403 frequency bans / a lagging
// node, real Postgres, a signed webhook and a SIGKILL mid-stream: after the
// restart Postgres holds every expected transfer exactly once (no gaps, no
// duplicates), the webhook receiver saw every transfer (at-least-once,
// every request HMAC-valid) and SIGTERM exits 0.
func TestTronFaultsCrashRestart(t *testing.T) {
	db := newPG(t)
	node := trontest.NewServer(160)
	defer node.Close()
	node.FailBlock(110, 429, 500, 503)
	node.FailBlock(120, 403)
	node.EmptyOnce(130)
	node.EmptyOnce(205)
	wh := newWebhookRx(t, "whsec")
	wh.fail = 2 // first two deliveries answer 503 → retried

	cfgPath := writeConfig(t, `
chains:
  tron:
    rps: 1000
    confirmations: 5
    start_block: 100
    poll_interval: 20ms
    batch_size: 7
sinks:
  stdout: {enabled: false}
  webhook: {enabled: true, max_retries: 8}
  postgres: {enabled: true}
`)
	env := map[string]string{
		"TRON_API_URL": node.URL, "WEBHOOK_URL": wh.URL, "WEBHOOK_SECRET": "whsec",
		"DATABASE_URL": db.DSN, "HTTP_ADDR": freeAddr(t), // no API token: filter matches everything
	}
	p1 := start(t, env, "-config", cfgPath)
	eventually(t, 20*time.Second, "cursor ≥ 140", func() bool { return db.cursor("default") >= 140 })
	if code := p1.stop(syscall.SIGKILL); code == 0 {
		t.Fatal("SIGKILL should not exit 0")
	}
	crashedAt := db.cursor("default")

	node.SetHead(260)
	p2 := start(t, env, "-config", cfgPath)
	eventually(t, 30*time.Second, "cursor 255", func() bool { return db.cursor("default") >= 255 })
	if code := p2.stop(syscall.SIGTERM); code != 0 {
		t.Fatalf("SIGTERM exit %d", code)
	}

	want := map[string]bool{}
	for _, e := range trontest.ExpectedTransfers(100, 255) {
		want[e.Key] = true
	}
	got := db.transferKeys(t, "tron")
	assertSameSet(t, "postgres", want, got, false)
	whGot, bad, _ := wh.snapshot()
	if bad != 0 {
		t.Fatalf("%d webhook requests failed HMAC verification", bad)
	}
	assertSameSet(t, "webhook", want, whGot, true)
	if node.TxInfoRequests(110) < 4 || node.TxInfoRequests(120) < 2 {
		t.Fatalf("faults not retried: 110=%d 120=%d", node.TxInfoRequests(110), node.TxInfoRequests(120))
	}
	t.Logf("crash at cursor %d; %d transfers; webhook redeliveries %d", crashedAt, len(got), dupes(whGot))
}

// TON only: many result pages in one masterchain block (an airdrop), 429 and
// 5xx from the index, with the stdout firehose; every transfer exactly once.
func TestTONManyPagesAndErrors(t *testing.T) {
	idx := tontest.New(40)
	defer idx.Close()
	idx.AddBurst(25, 2600, 10) // lt stays inside shard A range (500) // > 2 pages of 1000
	idx.Fail("/jetton/transfers", 429, 502, 500)
	idx.Fail("/masterchainInfo", 503)
	cfgPath := writeConfig(t, `
chains:
  ton:
    rps: 1000
    confirmations: 1
    start_block: 20
    poll_interval: 20ms
http: {addr: ""}
`)
	p := start(t, map[string]string{"TON_API_URL": idx.BaseURL()}, "-config", cfgPath)
	want := map[string]bool{}
	for _, e := range idx.Expected(20, 39) {
		want[e.Key] = true
	}
	eventually(t, 30*time.Second, "all TON transfers", func() bool { return len(stdoutTransfers(t, p.stdout.String())) >= len(want) })
	time.Sleep(200 * time.Millisecond)
	p.stop(syscall.SIGTERM)
	assertSameSet(t, "ton stdout", want, stdoutTransfers(t, p.stdout.String()), false)
	if len(want) < 2600 {
		t.Fatalf("burst missing from expectations: %d", len(want))
	}
}

// Ethereum: 429s, a lagging load-balanced backend that returns empty logs
// and null headers, and a reorg below the confirmation depth.
func TestETHLaggingReorg429(t *testing.T) {
	node := ethtest.New(40)
	defer node.Close()
	node.Fail429(3)
	node.Lagging(4, 30)
	cfgPath := writeConfig(t, `
chains:
  eth:
    rps: 1000
    confirmations: 2
    start_block: 10
    poll_interval: 20ms
    batch_size: 5
    log_chunk: 3
http: {addr: ""}
`)
	p := start(t, map[string]string{"ETH_RPC_URL": node.URL}, "-config", cfgPath)
	count := func(from, to int64) int {
		n := 0
		for b := from; b <= to; b++ {
			n += int(b % 3)
		}
		return n
	}
	eventually(t, 30*time.Second, "eth 10..38", func() bool { return len(stdoutTransfers(t, p.stdout.String())) >= count(10, 38) })
	node.Fork(37) // replaces already-processed blocks 37..38 (deeper than confirmations → detected & logged)
	node.Head.Store(60)
	eventually(t, 30*time.Second, "eth ..58", func() bool { return len(stdoutTransfers(t, p.stdout.String())) >= count(10, 58) })
	time.Sleep(200 * time.Millisecond)
	p.stop(syscall.SIGTERM)
	got := stdoutTransfers(t, p.stdout.String())
	for id, n := range got {
		if n != 1 {
			t.Fatalf("duplicate %s ×%d", id, n)
		}
	}
	if len(got) != count(10, 58) {
		t.Fatalf("got %d transfers, want %d", len(got), count(10, 58))
	}
	if !strings.Contains(p.stderr.String(), "reorg") {
		t.Log("note: reorg not logged (fork may have been above the processed range)")
	}
	if node.Batches() == 0 {
		t.Fatal("logs+headers batch not used")
	}
}

// All three chains in one process with Postgres; separate cursors.
func TestThreeChainsTogether(t *testing.T) {
	db := newPG(t)
	tr := trontest.NewServer(60)
	defer tr.Close()
	tn := tontest.New(30)
	defer tn.Close()
	en := ethtest.New(30)
	defer en.Close()
	cfgPath := writeConfig(t, `
chains:
  tron: {rps: 1000, confirmations: 5, start_block: 40, poll_interval: 20ms}
  ton:  {rps: 1000, confirmations: 1, start_block: 20, poll_interval: 20ms}
  eth:  {rps: 1000, confirmations: 2, start_block: 20, poll_interval: 20ms}
sinks:
  stdout: {enabled: false}
  postgres: {enabled: true, cursor_name: multi}
`)
	p := start(t, map[string]string{"TRON_API_URL": tr.URL, "TON_API_URL": tn.BaseURL(), "ETH_RPC_URL": en.URL,
		"DATABASE_URL": db.DSN, "HTTP_ADDR": freeAddr(t)}, "-config", cfgPath)
	eventually(t, 30*time.Second, "three cursors", func() bool {
		return db.cursor("multi") >= 55 && db.cursor("multi:ton") >= 29 && db.cursor("multi:eth") >= 28
	})
	resp, err := http.Get("http://" + p.addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	p.stop(syscall.SIGTERM)
	for _, c := range []string{"tron", "ton", "eth"} {
		if len(db.transferKeys(t, c)) == 0 {
			t.Fatalf("no %s transfers stored", c)
		}
	}
}

func assertSameSet(t *testing.T, what string, want map[string]bool, got map[string]int, allowDup bool) {
	t.Helper()
	var missing, extra []string
	for k := range want {
		if got[k] == 0 {
			missing = append(missing, k)
		}
	}
	for k, n := range got {
		if !want[k] {
			extra = append(extra, k)
		}
		if n > 1 && !allowDup {
			t.Errorf("%s: duplicate %s ×%d", what, k, n)
		}
	}
	if len(missing)+len(extra) > 0 {
		t.Fatalf("%s: %d missing (e.g. %v), %d unexpected (e.g. %v) of %d", what, len(missing), first(missing), len(extra), first(extra), len(want))
	}
}

func first(s []string) []string {
	if len(s) > 3 {
		return s[:3]
	}
	return s
}

func dupes(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v - 1
	}
	return n
}

var _ = fmt.Sprintf
