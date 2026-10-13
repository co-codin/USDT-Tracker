//go:build e2e

// Package e2e runs the real usdt-tracker binary (built once in TestMain)
// against scripted mock chain nodes (TRON, TON, Ethereum), a real Postgres
// (TEST_DATABASE_URL), a mock Telegram Bot API and an HMAC-verifying webhook
// receiver. Run with: make e2e TEST_DATABASE_URL=postgres://...
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-codin/USDT-Tracker/pkg/webhooksig"
)

var binary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "usdt-e2e-")
	if err != nil {
		panic(err)
	}
	binary = filepath.Join(dir, "usdt-tracker")
	if b := os.Getenv("E2E_BINARY"); b != "" {
		binary = b
	} else {
		cmd := exec.Command("go", "build", "-o", binary, "./cmd/usdt-tracker")
		cmd.Dir = repoRoot()
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			panic("build: " + err.Error())
		}
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func repoRoot() string {
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		panic(err)
	}
	return filepath.Dir(strings.TrimSpace(string(out)))
}

const apiToken = "e2e-token-0123456789abcdef"

// proc is one running usdt-tracker process.
type proc struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdout *syncBuf
	stderr *syncBuf
	addr   string
	done   chan struct{}
	err    error
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// writeConfig writes a YAML config file and returns its path.
func writeConfig(t *testing.T, yaml string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// start runs the binary with args and env (on top of a minimal clean env).
func start(t *testing.T, env map[string]string, args ...string) *proc {
	t.Helper()
	addr := env["HTTP_ADDR"]
	cmd := exec.Command(binary, args...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "LOG_LEVEL=info"}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Dir = t.TempDir()
	p := &proc{t: t, cmd: cmd, stdout: &syncBuf{}, stderr: &syncBuf{}, addr: addr, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = p.stdout, p.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = cmd.Process.Kill()
			<-p.done
		}
		if t.Failed() {
			t.Logf("stderr of %v:\n%s", args, tailStr(p.stderr.String(), 4000))
		}
	})
	if addr != "" {
		p.waitHTTP("/livez", 15*time.Second)
	}
	return p
}

func tailStr(s string, n int) string {
	if len(s) > n {
		return "…" + s[len(s)-n:]
	}
	return s
}

func (p *proc) waitHTTP(path string, d time.Duration) {
	p.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			p.t.Fatalf("process exited early: %v\n%s", p.err, tailStr(p.stderr.String(), 4000))
		default:
		}
		if resp, err := http.Get("http://" + p.addr + path); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.t.Fatalf("%s not ready", path)
}

// stop sends sig and returns the exit code.
func (p *proc) stop(sig syscall.Signal) int {
	p.t.Helper()
	_ = p.cmd.Process.Signal(sig)
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		p.t.Fatal("process did not exit")
	}
	var ee *exec.ExitError
	if errors.As(p.err, &ee) {
		return ee.ExitCode()
	}
	if p.err != nil {
		return -1
	}
	return 0
}

// api does an authenticated admin API call.
func (p *proc) api(method, path string, body any) (int, []byte) {
	p.t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, "http://"+p.addr+path, r)
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		p.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// --- Postgres -----------------------------------------------------------------

type pgDB struct {
	DSN  string
	pool *pgxpool.Pool
}

func newPG(t *testing.T) *pgDB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("e2e_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		pool.Close()
	})
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	sp, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sp.Close)
	return &pgDB{DSN: u.String(), pool: sp}
}

func (d *pgDB) cursor(name string) int64 {
	var n int64
	_ = d.pool.QueryRow(context.Background(), `SELECT last_block FROM listener_cursor WHERE name=$1`, name).Scan(&n)
	return n
}

// transferKeys returns chain|tx|log -> count (the unique constraint makes count 1).
func (d *pgDB) transferKeys(t *testing.T, chain string) map[string]int {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `SELECT tx_id, log_index FROM trc20_transfers WHERE chain=$1`, chain)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var tx string
		var li int
		_ = rows.Scan(&tx, &li)
		out[fmt.Sprintf("%s:%d", tx, li)]++
	}
	return out
}

// --- webhook receiver ---------------------------------------------------------

type webhookRx struct {
	*http.Server
	URL      string
	secret   string
	mu       sync.Mutex
	got      map[string]int // tx:log -> deliveries
	bad      int
	maxBlock int64
	fail     int // next n requests answer 503
}

func newWebhookRx(t *testing.T, secret string) *webhookRx {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	w := &webhookRx{secret: secret, got: map[string]int{}, URL: "http://" + l.Addr().String() + "/hook?token=s3cret"}
	w.Server = &http.Server{Handler: http.HandlerFunc(w.serve), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = w.Serve(l) }()
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func (w *webhookRx) serve(rw http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fail > 0 {
		w.fail--
		rw.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if err := webhooksig.Verify([]byte(w.secret), r.Header.Get(webhooksig.HeaderTimestamp), r.Header.Get(webhooksig.HeaderSignature), body, 5*time.Minute, time.Now()); err != nil {
		w.bad++
		rw.WriteHeader(http.StatusUnauthorized)
		return
	}
	var p struct {
		BlockNumber int64 `json:"block_number"`
		Transfers   []struct {
			TxID     string `json:"tx_id"`
			LogIndex int    `json:"log_index"`
		} `json:"transfers"`
	}
	if json.Unmarshal(body, &p) != nil {
		w.bad++
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	for _, tr := range p.Transfers {
		w.got[fmt.Sprintf("%s:%d", tr.TxID, tr.LogIndex)]++
	}
	w.maxBlock = max(w.maxBlock, p.BlockNumber)
	rw.WriteHeader(http.StatusNoContent)
}

func (w *webhookRx) snapshot() (map[string]int, int, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	cp := make(map[string]int, len(w.got))
	for k, v := range w.got {
		cp[k] = v
	}
	return cp, w.bad, w.maxBlock
}

// stdoutTransfers parses the stdout sink's JSON lines.
func stdoutTransfers(t *testing.T, s string) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
		if l == "" {
			continue
		}
		var m struct {
			Msg string `json:"msg"`
			ID  string `json:"id"`
		}
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("stdout line is not JSON: %q", l)
		}
		if m.Msg == "transfer" {
			out[m.ID]++
		}
	}
	return out
}
