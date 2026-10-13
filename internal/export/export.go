// Package export renders a per-address USDT statement as CSV (for
// accounting / bookkeeping) streamed from the stored transfers. It serves
// GET /v1/export.csv (admin bearer token) and the `export` CLI subcommand.
package export

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/co-codin/USDT-Tracker/internal/apierr"

	"github.com/co-codin/USDT-Tracker/internal/chainaddr"
	"github.com/co-codin/USDT-Tracker/internal/explorer"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// Header is the CSV header row.
var Header = []string{"time_utc", "chain", "tx_id", "log_index", "from", "to", "direction", "amount", "symbol", "memo", "explorer_url"}

// Query selects the transfers of one address.
type Query struct {
	Chain   string    // tron|ton|eth
	Address string    // canonical form (see chainaddr)
	From    time.Time // inclusive; zero = unbounded
	To      time.Time // exclusive; zero = unbounded
}

// Row is one stored transfer.
type Row struct {
	Time      time.Time
	Chain     string
	TxID      string
	LogIndex  int
	From, To  string
	AmountRaw *big.Int
	Decimals  int
	Symbol    string
	Memo      string
}

// Source streams matching rows in time order, calling fn for each.
type Source interface {
	StreamTransfers(ctx context.Context, q Query, fn func(Row) error) error
}

// ParseQuery validates raw parameters. address is required; chain is
// detected from the address when empty. from/to accept RFC 3339 timestamps
// or dates (YYYY-MM-DD); a date `to` includes that whole day (UTC).
func ParseQuery(chain, address, from, to string) (Query, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return Query{}, errors.New("address is required")
	}
	chain = strings.ToLower(strings.TrimSpace(chain))
	if chain == "" {
		c, err := chainaddr.Detect(address)
		if err != nil {
			return Query{}, err
		}
		chain = c
	}
	if !chainaddr.Valid(chain) {
		return Query{}, fmt.Errorf("unknown chain %q (want tron|ton|eth)", chain)
	}
	addr, err := chainaddr.Normalize(chain, address)
	if err != nil {
		return Query{}, err
	}
	q := Query{Chain: chain, Address: addr}
	if q.From, err = parseTime(from, false); err != nil {
		return Query{}, fmt.Errorf("from: %w", err)
	}
	if q.To, err = parseTime(to, true); err != nil {
		return Query{}, fmt.Errorf("to: %w", err)
	}
	if !q.From.IsZero() && !q.To.IsZero() && !q.To.After(q.From) {
		return Query{}, errors.New("to must be after from")
	}
	return q, nil
}

func parseTime(s string, endOfDay bool) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q (want YYYY-MM-DD or RFC 3339)", s)
	}
	if endOfDay {
		t = t.AddDate(0, 0, 1)
	}
	return t, nil
}

// Direction of a transfer relative to address: in, out or self.
func Direction(address, from, to string) string {
	switch {
	case from == address && to == address:
		return "self"
	case to == address:
		return "in"
	default:
		return "out"
	}
}

// Options tune Write.
type Options struct {
	BOM   bool   // prefix the UTF-8 byte order mark (Excel)
	Flush func() // called periodically so HTTP clients get data early
}

// Write streams the CSV for q from src to w and returns the row count.
// Times are UTC (RFC 3339), amounts are exact decimal strings.
func Write(ctx context.Context, w io.Writer, src Source, q Query, o Options) (int, error) {
	if o.BOM {
		if _, err := io.WriteString(w, BOM); err != nil {
			return 0, err
		}
	}
	flush := o.Flush
	cw := csv.NewWriter(w)
	if err := cw.Write(Header); err != nil {
		return 0, err
	}
	n := 0
	err := src.StreamTransfers(ctx, q, func(r Row) error {
		t := model.Transfer{Chain: r.Chain, TxID: r.TxID}
		rec := []string{
			r.Time.UTC().Format(time.RFC3339), r.Chain, r.TxID, fmt.Sprint(r.LogIndex),
			explorer.DisplayAddress(r.Chain, r.From), explorer.DisplayAddress(r.Chain, r.To),
			Direction(q.Address, r.From, r.To), model.FormatUnits(r.AmountRaw, r.Decimals), safe(r.Symbol),
			safe(r.Memo), explorer.TxURL(t),
		}
		if err := cw.Write(rec); err != nil {
			return err
		}
		n++
		if n%500 == 0 {
			cw.Flush()
			if err := cw.Error(); err != nil {
				return err
			}
			if flush != nil {
				flush()
			}
		}
		return nil
	})
	cw.Flush()
	if err == nil {
		err = cw.Error()
	}
	return n, err
}

// safe neutralises spreadsheet formula injection in free-text cells (memo
// comes from the sender of a transfer). Spreadsheets may ignore leading
// whitespace/control characters, so those are skipped before the first
// character is checked against = + - @ | % and their full-width forms; a
// risky cell is prefixed with a single quote. The text itself is kept
// (encoding/csv quotes embedded newlines).
func safe(s string) string {
	if s == "" {
		return s
	}
	first := strings.TrimLeftFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
	if first == "" {
		return s
	}
	r, _ := utf8.DecodeRuneInString(first)
	if strings.ContainsRune("=+-@|%＝＋－＠｜％\t", r) {
		return "'" + s
	}
	return s
}

// BOM is the UTF-8 byte order mark; Excel needs it to read UTF-8 CSV
// (Chinese / Cyrillic memos) correctly.
const BOM = "\ufeff"

// Handler serves GET /v1/export.csv?address=&chain=&from=&to= behind the
// admin bearer token. A nil source answers 503 (Postgres sink disabled).
type Handler struct {
	tokenHash [32]byte
	enabled   bool
	src       Source
	log       *slog.Logger
}

// NewHandler creates the handler; an empty token disables it (404).
func NewHandler(token string, src Source, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Handler{tokenHash: sha256.Sum256([]byte(token)), enabled: token != "", src: src, log: log}
}

func (h *Handler) authorized(r *http.Request) bool {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return false
	}
	got := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return subtle.ConstantTimeCompare(got[:], h.tokenHash[:]) == 1
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.enabled {
		apierr.Write(w, http.StatusNotFound, apierr.CodeAPIDisabled, "admin API disabled: set API_TOKEN (or api.token) to enable it")
		return
	}
	if !h.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="usdt-tracker"`)
		apierr.Write(w, http.StatusUnauthorized, apierr.CodeUnauthorized, "missing or invalid bearer token")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		apierr.Write(w, http.StatusMethodNotAllowed, apierr.CodeMethodNotAllowed, "method not allowed; use GET, HEAD")
		return
	}
	if h.src == nil {
		apierr.Write(w, http.StatusServiceUnavailable, apierr.CodePostgresRequired, "export needs the postgres sink (SINK_POSTGRES_ENABLED=true)")
		return
	}
	v := r.URL.Query()
	q, err := ParseQuery(v.Get("chain"), v.Get("address"), v.Get("from"), v.Get("to"))
	if err != nil {
		apierr.Write(w, http.StatusBadRequest, apierr.CodeInvalidRequest, err.Error())
		return
	}
	bom := false
	switch v.Get("bom") {
	case "", "0", "false":
	case "1", "true":
		bom = true
	default:
		apierr.Write(w, http.StatusBadRequest, apierr.CodeInvalidRequest, "bom must be 1 or 0")
		return
	}
	name := fmt.Sprintf("usdt-%s-%s.csv", q.Chain, explorer.DisplayAddress(q.Chain, q.Address))
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+strings.NewReplacer(`"`, "", ":", "_").Replace(name)+`"`)
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		return
	}
	flush := func() {
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
	n, err := Write(r.Context(), w, h.src, q, Options{BOM: bom, Flush: flush})
	if err != nil {
		// Headers are already sent: the truncated body is the signal; log it.
		h.log.Error("csv export failed", "chain", q.Chain, "rows", n, "err", err)
		return
	}
	h.log.Info("csv export", "chain", q.Chain, "rows", n)
}
