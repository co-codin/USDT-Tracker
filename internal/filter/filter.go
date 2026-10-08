// Package filter decides which transfers are interesting and why.
package filter

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// Direction restricts which side of a transfer a watched address must be on.
type Direction string

// Supported directions.
const (
	Both     Direction = "both"
	Incoming Direction = "incoming"
	Outgoing Direction = "outgoing"
)

// Mode combines the watch-list and amount conditions.
type Mode string

// Supported modes.
const (
	// Any matches transfers that touch a watched address OR are large.
	Any Mode = "any"
	// All matches transfers that touch a watched address AND are large.
	All Mode = "all"
)

// Config is the filter configuration.
type Config struct {
	WatchAddresses []string  // base58 or hex
	Direction      Direction // applies to WatchAddresses
	MinAmount      *big.Int  // raw units; nil/0 disables the large-transfer rule
	Mode           Mode
	// Dynamic marks the watch list as managed at runtime (admin API): the
	// watch rule is always active, so an empty list matches nothing instead
	// of switching to firehose mode when the last address is removed.
	Dynamic bool
}

// Watch is an extra watched address with its own direction (runtime entries).
type Watch struct {
	Address   string // base58 or hex
	Direction Direction
}

// Filter matches transfers. It is safe for concurrent use (read-only).
type Filter struct {
	watch     map[string]Direction // base58 address -> direction
	watchRule bool                 // the watch-list condition is configured
	min       *big.Int
	mode      Mode
}

// New validates cfg and builds a Filter.
func New(cfg Config) (*Filter, error) { return NewWithWatches(cfg, nil) }

// NewWithWatches builds a Filter from cfg plus extra per-address watches
// (e.g. rows loaded from the database). When an address appears more than
// once with different directions, the union applies (both).
func NewWithWatches(cfg Config, extra []Watch) (*Filter, error) {
	f := &Filter{watch: make(map[string]Direction), mode: cfg.Mode}
	dir, err := checkDirection(cfg.Direction)
	if err != nil {
		return nil, err
	}
	if f.mode == "" {
		f.mode = Any
	}
	switch f.mode {
	case Any, All:
	default:
		return nil, fmt.Errorf("filter: unknown mode %q (want any|all)", cfg.Mode)
	}
	for _, s := range cfg.WatchAddresses {
		if err := f.add(s, dir); err != nil {
			return nil, err
		}
	}
	for _, w := range extra {
		d, err := checkDirection(w.Direction)
		if err != nil {
			return nil, fmt.Errorf("filter: watch address %q: %w", w.Address, err)
		}
		if err := f.add(w.Address, d); err != nil {
			return nil, err
		}
	}
	f.watchRule = len(f.watch) > 0 || cfg.Dynamic
	if cfg.MinAmount != nil && cfg.MinAmount.Sign() > 0 {
		f.min = new(big.Int).Set(cfg.MinAmount)
	}
	return f, nil
}

func checkDirection(d Direction) (Direction, error) {
	switch d {
	case "":
		return Both, nil
	case Both, Incoming, Outgoing:
		return d, nil
	default:
		return "", fmt.Errorf("filter: unknown direction %q (want both|incoming|outgoing)", d)
	}
}

func (f *Filter) add(s string, d Direction) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	a, err := tron.ParseAddress(s)
	if err != nil {
		return fmt.Errorf("filter: watch address %q: %w", s, err)
	}
	key := a.String()
	if prev, ok := f.watch[key]; ok && prev != d {
		d = Both
	}
	f.watch[key] = d
	return nil
}

// MatchesEverything reports whether no rule is configured (firehose mode).
func (f *Filter) MatchesEverything() bool { return !f.watchRule && f.min == nil }

// WatchCount returns the number of distinct watched addresses.
func (f *Filter) WatchCount() int { return len(f.watch) }

// Match returns the reasons a transfer matched, or ok=false.
func (f *Filter) Match(t model.Transfer) (reasons []string, ok bool) {
	if f.MatchesEverything() {
		return []string{model.ReasonAll}, true
	}
	var watched bool
	if len(f.watch) > 0 {
		if d, hit := f.watch[t.To]; hit && d != Outgoing {
			reasons = append(reasons, model.ReasonIncoming)
			watched = true
		}
		if d, hit := f.watch[t.From]; hit && d != Incoming {
			reasons = append(reasons, model.ReasonOutgoing)
			watched = true
		}
	}
	large := f.min != nil && t.Amount != nil && t.Amount.Cmp(f.min) >= 0
	if large {
		reasons = append(reasons, model.ReasonLarge)
	}

	switch {
	case f.mode == All && f.watchRule && f.min != nil:
		ok = watched && large
	default:
		ok = watched || large
	}
	if !ok {
		return nil, false
	}
	return reasons, true
}

// Apply returns the matching transfers with Reasons populated.
func (f *Filter) Apply(in []model.Transfer) []model.Transfer {
	out := make([]model.Transfer, 0, len(in))
	for _, t := range in {
		if r, ok := f.Match(t); ok {
			t.Reasons = r
			out = append(out, t)
		}
	}
	return out
}
