// Package filter decides which transfers are interesting and why.
package filter

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/co-codin/tron-usdt-listener/internal/model"
	"github.com/co-codin/tron-usdt-listener/internal/tron"
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
	WatchAddresses []string // base58 or hex
	Direction      Direction
	MinAmount      *big.Int // raw units; nil/0 disables the large-transfer rule
	Mode           Mode
}

// Filter matches transfers. It is safe for concurrent use (read-only).
type Filter struct {
	watch map[string]struct{} // base58 addresses
	dir   Direction
	min   *big.Int
	mode  Mode
}

// New validates cfg and builds a Filter.
func New(cfg Config) (*Filter, error) {
	f := &Filter{watch: make(map[string]struct{}), dir: cfg.Direction, mode: cfg.Mode}
	if f.dir == "" {
		f.dir = Both
	}
	if f.mode == "" {
		f.mode = Any
	}
	switch f.dir {
	case Both, Incoming, Outgoing:
	default:
		return nil, fmt.Errorf("filter: unknown direction %q (want both|incoming|outgoing)", cfg.Direction)
	}
	switch f.mode {
	case Any, All:
	default:
		return nil, fmt.Errorf("filter: unknown mode %q (want any|all)", cfg.Mode)
	}
	for _, s := range cfg.WatchAddresses {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		a, err := tron.ParseAddress(s)
		if err != nil {
			return nil, fmt.Errorf("filter: watch address %q: %w", s, err)
		}
		f.watch[a.String()] = struct{}{}
	}
	if cfg.MinAmount != nil && cfg.MinAmount.Sign() > 0 {
		f.min = new(big.Int).Set(cfg.MinAmount)
	}
	return f, nil
}

// MatchesEverything reports whether no rule is configured (firehose mode).
func (f *Filter) MatchesEverything() bool { return len(f.watch) == 0 && f.min == nil }

// Match returns the reasons a transfer matched, or ok=false.
func (f *Filter) Match(t model.Transfer) (reasons []string, ok bool) {
	if f.MatchesEverything() {
		return []string{model.ReasonAll}, true
	}
	var watched bool
	if len(f.watch) > 0 {
		if f.dir != Outgoing {
			if _, hit := f.watch[t.To]; hit {
				reasons = append(reasons, model.ReasonIncoming)
				watched = true
			}
		}
		if f.dir != Incoming {
			if _, hit := f.watch[t.From]; hit {
				reasons = append(reasons, model.ReasonOutgoing)
				watched = true
			}
		}
	}
	large := f.min != nil && t.Amount != nil && t.Amount.Cmp(f.min) >= 0
	if large {
		reasons = append(reasons, model.ReasonLarge)
	}

	switch {
	case f.mode == All && len(f.watch) > 0 && f.min != nil:
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
