package app

import (
	"log/slog"
	"math/big"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth/erc20"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton/jetton"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trc20"
	"github.com/co-codin/USDT-Tracker/internal/config"
	"github.com/co-codin/USDT-Tracker/internal/listener"
	"github.com/co-codin/USDT-Tracker/internal/metrics"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// chainDef is everything the app needs to know about one chain. Adding a
// chain means adding its package under internal/chain, its config section
// and one entry here – no switch statements elsewhere in this package.
type chainDef struct {
	// newSource builds the block source (client + decoder).
	newSource func(a *App, cm *metrics.Chain) (chain.Source, error)
	// minAmount is the filter threshold in token units.
	minAmount func(cfg config.Config) (*big.Int, error)
	// watchAddresses is the static watch list from the config.
	watchAddresses func(cfg config.Config) []string
	// cursorFile is the file cursor used without Postgres.
	cursorFile func(cfg config.Config) string
	// bareCursorName: the Postgres cursor row is <cursor_name> (TRON, for
	// compatibility) instead of <cursor_name>:<chain>.
	bareCursorName bool
	// tuneListener overrides the shared listener settings (optional).
	tuneListener func(cfg config.Config, lc *listener.Config)
	// warn logs start-up advice (optional).
	warn func(cfg config.Config, log *slog.Logger)
}

var chainDefs = map[string]chainDef{
	model.ChainTron: {
		newSource: func(a *App, cm *metrics.Chain) (chain.Source, error) {
			cfg, opts, log := a.cfg, a.opts, a.log
			contract, err := tron.ParseAddress(cfg.Token.Contract)
			if err != nil {
				return nil, err
			}
			client := tron.NewClient(tron.Options{
				BaseURL: cfg.Tron.APIURL, APIKey: cfg.Tron.APIKey, Timeout: cfg.Tron.Timeout.D(),
				RPS: cfg.Tron.RPS, MaxRetries: cfg.Tron.MaxRetries, Backoff: opts.Backoff,
				UserAgent: "usdt-tracker/" + opts.Version, Observer: cm, Logger: log,
			})
			log.Info("tron source", "api_url", cfg.Tron.APIURL, "api_key_set", cfg.Tron.APIKey != "",
				"token", cfg.Token.Symbol, "contract", contract.String(), "verify_tx_count", cfg.Tron.VerifyTxCount)
			dec := trc20.New(trc20.Token{Contract: contract, Symbol: cfg.Token.Symbol, Decimals: cfg.Token.Decimals})
			src := trc20.NewSource(client, dec, log, cm.DecodeErrors.Inc)
			src.VerifyTxCount(cfg.Tron.VerifyTxCount)
			return src, nil
		},
		minAmount:      func(cfg config.Config) (*big.Int, error) { return cfg.MinAmount() },
		watchAddresses: func(cfg config.Config) []string { return cfg.Filter.WatchAddresses },
		cursorFile:     func(cfg config.Config) string { return cfg.Cursor.File },
		bareCursorName: true,
		warn: func(cfg config.Config, log *slog.Logger) {
			if cfg.Tron.APIKey == "" {
				log.Warn("TRON_PRO_API_KEY not set: using public rate limits, keep tron.rps low")
			}
		},
	},
	model.ChainTON: {
		newSource: func(a *App, cm *metrics.Chain) (chain.Source, error) {
			cfg, opts, log := a.cfg, a.opts, a.log
			master, err := ton.ParseAddress(cfg.TON.JettonMaster)
			if err != nil {
				return nil, wrapf("ton.jetton_master", err)
			}
			client := ton.NewClient(ton.Options{
				BaseURL: cfg.TON.APIURL, APIKey: cfg.TON.APIKey, Timeout: cfg.TON.Timeout.D(),
				RPS: cfg.TON.RPS, MaxRetries: cfg.TON.MaxRetries, Backoff: opts.Backoff,
				UserAgent: "usdt-tracker/" + opts.Version, Observer: cm, Logger: log,
			})
			log.Info("ton source", "api_url", cfg.TON.APIURL, "api_key_set", cfg.TON.APIKey != "",
				"token", cfg.TON.Symbol, "jetton_master", master.Friendly(true, false))
			return jetton.NewSource(client, jetton.Token{Master: master, Symbol: cfg.TON.Symbol, Decimals: cfg.TON.Decimals},
				log.With("chain", model.ChainTON), cm.DecodeErrors.Inc), nil
		},
		minAmount:      func(cfg config.Config) (*big.Int, error) { return cfg.TONMinAmount() },
		watchAddresses: func(cfg config.Config) []string { return cfg.TON.WatchAddresses },
		cursorFile:     func(cfg config.Config) string { return cfg.TON.CursorFile },
		tuneListener: func(cfg config.Config, lc *listener.Config) {
			t := cfg.TON
			lc.Confirmations, lc.PollInterval, lc.StartBlock = t.Confirmations, t.PollInterval.D(), t.StartBlock
			lc.BatchSize, lc.Concurrency = t.BatchSize, t.Concurrency
		},
		warn: func(cfg config.Config, log *slog.Logger) {
			if cfg.TON.APIKey == "" {
				log.Warn("TONCENTER_API_KEY not set: keyless toncenter allows ~1 req/s, which cannot keep up with mainnet head (2 requests per masterchain block); get a key from @tonapibot and raise TON_RPS")
			}
		},
	},
	model.ChainETH: {
		newSource: func(a *App, cm *metrics.Chain) (chain.Source, error) {
			cfg, opts, log := a.cfg, a.opts, a.log
			contract, err := eth.NormalizeAddress(cfg.ETH.Contract)
			if err != nil {
				return nil, wrapf("eth.contract", err)
			}
			client := eth.NewClient(eth.Options{
				URL: cfg.ETH.RPCURL, Timeout: cfg.ETH.Timeout.D(), RPS: cfg.ETH.RPS, MaxRetries: cfg.ETH.MaxRetries,
				Backoff: opts.Backoff, UserAgent: "usdt-tracker/" + opts.Version, Observer: cm, Logger: log,
			})
			// The RPC URL may embed a provider key: log only scheme://host.
			log.Info("eth source", "rpc", eth.RedactURL(cfg.ETH.RPCURL), "token", cfg.ETH.Symbol,
				"contract", eth.Checksum(contract), "log_chunk", cfg.ETH.LogChunk, "head_tag", cfg.ETH.HeadTag)
			return erc20.NewSource(client, erc20.Token{Contract: contract, Symbol: cfg.ETH.Symbol, Decimals: cfg.ETH.Decimals},
				cfg.ETH.LogChunk, log.With("chain", model.ChainETH), erc20.Options{
					HeadTag: eth.HeadTag(cfg.ETH.HeadTag), OnDecodeError: cm.DecodeErrors.Inc, OnReorg: cm.ReorgsDetected.Inc,
				}), nil
		},
		minAmount:      func(cfg config.Config) (*big.Int, error) { return cfg.ETHMinAmount() },
		watchAddresses: func(cfg config.Config) []string { return cfg.ETH.WatchAddresses },
		cursorFile:     func(cfg config.Config) string { return cfg.ETH.CursorFile },
		tuneListener: func(cfg config.Config, lc *listener.Config) {
			e := cfg.ETH
			lc.Confirmations, lc.PollInterval, lc.StartBlock = e.Confirmations, e.PollInterval.D(), e.StartBlock
			lc.BatchSize, lc.Concurrency = e.BatchSize, 1 // ranges are fetched with chunked eth_getLogs
		},
	},
}

func wrapf(field string, err error) error { return &fieldError{field, err} }

type fieldError struct {
	field string
	err   error
}

func (e *fieldError) Error() string { return e.field + ": " + e.err.Error() }
func (e *fieldError) Unwrap() error { return e.err }
