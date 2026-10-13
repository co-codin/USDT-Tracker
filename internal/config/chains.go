package config

import (
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/co-codin/USDT-Tracker/internal/model"
)

// Per-chain configuration.
//
// Preferred shape (one block per chain, presence = enabled):
//
//	chains:
//	  tron: {api_url: …, confirmations: 20}
//	  ton:  {api_key: …, rps: 9}
//	  eth:  {enabled: false, rpc_url: …}
//
// Deprecated aliases that keep working: `chains: [tron, ton]` (a list) plus
// the top-level `tron:`, `ton:`, `eth:` blocks, and for TRON the
// `listener.{confirmations,poll_interval,start_block,batch_size,concurrency}`,
// `token.*`, `cursor.file` and `filter.watch_addresses` keys. Environment
// variables are unchanged (CHAINS, TRON_*, TON_*, ETH_*, …).

// ChainSpec registers a chain with the config loader.
type ChainSpec struct {
	Name string
	// Decode applies a chains.<name> YAML block onto c (unknown keys are errors).
	Decode func(c *Config, n *yaml.Node) error
	// Validate adds errors for an enabled chain (optional).
	Validate func(c *Config, add func(format string, a ...any))
}

var chainSpecs []ChainSpec

// RegisterChain adds a chain to the registry (call from init).
func RegisterChain(s ChainSpec) { chainSpecs = append(chainSpecs, s) }

// KnownChains returns the registered chain names in registration order.
func KnownChains() []string {
	out := make([]string, len(chainSpecs))
	for i, s := range chainSpecs {
		out[i] = s.Name
	}
	return out
}

func chainSpec(name string) (ChainSpec, bool) {
	for _, s := range chainSpecs {
		if s.Name == name {
			return s, true
		}
	}
	return ChainSpec{}, false
}

// The one registration line per chain.
func init() {
	RegisterChain(ChainSpec{Name: model.ChainTron, Decode: decodeTron})
	RegisterChain(ChainSpec{Name: model.ChainTON, Decode: decodeInto(func(c *Config) any { return &c.TON }), Validate: (*Config).validateTONSpec})
	RegisterChain(ChainSpec{Name: model.ChainETH, Decode: decodeInto(func(c *Config) any { return &c.ETH }), Validate: (*Config).validateETHSpec})
}

func (c *Config) validateTONSpec(add func(string, ...any)) { c.validateTON(add) }
func (c *Config) validateETHSpec(add func(string, ...any)) { c.validateETH(add) }

// decodeInto decodes a chain block onto the struct returned by target,
// stripping the shared "enabled" key first.
func decodeInto(target func(*Config) any) func(*Config, *yaml.Node) error {
	return func(c *Config, n *yaml.Node) error { return strictDecode(n, target(c)) }
}

// tronChain is the chains.tron block: the TRON client settings plus the
// listener/token/cursor/watch keys that historically lived elsewhere.
type tronChain struct {
	Tron           `yaml:",inline"`
	Confirmations  int64    `yaml:"confirmations"`
	PollInterval   Duration `yaml:"poll_interval"`
	StartBlock     int64    `yaml:"start_block"`
	BatchSize      int      `yaml:"batch_size"`
	Concurrency    int      `yaml:"concurrency"`
	Contract       string   `yaml:"contract"`
	Symbol         string   `yaml:"symbol"`
	Decimals       int      `yaml:"decimals"`
	WatchAddresses []string `yaml:"watch_addresses"`
	CursorFile     string   `yaml:"cursor_file"`
}

func decodeTron(c *Config, n *yaml.Node) error {
	t := tronChain{
		Tron: c.Tron, Confirmations: c.Listener.Confirmations, PollInterval: c.Listener.PollInterval,
		StartBlock: c.Listener.StartBlock, BatchSize: c.Listener.BatchSize, Concurrency: c.Listener.Concurrency,
		Contract: c.Token.Contract, Symbol: c.Token.Symbol, Decimals: c.Token.Decimals,
		WatchAddresses: c.Filter.WatchAddresses, CursorFile: c.Cursor.File,
	}
	if err := strictDecode(n, &t); err != nil {
		return err
	}
	c.Tron = t.Tron
	c.Listener.Confirmations, c.Listener.PollInterval, c.Listener.StartBlock = t.Confirmations, t.PollInterval, t.StartBlock
	c.Listener.BatchSize, c.Listener.Concurrency = t.BatchSize, t.Concurrency
	c.Token = Token{Contract: t.Contract, Symbol: t.Symbol, Decimals: t.Decimals}
	c.Filter.WatchAddresses, c.Cursor.File = t.WatchAddresses, t.CursorFile
	return nil
}

func strictDecode(n *yaml.Node, out any) error {
	var buf strings.Builder
	enc := yaml.NewEncoder(&buf)
	if err := enc.Encode(n); err != nil {
		return err
	}
	dec := yaml.NewDecoder(strings.NewReader(buf.String()))
	dec.KnownFields(true)
	return dec.Decode(out)
}

// legacyChainKeys are the deprecated top-level per-chain blocks.
var legacyChainKeys = map[string]string{"tron": model.ChainTron, "ton": model.ChainTON, "eth": model.ChainETH}

// applyChainsNode handles the chains: key of the YAML root mapping. A
// mapping (new shape) is decoded per chain and removed from root so the
// rest decodes normally; a sequence (deprecated shape) is left in place.
// It returns deprecation notices.
func applyChainsNode(c *Config, root *yaml.Node) ([]string, error) {
	if root.Kind == yaml.DocumentNode && len(root.Content) == 1 {
		root = root.Content[0]
	}
	if root.Kind != yaml.MappingNode {
		return nil, nil
	}
	var notes []string
	var chainsIdx = -1
	legacy := []string{}
	for i := 0; i+1 < len(root.Content); i += 2 {
		k := root.Content[i].Value
		if k == "chains" {
			chainsIdx = i
		}
		if _, ok := legacyChainKeys[k]; ok {
			legacy = append(legacy, k)
		}
	}
	if chainsIdx < 0 {
		if len(legacy) > 0 {
			notes = append(notes, fmt.Sprintf("top-level %s: blocks are deprecated; move them under chains.<name>", strings.Join(legacy, ", ")))
		}
		return notes, nil
	}
	val := root.Content[chainsIdx+1]
	switch val.Kind {
	case yaml.SequenceNode:
		notes = append(notes, "chains: [list] is deprecated; use a chains.<name> block per chain")
		if len(legacy) > 0 {
			notes = append(notes, fmt.Sprintf("top-level %s: blocks are deprecated; move them under chains.<name>", strings.Join(legacy, ", ")))
		}
		return notes, nil
	case yaml.MappingNode:
	default:
		return nil, fmt.Errorf("line %d: chains must be a mapping (chains.<name>: {...}) or a list", val.Line)
	}
	if len(legacy) > 0 {
		return nil, fmt.Errorf("both chains.<name> blocks and deprecated top-level %s: given; use only chains.<name>", strings.Join(legacy, ", "))
	}
	enabled := map[string]bool{}
	for i := 0; i+1 < len(val.Content); i += 2 {
		name, body := strings.ToLower(val.Content[i].Value), val.Content[i+1]
		spec, ok := chainSpec(name)
		if !ok {
			return nil, fmt.Errorf("line %d: chains: unknown chain %q (want %s)", val.Content[i].Line, name, strings.Join(KnownChains(), "|"))
		}
		on := true
		if body.Kind == yaml.ScalarNode && (body.Tag == "!!null" || body.Value == "") {
			body = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		}
		if body.Kind != yaml.MappingNode {
			return nil, fmt.Errorf("line %d: chains.%s must be a mapping", body.Line, name)
		}
		rest := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		for j := 0; j+1 < len(body.Content); j += 2 {
			if body.Content[j].Value == "enabled" {
				if err := body.Content[j+1].Decode(&on); err != nil {
					return nil, fmt.Errorf("chains.%s.enabled: %w", name, err)
				}
				continue
			}
			rest.Content = append(rest.Content, body.Content[j], body.Content[j+1])
		}
		if err := spec.Decode(c, rest); err != nil {
			return nil, fmt.Errorf("chains.%s: %w", name, err)
		}
		enabled[name] = on
	}
	c.Chains = nil
	for _, s := range chainSpecs {
		if enabled[s.Name] {
			c.Chains = append(c.Chains, s.Name)
		}
	}
	if len(c.Chains) == 0 {
		return nil, errors.New("chains: every chain is disabled")
	}
	root.Content = append(root.Content[:chainsIdx], root.Content[chainsIdx+2:]...)
	return notes, nil
}
