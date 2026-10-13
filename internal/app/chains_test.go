package app

import (
	"testing"

	"github.com/co-codin/USDT-Tracker/internal/chainaddr"
	"github.com/co-codin/USDT-Tracker/internal/config"
)

func TestEveryChainIsRegistered(t *testing.T) {
	for _, c := range chainaddr.Chains {
		d, ok := chainDefs[c]
		if !ok || d.newSource == nil || d.minAmount == nil || d.watchAddresses == nil || d.cursorFile == nil {
			t.Errorf("chain %q is not fully registered in chainDefs", c)
		}
	}
	if len(chainDefs) != len(chainaddr.Chains) {
		t.Errorf("chainDefs has %d chains, chainaddr knows %d", len(chainDefs), len(chainaddr.Chains))
	}
}

// Every chain the config loader knows has an app definition and vice versa.
func TestChainRegistriesAgree(t *testing.T) {
	for _, c := range config.KnownChains() {
		if _, ok := chainDefs[c]; !ok {
			t.Errorf("chain %q registered in config but has no chainDef", c)
		}
	}
	if len(chainDefs) != len(config.KnownChains()) {
		t.Errorf("chainDefs %d vs config %d", len(chainDefs), len(config.KnownChains()))
	}
}
