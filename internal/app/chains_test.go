package app

import (
	"testing"

	"github.com/co-codin/USDT-Tracker/internal/chainaddr"
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
