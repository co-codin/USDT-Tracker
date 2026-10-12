package explorer

import (
	"testing"

	"github.com/co-codin/USDT-Tracker/internal/model"
)

func TestLinks(t *testing.T) {
	tr := model.Transfer{Chain: model.ChainETH, TxID: "0xabc"}
	if TxURL(tr) != "https://etherscan.io/tx/0xabc" {
		t.Fatal(TxURL(tr))
	}
	if TxURL(model.Transfer{TxID: "ff"}) != "https://tronscan.org/#/transaction/ff" {
		t.Fatal()
	}
	if TxURL(model.Transfer{Chain: model.ChainTON, TxID: "ee"}) != "https://tonviewer.com/transaction/ee" {
		t.Fatal()
	}
	if got := DisplayAddress(model.ChainETH, "0xdac17f958d2ee523a2206206994597c13d831ec7"); got != "0xdAC17F958D2ee523a2206206994597C13D831ec7" {
		t.Fatal(got)
	}
	if got := AddressURL(model.ChainTron, "TX"); got != "https://tronscan.org/#/address/TX" {
		t.Fatal(got)
	}
	if ChainLabel(model.ChainETH) != "Ethereum" || ChainLabel("") != "TRON" {
		t.Fatal()
	}
}
