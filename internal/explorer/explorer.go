// Package explorer builds public block-explorer links and display forms of
// addresses for every supported chain (tronscan, tonviewer, etherscan).
package explorer

import (
	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// Default transaction link prefixes.
const (
	TronTx = "https://tronscan.org/#/transaction/"
	TONTx  = "https://tonviewer.com/transaction/"
	ETHTx  = "https://etherscan.io/tx/"
)

// TxPrefix returns the default transaction link prefix for chain c.
func TxPrefix(c string) string {
	switch c {
	case model.ChainTON:
		return TONTx
	case model.ChainETH:
		return ETHTx
	default:
		return TronTx
	}
}

// TxURL returns the explorer link of a transfer's transaction.
func TxURL(t model.Transfer) string { return TxPrefix(t.ChainName()) + t.TxID }

// AddressURL returns the explorer link of address a on chain c.
func AddressURL(c, a string) string {
	switch c {
	case model.ChainTON:
		return "https://tonviewer.com/" + ton.FriendlyFromRaw(a)
	case model.ChainETH:
		return "https://etherscan.io/address/" + eth.Checksum(a)
	default:
		return "https://tronscan.org/#/address/" + a
	}
}

// DisplayAddress returns the form users recognise: wallet-style UQ… on TON,
// EIP-55 checksummed on Ethereum, unchanged base58 on TRON.
func DisplayAddress(c, a string) string {
	switch c {
	case model.ChainTON:
		return ton.FriendlyFromRaw(a)
	case model.ChainETH:
		return eth.Checksum(a)
	default:
		return a
	}
}

// ChainLabel is a short human label for chain c.
func ChainLabel(c string) string {
	switch c {
	case model.ChainTON:
		return "TON"
	case model.ChainETH:
		return "Ethereum"
	default:
		return "TRON"
	}
}
