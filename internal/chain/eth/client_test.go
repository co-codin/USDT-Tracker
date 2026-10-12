package eth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth/ethtest"
	"github.com/co-codin/USDT-Tracker/internal/retry"
)

var fast = retry.Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond}

func TestClientBlockNumberAndRetry429(t *testing.T) {
	n := ethtest.New(1234)
	defer n.Close()
	n.Fail429(2)
	c := eth.NewClient(eth.Options{URL: n.URL, RPS: 1000, Backoff: fast})
	h, err := c.BlockNumber(context.Background())
	if err != nil || h != 1234 {
		t.Fatalf("head %d %v", h, err)
	}
	if got := n.Calls("eth_blockNumber"); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
}

func TestClientRangeTooLarge(t *testing.T) {
	n := ethtest.New(100)
	defer n.Close()
	n.MaxRange = 5
	c := eth.NewClient(eth.Options{URL: n.URL, RPS: 1000, Backoff: fast})
	_, err := c.GetLogs(context.Background(), eth.LogFilter{FromBlock: 1, ToBlock: 10, Address: ethtest.USDT, Topics: []string{eth.TransferTopic}})
	if !errors.Is(err, eth.ErrRangeTooLarge) {
		t.Fatalf("err = %v", err)
	}
	if n.Calls("eth_getLogs") != 1 {
		t.Fatal("range errors must not be retried")
	}
}

func TestClientPermanentErrorAndNoURLLeak(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer srv.Close()
	c := eth.NewClient(eth.Options{URL: srv.URL + "/v3/SECRETKEY", RPS: 1000, Backoff: fast})
	_, err := c.BlockNumber(context.Background())
	if err == nil || strings.Contains(err.Error(), "SECRETKEY") {
		t.Fatalf("err = %v", err)
	}
	c = eth.NewClient(eth.Options{URL: "http://127.0.0.1:1/v3/SECRETKEY", RPS: 1000, MaxRetries: 1, Backoff: fast})
	_, err = c.BlockNumber(context.Background())
	if err == nil || strings.Contains(err.Error(), "SECRETKEY") {
		t.Fatalf("network err leaks url: %v", err)
	}
}

func TestRedactURL(t *testing.T) {
	cases := map[string]string{
		"https://mainnet.infura.io/v3/abc":    "https://mainnet.infura.io/***",
		"https://ethereum-rpc.publicnode.com": "https://ethereum-rpc.publicnode.com",
		"https://user:pw@node.example/":       "https://node.example/***",
		"https://node.example/?apikey=secret": "https://node.example/***",
		"::bad":                               "<invalid url>",
	}
	for in, want := range cases {
		if got := eth.RedactURL(in); got != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}

func TestQuantity(t *testing.T) {
	if eth.Quantity(255) != "0xff" {
		t.Fatal()
	}
	if n, err := eth.ParseQuantity("0x18f7600"); err != nil || n != 26179072 {
		t.Fatal(n, err)
	}
	for _, bad := range []string{"", "0x", "12", "0xzz"} {
		if _, err := eth.ParseQuantity(bad); err == nil {
			t.Errorf("%q", bad)
		}
	}
}
