package filter

import (
	"math/big"
	"testing"

	"github.com/co-codin/USDT-Tracker/internal/model"
)

const (
	tonA = "0:9db2cae853cf2db870ea06d1e8286f202cd9ad689dfc74b98195520d06495859"
	tonB = "0:6fc4b8ae3a3c8bbf09a0ee6dd07be32aabc0b661d017a82f5d28675c9bd7ab81"
)

func TestTONFilter(t *testing.T) {
	// user-friendly config address matches raw transfer addresses
	f, err := NewWithWatches(Config{Chain: "ton", WatchAddresses: []string{"UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg"}, Direction: Incoming},
		[]Watch{
			{Chain: "ton", Address: "0:6FC4B8AE3A3C8BBF09A0EE6DD07BE32AABC0B661D017A82F5D28675C9BD7AB81", Direction: Outgoing},
			{Chain: "tron", Address: "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"}, // other chain: ignored
			{Address: "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"},                // empty chain = tron: ignored
		})
	if err != nil {
		t.Fatal(err)
	}
	if f.WatchCount() != 2 {
		t.Fatalf("watch count %d", f.WatchCount())
	}
	tr := model.Transfer{Chain: "ton", From: tonB, To: tonA, Amount: big.NewInt(1)}
	r, ok := f.Match(tr)
	if !ok || len(r) != 2 {
		t.Fatalf("match %v %v", r, ok)
	}
	if _, ok := f.Match(model.Transfer{Chain: "ton", From: tonA, To: tonB, Amount: big.NewInt(1)}); ok {
		t.Error("directions not applied")
	}
	if _, err := New(Config{Chain: "ton", WatchAddresses: []string{"TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"}}); err == nil {
		t.Error("tron address accepted for ton filter")
	}
	if _, err := New(Config{Chain: "tron", WatchAddresses: []string{tonA}}); err == nil {
		t.Error("ton address accepted for tron filter")
	}
	if _, err := New(Config{Chain: "eth"}); err == nil {
		t.Error("unknown chain accepted")
	}
}
