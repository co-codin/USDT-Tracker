package watch

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// alice and bob are declared in api_test.go.

func TestNormalizeAddress(t *testing.T) {
	if got, err := NormalizeAddress("  " + alice + " "); err != nil || got != alice {
		t.Fatalf("valid address: %q %v", got, err)
	}
	for _, bad := range []string{
		"",
		"Tnope",
		"TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yX", // checksum
		"TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2y0", // '0' is not base58
		"41f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07", // hex is not accepted by the API
		"0xf4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07", // EVM
		"1BoatSLRHtKNngkdXEeobR76b53LETtpyT",         // bitcoin (wrong prefix)
		strings.Repeat("T", 34),
	} {
		if _, err := NormalizeAddress(bad); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}
}

func TestNormalizeDirectionAndLabel(t *testing.T) {
	for in, want := range map[string]string{"": "both", "BOTH": "both", " incoming ": "incoming", "outgoing": "outgoing"} {
		if got, err := NormalizeDirection(in); err != nil || got != want {
			t.Errorf("direction %q = %q %v", in, got, err)
		}
	}
	if _, err := NormalizeDirection("sideways"); err == nil {
		t.Error("expected error for unknown direction")
	}
	s := func(v string) *string { return &v }
	if l, err := NormalizeLabel(s("  order 42 ")); err != nil || *l != "order 42" {
		t.Errorf("label: %v %v", l, err)
	}
	if l, err := NormalizeLabel(s("   ")); err != nil || l != nil {
		t.Errorf("blank label must become nil: %v %v", l, err)
	}
	if l, err := NormalizeLabel(nil); err != nil || l != nil {
		t.Errorf("nil label: %v %v", l, err)
	}
	if _, err := NormalizeLabel(s(strings.Repeat("я", MaxLabelLen+1))); err == nil {
		t.Error("expected error for long label")
	}
	if _, err := NormalizeLabel(s(strings.Repeat("я", MaxLabelLen))); err != nil {
		t.Errorf("label of exactly %d runes must be accepted: %v", MaxLabelLen, err)
	}
}

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStore()
	if _, err := m.AddWatch(ctx, Entry{Address: alice, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AddWatch(ctx, Entry{Address: alice, Enabled: true}); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate: %v", err)
	}
	e, err := m.AddWatch(ctx, Entry{Address: bob, Direction: DirectionIncoming, Enabled: true})
	if err != nil || e.CreatedAt.IsZero() || e.Direction != DirectionIncoming {
		t.Fatalf("add bob: %+v %v", e, err)
	}
	all, _ := m.ListWatches(ctx)
	if len(all) != 2 || all[0].Address != alice || all[0].Direction != DirectionBoth {
		t.Fatalf("list: %+v", all)
	}
	_ = m.SetEnabled(alice, false)
	if en, _ := m.EnabledWatches(ctx); len(en) != 1 || en[0].Address != bob {
		t.Fatalf("enabled: %+v", en)
	}
	if all, _ := m.ListWatches(ctx); len(all) != 2 {
		t.Fatalf("EnabledWatches must not mutate the store: %+v", all)
	}
	if err := m.DeleteWatch(ctx, bob); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteWatch(ctx, bob); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing: %v", err)
	}
	m.Err = errors.New("db down")
	if _, err := m.EnabledWatches(ctx); err == nil {
		t.Fatal("expected injected error")
	}
}
