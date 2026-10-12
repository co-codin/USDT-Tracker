package eth

import (
	"encoding/hex"
	"testing"
)

func TestTransferTopicIsKeccakOfSignature(t *testing.T) {
	got := "0x" + hex.EncodeToString(Keccak256([]byte("Transfer(address,address,uint256)")))
	if got != TransferTopic {
		t.Fatalf("topic = %s, want %s", got, TransferTopic)
	}
}

func TestChecksumEIP55Vectors(t *testing.T) {
	// Test vectors from EIP-55.
	for _, want := range []string{
		"0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
		"0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
		"0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
		"0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
		USDTContract,
	} {
		if got := Checksum(want); got != want {
			t.Errorf("Checksum = %s, want %s", got, want)
		}
		n, err := NormalizeAddress(want)
		if err != nil {
			t.Fatalf("%s: %v", want, err)
		}
		if Checksum(n) != want {
			t.Errorf("roundtrip %s", want)
		}
	}
}

func TestNormalizeAddress(t *testing.T) {
	ok := map[string]string{
		"0xdac17f958d2ee523a2206206994597c13d831ec7":   "0xdac17f958d2ee523a2206206994597c13d831ec7",
		"0xDAC17F958D2EE523A2206206994597C13D831EC7":   "0xdac17f958d2ee523a2206206994597c13d831ec7",
		" 0xdAC17F958D2ee523a2206206994597C13D831ec7 ": "0xdac17f958d2ee523a2206206994597c13d831ec7",
	}
	for in, want := range ok {
		got, err := NormalizeAddress(in)
		if err != nil || got != want {
			t.Errorf("%q: got %q, %v", in, got, err)
		}
	}
	for _, bad := range []string{
		"", "dac17f958d2ee523a2206206994597c13d831ec7", "0xdac17f958d2ee523a2206206994597c13d831ec",
		"0xdAC17F958D2ee523a2206206994597C13D831eC7", // bad checksum
		"0xzac17f958d2ee523a2206206994597c13d831ec7",
		"TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
	} {
		if _, err := NormalizeAddress(bad); err == nil {
			t.Errorf("%q: want error", bad)
		}
	}
}

func TestTopicAddress(t *testing.T) {
	a, err := TopicAddress("0x000000000000000000000000c7bbec68d12a0d1830360f8ec58fa599ba1b0e9b")
	if err != nil || a != "0xc7bbec68d12a0d1830360f8ec58fa599ba1b0e9b" {
		t.Fatalf("%q %v", a, err)
	}
	if _, err := TopicAddress("0x100000000000000000000000c7bbec68d12a0d1830360f8ec58fa599ba1b0e9b"); err == nil {
		t.Fatal("want padding error")
	}
	if _, err := TopicAddress("0x12"); err == nil {
		t.Fatal("want length error")
	}
}
