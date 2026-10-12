package ton

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// ParseShard parses a shard id as returned by toncenter: unsigned hex
// ("8000000000000000", "c000000000000000") or signed decimal ("-9223372036854775808").
func ParseShard(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "-") {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ton: bad shard %q: %w", s, err)
		}
		return uint64(n), nil //nolint:gosec // G115: two's complement reinterpretation is intended
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(s), "0x"), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("ton: bad shard %q: %w", s, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("ton: bad shard %q: zero", s)
	}
	return n, nil
}

// ShardContains reports whether account hash belongs to shard: the bits
// above the shard's lowest set bit are the account-id prefix.
func ShardContains(shard uint64, hash [32]byte) bool {
	low := shard & -shard
	mask := ^(low | (low - 1))
	prefix := binary.BigEndian.Uint64(hash[:8])
	return (prefix^shard)&mask == 0
}
