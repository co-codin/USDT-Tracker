#!/usr/bin/env bash
# Read-only live smoke test against public mainnet endpoints. Runs the real
# binary per chain for SECS seconds from the current head (stdout sink, temp
# cursor, no database) and checks it starts, advances and emits transfers.
# Keys are read from the environment only (TRON_PRO_API_KEY,
# TONCENTER_API_KEY, ETH_RPC_URL); never commit them.
set -uo pipefail
SECS=${SECS:-60}
BIN=${BIN:-./bin/usdt-tracker}
[ -x "$BIN" ] || go build -o "$BIN" ./cmd/usdt-tracker
export ETH_RPC_URL=${ETH_RPC_URL:-https://ethereum-rpc.publicnode.com}
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
fail=0
for chain in ${SMOKE_CHAINS:-tron eth ton}; do
  out=$tmp/$chain.out err=$tmp/$chain.err
  env CHAINS=$chain CURSOR_FILE=$tmp/c-$chain.json TON_CURSOR_FILE=$tmp/t.json ETH_CURSOR_FILE=$tmp/e.json \
    HTTP_ADDR= LOG_LEVEL=info timeout -s TERM "$SECS" "$BIN" >"$out" 2>"$err"
  code=$?
  n=$(grep -c '"tx_id"' "$out" || true)
  started=$(grep -c 'listener starting' "$err" || true)
  errs=$(grep -c '"level":"ERROR"' "$err" || true)
  if [ "$started" -ge 1 ] && [ "$n" -ge 1 ] && { [ $code -eq 0 ] || [ $code -eq 124 ]; }; then
    echo "PASS $chain: $n transfers in ${SECS}s, $errs error lines"
  else
    echo "FAIL $chain: exit=$code started=$started transfers=$n errors=$errs"; tail -5 "$err"; fail=1
  fi
done
exit $fail
