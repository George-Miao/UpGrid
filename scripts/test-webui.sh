#!/usr/bin/env bash
set -euo pipefail

workspace=$(cd "$(dirname "$0")/.." && pwd)
test_data=$(mktemp -d "${TMPDIR:-/tmp}/upgrid-webui.XXXXXX")
server_log="$test_data/server.log"
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$test_data"
}
trap cleanup EXIT

cargo build --manifest-path "$workspace/Cargo.toml"
target_directory=$(cargo metadata --manifest-path "$workspace/Cargo.toml" \
  --no-deps --format-version 1 | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
"$target_directory/debug/upgrid" \
  --bind 127.0.0.1:18080 \
  --raft-url up://127.0.0.1:18451 \
  --data-dir "$test_data/data" \
  --username admin \
  --password test-password \
  --secret-key AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= \
  >"$server_log" 2>&1 &
server_pid=$!

ready=false
for _ in $(seq 1 100); do
  if curl -fsS -u admin:test-password \
    http://127.0.0.1:18080/api/v1/targets >/dev/null; then
    ready=true
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$server_log"
    exit 1
  fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then
  echo "UpGrid did not become ready" >&2
  cat "$server_log"
  exit 1
fi

UPGRID_UI_URL=http://127.0.0.1:18080 \
  UPGRID_USERNAME=admin \
  UPGRID_PASSWORD=test-password \
  pnpm --dir "$workspace/frontend" test
