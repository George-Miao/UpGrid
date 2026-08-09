#!/usr/bin/env bash
set -euo pipefail

workspace=$(cd "$(dirname "$0")/.." && pwd)
test_data=$(mktemp -d "${TMPDIR:-/tmp}/upgrid-webui.XXXXXX")
server_log="$test_data/server.log"
setup_log="$test_data/setup.log"
new_setup_log="$test_data/new-setup.log"
server_pid=""
setup_pid=""
new_setup_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [[ -n "$setup_pid" ]]; then
    kill "$setup_pid" 2>/dev/null || true
    wait "$setup_pid" 2>/dev/null || true
  fi
  if [[ -n "$new_setup_pid" ]]; then
    kill "$new_setup_pid" 2>/dev/null || true
    wait "$new_setup_pid" 2>/dev/null || true
  fi
  rm -rf "$test_data"
}
trap cleanup EXIT

cargo build --manifest-path "$workspace/Cargo.toml"
target_directory=$(cargo metadata --manifest-path "$workspace/Cargo.toml" \
  --no-deps --format-version 1 | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
"$target_directory/debug/upgrid" \
  --new-cluster \
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

"$target_directory/debug/upgrid" \
  --bind 127.0.0.1:18081 \
  --raft-url up://127.0.0.1:18452 \
  --data-dir "$test_data/joining-data" \
  --username admin \
  --password test-password \
  >"$setup_log" 2>&1 &
setup_pid=$!

ready=false
for _ in $(seq 1 100); do
  if curl -fsS -u admin:test-password http://127.0.0.1:18081/ >/dev/null; then
    ready=true
    break
  fi
  if ! kill -0 "$setup_pid" 2>/dev/null; then
    cat "$setup_log"
    exit 1
  fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then
  echo "UpGrid setup WebUI did not become ready" >&2
  cat "$setup_log"
  exit 1
fi

"$target_directory/debug/upgrid" \
  --bind 127.0.0.1:18082 \
  --raft-url up://127.0.0.1:18453 \
  --data-dir "$test_data/new-data" \
  --username admin \
  --password test-password \
  >"$new_setup_log" 2>&1 &
new_setup_pid=$!

ready=false
for _ in $(seq 1 100); do
  if curl -fsS -u admin:test-password http://127.0.0.1:18082/ >/dev/null; then
    ready=true
    break
  fi
  if ! kill -0 "$new_setup_pid" 2>/dev/null; then
    cat "$new_setup_log"
    exit 1
  fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then
  echo "UpGrid new-Cluster WebUI did not become ready" >&2
  cat "$new_setup_log"
  exit 1
fi

UPGRID_UI_URL=http://127.0.0.1:18080 \
  UPGRID_SETUP_URL=http://127.0.0.1:18081 \
  UPGRID_NEW_SETUP_URL=http://127.0.0.1:18082 \
  UPGRID_USERNAME=admin \
  UPGRID_PASSWORD=test-password \
  pnpm --dir "$workspace/frontend" test "$@"
