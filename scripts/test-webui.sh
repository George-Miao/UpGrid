#!/usr/bin/env bash
set -euo pipefail

workspace=$(cd "$(dirname "$0")/.." && pwd)
test_data=$(mktemp -d "${TMPDIR:-/tmp}/upgrid-webui.XXXXXX")
api_base_port="${UPGRID_WEBUI_TEST_API_BASE_PORT:-$((20000 + $$ % 10000))}"
raft_base_port="${UPGRID_WEBUI_TEST_RAFT_BASE_PORT:-$((40000 + $$ % 10000))}"
server_log="$test_data/server.log"
setup_log="$test_data/setup.log"
new_setup_log="$test_data/new-setup.log"
warning_log="$test_data/warning.log"
server_pid=""
setup_pid=""
new_setup_pid=""
warning_pid=""

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
  if [[ -n "$warning_pid" ]]; then
    kill "$warning_pid" 2>/dev/null || true
    wait "$warning_pid" 2>/dev/null || true
  fi
  rm -rf "$test_data"
}
trap cleanup EXIT

cargo build --manifest-path "$workspace/Cargo.toml"
target_directory=$(cargo metadata --manifest-path "$workspace/Cargo.toml" \
  --no-deps --format-version 1 | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
"$target_directory/debug/upgrid" \
  --new-cluster \
  --bind "127.0.0.1:${api_base_port}" \
  --raft-url "up://127.0.0.1:${raft_base_port}" \
  --data-dir "$test_data/data" \
  --username admin \
  --password test-password \
  --deployment-key AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= \
  --quic-ca-key AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE= \
  >"$server_log" 2>&1 &
server_pid=$!

ready=false
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:${api_base_port}/healthz" >/dev/null; then
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
curl -fsS \
  --cookie-jar "$test_data/session.cookies" \
  --header "content-type: application/json" \
  --data '{"username":"admin","password":"test-password"}' \
  "http://127.0.0.1:${api_base_port}/api/v1/auth/login" >/dev/null
session_cookie=$(sed -n 's/^#HttpOnly_127\.0\.0\.1[[:space:]].*[[:space:]]upgrid_session[[:space:]]\(.*\)$/\1/p' "$test_data/session.cookies")
if [[ -z "$session_cookie" ]]; then
  echo "UpGrid did not issue a browser session cookie" >&2
  exit 1
fi
jq --null-input --arg value "$session_cookie" '{
  cookies: [{
    name: "upgrid_session",
    value: $value,
    domain: "127.0.0.1",
    path: "/",
    expires: -1,
    httpOnly: true,
    secure: false,
    sameSite: "Strict"
  }],
  origins: []
}' >"$test_data/storage-state.json"
created_token=$(curl -fsS \
  --cookie "$test_data/session.cookies" \
  --header "content-type: application/json" \
  --data '{"name":"WebUI test harness"}' \
  "http://127.0.0.1:${api_base_port}/api/v1/api-tokens")
api_token=$(printf '%s' "$created_token" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')
if [[ -z "$api_token" ]]; then
  echo "UpGrid did not issue a WebUI test API Token" >&2
  exit 1
fi


"$target_directory/debug/upgrid" \
  --bind "127.0.0.1:$((api_base_port + 1))" \
  --raft-url "up://127.0.0.1:$((raft_base_port + 1))" \
  --data-dir "$test_data/joining-data" \
  >"$setup_log" 2>&1 &
setup_pid=$!

ready=false
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$((api_base_port + 1))/" >/dev/null; then
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
  --bind "127.0.0.1:$((api_base_port + 2))" \
  --raft-url "up://127.0.0.1:$((raft_base_port + 2))" \
  --data-dir "$test_data/new-data" \
  >"$new_setup_log" 2>&1 &
new_setup_pid=$!

ready=false
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$((api_base_port + 2))/" >/dev/null; then
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

"$target_directory/debug/upgrid" \
  --new-cluster \
  --bind "127.0.0.1:$((api_base_port + 3))" \
  --raft-url "up://127.0.0.1:$((raft_base_port + 3))" \
  --data-dir "$test_data/warning-data" \
  --username admin \
  --password test-password \
  >"$warning_log" 2>&1 &
warning_pid=$!
ready=false
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$((api_base_port + 3))/healthz" >/dev/null; then
    ready=true
    break
  fi
  if ! kill -0 "$warning_pid" 2>/dev/null; then
    cat "$warning_log"
    exit 1
  fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then
  echo "UpGrid warning seed did not become ready" >&2
  cat "$warning_log"
  exit 1
fi
kill "$warning_pid"
wait "$warning_pid" 2>/dev/null || true
warning_pid=""
"$target_directory/debug/upgrid" \
  --join not-a-valid-join-token \
  --bind "127.0.0.1:$((api_base_port + 3))" \
  --raft-url "up://127.0.0.1:$((raft_base_port + 3))" \
  --data-dir "$test_data/warning-data" \
  >"$warning_log" 2>&1 &
warning_pid=$!
ready=false
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$((api_base_port + 3))/healthz" >/dev/null; then
    ready=true
    break
  fi
  if ! kill -0 "$warning_pid" 2>/dev/null; then
    cat "$warning_log"
    exit 1
  fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then
  echo "UpGrid warning fixture did not become ready" >&2
  cat "$warning_log"
  exit 1
fi

UPGRID_UI_URL="http://127.0.0.1:${api_base_port}" \
  UPGRID_SETUP_URL="http://127.0.0.1:$((api_base_port + 1))" \
  UPGRID_NEW_SETUP_URL="http://127.0.0.1:$((api_base_port + 2))" \
  UPGRID_WARNING_URL="http://127.0.0.1:$((api_base_port + 3))" \
  UPGRID_EXPECTED_RAFT_URL="up://127.0.0.1:${raft_base_port}" \
  UPGRID_STORAGE_STATE="$test_data/storage-state.json" \
  pnpm --dir "$workspace/frontend" test "$@"
