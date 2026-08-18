#!/usr/bin/env bash
set -euo pipefail

workspace=$(cd "$(dirname "$0")/.." && pwd)
test_data=$(mktemp -d "${TMPDIR:-/tmp}/upgrid-api-tls.XXXXXX")
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$test_data"
}
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout "$test_data/key.pem" \
  -out "$test_data/cert.pem" >/dev/null 2>&1

cargo build --manifest-path "$workspace/Cargo.toml"
target_directory=$(cargo metadata --manifest-path "$workspace/Cargo.toml" \
  --no-deps --format-version 1 | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
"$target_directory/debug/upgrid" \
  --new-cluster \
  --bind 127.0.0.1:18443 \
  --raft-url up://127.0.0.1:18454 \
  --data-dir "$test_data/data" \
  --username admin \
  --password test-password \
  --deployment-key AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= \
  --quic-ca-key AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE= \
  --tls-cert "$test_data/cert.pem" \
  --tls-key "$test_data/key.pem" \
  >"$test_data/server.log" 2>&1 &
server_pid=$!
ready=false

for _ in $(seq 1 100); do
  if curl --fail --silent --cacert "$test_data/cert.pem" \
    https://localhost:18443/healthz >/dev/null; then
    ready=true
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$test_data/server.log"
    exit 1
  fi
  sleep 0.1
done

if [[ "$ready" != true ]]; then
  echo "TLS API did not become ready" >&2
  cat "$test_data/server.log" >&2
  exit 1
fi
http_version=$(curl --fail --silent --http2 --cacert "$test_data/cert.pem" \
  --output /dev/null --write-out '%{http_version}' \
  https://localhost:18443/healthz)
if [[ "$http_version" != "2" ]]; then
  echo "TLS API negotiated HTTP/$http_version instead of HTTP/2" >&2
  exit 1
fi
exec 3<>/dev/tcp/127.0.0.1/18443
request_pids=()
for _ in {1..8}; do
  curl --fail --silent --max-time 5 --cacert "$test_data/cert.pem" \
    https://localhost:18443/healthz >/dev/null &
  request_pids+=("$!")
done
for pid in "${request_pids[@]}"; do
  wait "$pid"
done
exec 3>&-

curl --fail --silent --cacert "$test_data/cert.pem" \
  --cookie-jar "$test_data/cookies" \
  --header 'content-type: application/json' \
  --data '{"username":"admin","password":"test-password"}' \
  https://localhost:18443/api/v1/auth/login >/dev/null
curl --fail --silent --cacert "$test_data/cert.pem" \
  --cookie "$test_data/cookies" \
  https://localhost:18443/api/v1/cluster >/dev/null
echo "Native API TLS verified"
