#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
api_base_port="${UPGRID_TEST_API_BASE_PORT:-19080}"
raft_base_port="${UPGRID_TEST_RAFT_BASE_PORT:-19451}"
settle_seconds="${UPGRID_TEST_SETTLE_SECONDS:-4}"
node_count="${UPGRID_TEST_NODE_COUNT:-3}"
rust_log="${UPGRID_TEST_RUST_LOG:-info}"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/upgrid-cluster.XXXXXX")"
username="cluster-test"
password="cluster-test-password"
secret_key="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

cargo build
target_directory="$(cargo metadata --no-deps --format-version 1 | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
binary="${target_directory}/debug/upgrid"

start_node() {
  local number="$1"
  local api_port=$((api_base_port + number - 1))
  local raft_port=$((raft_base_port + number - 1))
  shift
  RUST_LOG="$rust_log" "$binary" \
    --bind "127.0.0.1:${api_port}" \
    --raft-url "up://127.0.0.1:${raft_port}" \
    --data-dir "${test_root}/node-${number}" \
    --username "$username" \
    --password "$password" \
    "$@" >"${test_root}/node-${number}.log" 2>&1 &
  pids+=("$!")
}

wait_for_api() {
  local port="$1"
  local pid="$2"
  local attempts=0
  until curl --fail --silent --user "${username}:${password}" \
    "http://127.0.0.1:${port}/api/v1/targets" >/dev/null; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "Node process for API port ${port} exited" >&2
      return 1
    fi
    attempts=$((attempts + 1))
    if (( attempts >= 100 )); then
      echo "Node API on port ${port} did not become ready" >&2
      return 1
    fi
    sleep 0.1
  done
}

start_node 1 --secret-key "$secret_key"
wait_for_api "$api_base_port" "${pids[0]}"

for number in $(seq 2 "$node_count"); do
  join_link="$(curl --fail --silent --user "${username}:${password}" \
    --header 'content-type: application/json' \
    --data '{"expires_in_seconds":300}' \
    "http://127.0.0.1:${api_base_port}/api/v1/join-links" \
    | jq --raw-output '.url')"
  start_node "$number" \
    --join "$join_link"
done

for number in $(seq 2 "$node_count"); do
  wait_for_api "$((api_base_port + number - 1))" "${pids[number - 1]}"
done

# Exercise multiple heartbeat/read-barrier rounds instead of validating only
# the instant at which membership commits.
sleep "$settle_seconds"

target_count=12
for number in $(seq 1 "$target_count"); do
  curl --fail --silent --user "${username}:${password}" \
    --header 'content-type: application/json' \
    --data "{\"name\":\"Cluster verification ${number}\",\"url\":\"http://127.0.0.1:${api_base_port}/healthz?target=${number}\",\"method\":\"GET\",\"interval_seconds\":60,\"timeout_seconds\":10,\"failure_threshold\":3}" \
    "http://127.0.0.1:$((api_base_port + 1))/api/v1/targets" >/dev/null
done

attempts=0
until response="$(curl --fail --silent --user "${username}:${password}" \
  "http://127.0.0.1:${api_base_port}/api/v1/targets")" \
  && (( $(printf '%s' "$response" | jq '[.[] | select(.name | startswith("Cluster verification ")) | select(.latest_evaluation != null)] | length') == target_count )); do
  attempts=$((attempts + 1))
  if (( attempts >= 100 )); then
    echo "Cluster verification Target was not evaluated" >&2
    exit 1
  fi
  sleep 0.1
done

for offset in $(seq 0 "$((node_count - 1))"); do
  response="$(curl --fail --silent --user "${username}:${password}" \
    "http://127.0.0.1:$((api_base_port + offset))/api/v1/targets")"
  if [[ "$response" != *'"name":"Cluster verification 1"'* ]]; then
    echo "Replicated Target missing from Node $((offset + 1)): ${response}" >&2
    exit 1
  fi
done

executor_count="$(printf '%s' "$response" \
  | jq '[.[] | select(.name | startswith("Cluster verification ")) | .latest_evaluation.executor_node_id] | unique | length')"
if (( node_count > 1 && executor_count < 2 )); then
  echo "Evaluations were not distributed across Nodes: ${response}" >&2
  exit 1
fi

UPGRID_API_URL="http://127.0.0.1:${api_base_port}" \
UPGRID_USERNAME="$username" \
UPGRID_PASSWORD="$password" \
  "${script_dir}/verify-reference-workload.sh"

if (( node_count >= 3 )); then
  kill "${pids[0]}"
  wait "${pids[0]}" 2>/dev/null || true
  sleep 7

  create_response="$(curl --fail --silent --user "${username}:${password}" \
    --header 'content-type: application/json' \
    --data "{\"name\":\"Failover verification\",\"url\":\"http://127.0.0.1:$((api_base_port + 1))/healthz\",\"interval_seconds\":60,\"timeout_seconds\":10,\"failure_threshold\":3}" \
    "http://127.0.0.1:$((api_base_port + 1))/api/v1/targets")"
  failover_target_id="$(printf '%s' "$create_response" | jq --raw-output '.id')"

  attempts=0
  until response="$(curl --fail --silent --user "${username}:${password}" \
    "http://127.0.0.1:$((api_base_port + 1))/api/v1/targets/${failover_target_id}")" \
    && [[ "$response" == *'"latest_evaluation":{'* ]]; do
    attempts=$((attempts + 1))
    if (( attempts >= 150 )); then
      echo "Cluster did not evaluate Failover Target ${failover_target_id}: ${response:-unavailable}" >&2
      exit 1
    fi
    sleep 0.1
  done

  response="$(curl --fail --silent --user "${username}:${password}" \
    "http://127.0.0.1:$((api_base_port + 2))/api/v1/targets")"
  if [[ "$response" != *'"name":"Failover verification"'* ]]; then
    echo "Failover write did not replicate to the remaining follower" >&2
    exit 1
  fi
fi

if rg --quiet 'timeout after 50ms' "${test_root}"/*.log; then
  echo "Detected the regressed 50 ms replication timeout" >&2
  exit 1
fi

if rg --quiet '(scheduler|alert worker) could not establish a read barrier' "${test_root}"/*.log; then
  echo "Detected a worker read-barrier failure after Cluster startup" >&2
  exit 1
fi

echo "Local ${node_count}-Node Cluster verified; logs: ${test_root}"
