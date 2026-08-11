#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
api_base_port="${UPGRID_TEST_API_BASE_PORT:-19080}"
raft_base_port="${UPGRID_TEST_RAFT_BASE_PORT:-19451}"
settle_seconds="${UPGRID_TEST_SETTLE_SECONDS:-4}"
node_count="${UPGRID_TEST_NODE_COUNT:-3}"
admission_only="${UPGRID_TEST_ADMISSION_ONLY:-false}"
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
    --node-name "test-node-${number}" \
    "$@" >"${test_root}/node-${number}.log" 2>&1 &
  pids+=("$!")
}

wait_for_api() {
  local port="$1"
  local pid="$2"
  local attempts=0
  until curl --fail --silent --max-time 6 \
    "http://127.0.0.1:${port}/healthz" >/dev/null; do
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

start_node 1 --new-cluster --secret-key "$secret_key" --username "$username" --password "$password"
wait_for_api "$api_base_port" "${pids[0]}"
curl --fail --silent \
  --cookie-jar "${test_root}/session.cookies" \
  --header 'content-type: application/json' \
  --data "{\"username\":\"${username}\",\"password\":\"${password}\"}" \
  "http://127.0.0.1:${api_base_port}/api/v1/auth/login" >/dev/null
created_token="$(curl --fail --silent \
  --cookie "${test_root}/session.cookies" \
  --header 'content-type: application/json' \
  --data '{"name":"Local Cluster verifier"}' \
  "http://127.0.0.1:${api_base_port}/api/v1/api-tokens")"
api_token="$(printf '%s' "$created_token" | jq --raw-output '.value')"


join_token="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
  --header 'content-type: application/json' \
  --data '{"expires_in_seconds":300}' \
  "http://127.0.0.1:${api_base_port}/api/v1/join-tokens")"
join_link="$(printf '%s' "$join_token" | jq --raw-output '.url')"
join_token_id="$(printf '%s' "$join_token" | jq --raw-output '.id')"

for number in $(seq 2 "$node_count"); do
  start_node "$number" \
    --join "$join_link"
done

for number in $(seq 2 "$node_count"); do
  wait_for_api "$((api_base_port + number - 1))" "${pids[number - 1]}"
done

cluster="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
  "http://127.0.0.1:${api_base_port}/api/v1/cluster")"
if (( $(printf '%s' "$cluster" | jq '[.members[].name | select(startswith("test-node-"))] | length') != node_count )); then
  echo "Configured Node names are missing from Cluster topology: ${cluster}" >&2
  exit 1
fi

curl --fail --silent --header "authorization: Bearer ${api_token}" \
  --request DELETE \
  "http://127.0.0.1:${api_base_port}/api/v1/join-tokens/${join_token_id}" >/dev/null

revoked_number=$((node_count + 1))
start_node "$revoked_number" --join "$join_link"
revoked_pid="${pids[revoked_number - 1]}"
rejected=false
for _ in $(seq 1 100); do
  if ! kill -0 "$revoked_pid" 2>/dev/null; then
    rejected=true
    break
  fi
  sleep 0.1
done
if [[ "$rejected" != true ]]; then
  echo "Revoked Join Token still admitted a Node" >&2
  exit 1
fi
if ! rg --quiet 'invalid, expired, or revoked' "${test_root}/node-${revoked_number}.log"; then
  echo "Revoked Join Token failed without the expected rejection" >&2
  cat "${test_root}/node-${revoked_number}.log" >&2
  exit 1
fi

if (( node_count >= 3 )); then
  kill "${pids[2]}"
  wait "${pids[2]}" 2>/dev/null || true
  start_node 3 --join "$join_link"
  matching_restart_pid="${pids[${#pids[@]} - 1]}"
  wait_for_api "$((api_base_port + 2))" "$matching_restart_pid"
  setup_state="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
    "http://127.0.0.1:$((api_base_port + 2))/api/v1/setup")"
  if [[ "$(printf '%s' "$setup_state" | jq --raw-output '.warning')" != "null" ]]; then
    echo "Matching persisted Join Token unexpectedly produced a warning: ${setup_state}" >&2
    exit 1
  fi

  kill "$matching_restart_pid"
  wait "$matching_restart_pid" 2>/dev/null || true
  start_node 3 --join "not-a-valid-join-token"
  existing_restart_pid="${pids[${#pids[@]} - 1]}"
  wait_for_api "$((api_base_port + 2))" "$existing_restart_pid"
  setup_state="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
    "http://127.0.0.1:$((api_base_port + 2))/api/v1/setup")"
  if [[ "$(printf '%s' "$setup_state" | jq --raw-output '.warning')" != *"invalid"* ]]; then
    echo "Invalid persisted Join Token did not produce a WebUI warning: ${setup_state}" >&2
    exit 1
  fi
fi

if [[ "$admission_only" == true ]]; then
  echo "Local ${node_count}-Node admission and restart behavior verified"
  exit 0
fi

# Exercise multiple heartbeat/read-barrier rounds instead of validating only
# the instant at which membership commits.
sleep "$settle_seconds"

target_count=12
for number in $(seq 1 "$target_count"); do
  curl --fail --silent --header "authorization: Bearer ${api_token}" \
    --header 'content-type: application/json' \
    --data "{\"name\":\"Cluster verification ${number}\",\"url\":\"http://127.0.0.1:${api_base_port}/healthz?target=${number}\",\"method\":\"GET\",\"interval_seconds\":60,\"timeout_seconds\":10,\"failure_threshold\":3}" \
    "http://127.0.0.1:$((api_base_port + 1))/api/v1/targets" >/dev/null
done

attempts=0
until response="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
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
  response="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
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
UPGRID_API_TOKEN="$api_token" \
  "${script_dir}/verify-reference-workload.sh"

if (( node_count >= 3 )); then
  kill "${pids[0]}"
  wait "${pids[0]}" 2>/dev/null || true
  sleep 7

  create_response="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
    --header 'content-type: application/json' \
    --data "{\"name\":\"Failover verification\",\"url\":\"http://127.0.0.1:$((api_base_port + 1))/healthz\",\"interval_seconds\":60,\"timeout_seconds\":10,\"failure_threshold\":3}" \
    "http://127.0.0.1:$((api_base_port + 1))/api/v1/targets")"
  failover_target_id="$(printf '%s' "$create_response" | jq --raw-output '.id')"

  attempts=0
  until response="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
    "http://127.0.0.1:$((api_base_port + 1))/api/v1/targets/${failover_target_id}")" \
    && [[ "$response" == *'"latest_evaluation":{'* ]]; do
    attempts=$((attempts + 1))
    if (( attempts >= 150 )); then
      echo "Cluster did not evaluate Failover Target ${failover_target_id}: ${response:-unavailable}" >&2
      exit 1
    fi
    sleep 0.1
  done

  response="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
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
