#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
api_base_port="${UPGRID_TEST_API_BASE_PORT:-19080}"
raft_base_port="${UPGRID_TEST_RAFT_BASE_PORT:-19451}"
settle_seconds="${UPGRID_TEST_SETTLE_SECONDS:-4}"
node_count="${UPGRID_TEST_NODE_COUNT:-3}"
admission_only="${UPGRID_TEST_ADMISSION_ONLY:-false}"
node_lifecycle_only="${UPGRID_TEST_NODE_LIFECYCLE_ONLY:-false}"
rust_log="${UPGRID_TEST_RUST_LOG:-info}"
peer_lease_wait_seconds="${UPGRID_TEST_PEER_LEASE_WAIT_SECONDS:-31}"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/upgrid-cluster.XXXXXX")"
username="cluster-test"
password="cluster-test-password"
deployment_key="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
quic_ca_key="AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="
pids=()
moved_raft_port=""

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
  local raft_port="${UPGRID_TEST_RAFT_PORT_OVERRIDE:-$((raft_base_port + number - 1))}"
  shift
  local reachability=()
  local configured_reachability="${UPGRID_TEST_CONFIGURED_REACHABILITY:-true}"
  if (( number > 1 )); then
    configured_reachability="${UPGRID_TEST_JOINER_CONFIGURED_REACHABILITY:-$configured_reachability}"
  fi
  if [[ "$configured_reachability" == true ]]; then
    reachability=(--reachable-address "up://127.0.0.1:${raft_port}")
  fi
  RUST_LOG="$rust_log" "$binary" \
    --bind "127.0.0.1:${api_port}" \
    --raft-port "${raft_port}" \
    "${reachability[@]}" \
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

wait_for_setup() {
  local port="$1"
  local pid="$2"
  local response
  for _ in $(seq 1 100); do
    if response="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
      "http://127.0.0.1:${port}/api/v1/setup")"; then
      printf '%s' "$response"
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    sleep 0.1
  done
  return 1
}

start_node 1 --new-cluster --deployment-key "$deployment_key" --quic-ca-key "$quic_ca_key" --username "$username" --password "$password"
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
  moved_raft_port=$((raft_base_port + node_count + 10))
  UPGRID_TEST_RAFT_PORT_OVERRIDE="$moved_raft_port" start_node 3 --join "$join_link"
  matching_restart_pid="${pids[${#pids[@]} - 1]}"
  wait_for_api "$((api_base_port + 2))" "$matching_restart_pid"
  setup_state="$(wait_for_setup "$((api_base_port + 2))" "$matching_restart_pid")"
  if [[ "$(printf '%s' "$setup_state" | jq --raw-output '.warning')" != "null" ]]; then
    echo "Matching persisted Join Token unexpectedly produced a warning: ${setup_state}" >&2
    exit 1
  fi

  kill "$matching_restart_pid"
  wait "$matching_restart_pid" 2>/dev/null || true
  UPGRID_TEST_RAFT_PORT_OVERRIDE="$moved_raft_port" start_node 3 --join "not-a-valid-join-token"
  existing_restart_pid="${pids[${#pids[@]} - 1]}"
  wait_for_api "$((api_base_port + 2))" "$existing_restart_pid"
  setup_state="$(wait_for_setup "$((api_base_port + 2))" "$existing_restart_pid")"
  if [[ "$(printf '%s' "$setup_state" | jq --raw-output '.warning')" != *"invalid"* ]]; then
    echo "Invalid persisted Join Token did not produce a WebUI warning: ${setup_state}" >&2
    exit 1
  fi
fi

if (( node_count > 1 )); then
  sleep "$peer_lease_wait_seconds"
  cluster="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
    "http://127.0.0.1:${api_base_port}/api/v1/cluster")"
  if (( $(printf '%s' "$cluster" | jq '[.members[] | select(.reachable_addresses | length == 0)] | length') != 0 )); then
    echo "Peer-discovered reachable address lease was not renewed: ${cluster}" >&2
    exit 1
  fi
  if (( $(printf '%s' "$cluster" | jq '.connectivity_failures | length') != 0 )); then
    echo "Healthy cluster reported connectivity failures: ${cluster}" >&2
    exit 1
  fi
  if [[ -n "$moved_raft_port" ]]; then
    moved_address="up://127.0.0.1:${moved_raft_port}"
    if (( $(printf '%s' "$cluster" | jq --arg address "$moved_address" '[.members[] | select(.name == "test-node-3") | .reachable_addresses[] | select(. == $address)] | length') == 0 )); then
      echo "Changed peer endpoint was not renewed: ${cluster}" >&2
      exit 1
    fi
  fi
fi
if [[ "$node_lifecycle_only" == true ]]; then
  cluster="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
    "http://127.0.0.1:${api_base_port}/api/v1/cluster")"
  removed_id="$(printf '%s' "$cluster" | jq --raw-output \
    '.members[] | select(.name == "test-node-3") | .id')"
  drain="$(curl --fail --silent --request PUT \
    --header "authorization: Bearer ${api_token}" \
    --header 'content-type: application/json' \
    --data '{"draining":true}' \
    "http://127.0.0.1:${api_base_port}/api/v1/nodes/${removed_id}/drain")"
  if [[ "$(printf '%s' "$drain" | jq --raw-output '.draining')" != "true" ]]; then
    echo "Node did not enter draining state: ${drain}" >&2
    exit 1
  fi
  curl --fail --silent --request PUT \
    --header "authorization: Bearer ${api_token}" \
    --header 'content-type: application/json' \
    --data '{"draining":false}' \
    "http://127.0.0.1:${api_base_port}/api/v1/nodes/${removed_id}/drain" >/dev/null

  kill "$existing_restart_pid"
  wait "$existing_restart_pid" 2>/dev/null || true
  removed="$(curl --fail --silent --request DELETE \
    --header "authorization: Bearer ${api_token}" \
    "http://127.0.0.1:${api_base_port}/api/v1/nodes/${removed_id}?force=true")"
  if [[ "$(printf '%s' "$removed" | jq --raw-output '.status')" != "removed" ]] ||
    [[ "$removed" != *"one-use join token"* ]]; then
    echo "Failed-Node removal did not return replacement guidance: ${removed}" >&2
    exit 1
  fi

  replacement_token="$(curl --fail --silent \
    --header "authorization: Bearer ${api_token}" \
    --header 'content-type: application/json' \
    --data '{"expires_in_seconds":300,"max_uses":1}' \
    "http://127.0.0.1:${api_base_port}/api/v1/join-tokens")"
  replacement_link="$(printf '%s' "$replacement_token" | jq --raw-output '.url')"
  start_node 4 --join "$replacement_link"
  replacement_pid="${pids[${#pids[@]} - 1]}"
  wait_for_api "$((api_base_port + 3))" "$replacement_pid"

  attempts=0
  until cluster="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
    "http://127.0.0.1:${api_base_port}/api/v1/cluster")" \
    && (( $(printf '%s' "$cluster" | jq '[.members[] | select(.name == "test-node-4")] | length') == 1 )); do
    attempts=$((attempts + 1))
    if (( attempts >= 100 )); then
      echo "Replacement Node did not join the Cluster: ${cluster:-unavailable}" >&2
      exit 1
    fi
    sleep 0.1
  done
  echo "Local failed-Node drain, removal, and replacement behavior verified"
  exit 0
fi

if [[ "$admission_only" == true ]]; then
  echo "Local ${node_count}-Node admission and restart behavior verified"
  exit 0
fi

# Exercise multiple heartbeat/read-barrier rounds instead of validating only
# the instant at which membership commits.
sleep "$settle_seconds"

multi_location_target="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
  --header 'content-type: application/json' \
  --data "{\"name\":\"Multi-location verification\",\"url\":\"http://127.0.0.1:${api_base_port}/healthz\",\"method\":\"GET\",\"interval_seconds\":60,\"timeout_seconds\":10,\"failure_threshold\":3,\"locations\":${node_count}}" \
  "http://127.0.0.1:$((api_base_port + 1))/api/v1/targets")"
multi_location_target_id="$(printf '%s' "$multi_location_target" | jq --raw-output '.id')"
attempts=0
until response="$(curl --fail --silent --header "authorization: Bearer ${api_token}" \
  "http://127.0.0.1:${api_base_port}/api/v1/targets/${multi_location_target_id}")" \
  && [[ "$(printf '%s' "$response" | jq --raw-output '.locations')" == "$node_count" ]] \
  && [[ "$(printf '%s' "$response" | jq --raw-output '.latest_evaluation.succeeded')" == "true" ]]; do
  attempts=$((attempts + 1))
  if (( attempts >= 150 )); then
    echo "Multi-location Target did not aggregate ${node_count} Node results: ${response:-unavailable}" >&2
    exit 1
  fi
  sleep 0.1
done

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
