#!/usr/bin/env bash
set -euo pipefail

api_url="${UPGRID_API_URL:-http://127.0.0.1:8080}"
target_url="${UPGRID_LOAD_TARGET_URL:-${api_url}/healthz}"
username="${UPGRID_USERNAME:-admin}"
password="${UPGRID_PASSWORD:-upgrid}"
target_count="${UPGRID_LOAD_TARGET_COUNT:-1000}"
interval_seconds="${UPGRID_LOAD_INTERVAL_SECONDS:-60}"
required=$((target_count * 99 / 100))
run_id="$(date +%s)-$$"

for number in $(seq 1 "$target_count"); do
  curl --fail --silent --user "${username}:${password}" \
    --header 'content-type: application/json' \
    --data "{\"name\":\"Reference workload ${run_id} ${number}\",\"url\":\"${target_url}?run=${run_id}&target=${number}\",\"interval_seconds\":${interval_seconds},\"timeout_seconds\":10,\"failure_threshold\":3}" \
    "${api_url}/api/v1/targets" >/dev/null
done

deadline=$((SECONDS + interval_seconds))
while (( SECONDS < deadline )); do
  response="$(curl --fail --silent --user "${username}:${password}" \
    "${api_url}/api/v1/targets")"
  completed="$(printf '%s' "$response" | jq --arg prefix "Reference workload ${run_id} " \
    '[.[] | select(.name | startswith($prefix)) | select(.latest_evaluation != null)] | length')"
  if (( completed >= required )); then
    echo "Reference workload verified: ${completed}/${target_count} evaluated within ${interval_seconds}s"
    exit 0
  fi
  sleep 1
done

echo "Reference workload missed its SLO: ${completed:-0}/${target_count}, required ${required}" >&2
exit 1
