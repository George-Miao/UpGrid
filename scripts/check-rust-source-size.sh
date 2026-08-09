#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "$0")/.." && pwd)"
failed=false

while IFS= read -r file; do
  lines="$(wc -l <"$file")"
  if (( lines > 500 )); then
    printf '%s has %s lines; split it below the 500-line limit\n' \
      "${file#"$repository"/}" "$lines" >&2
    failed=true
  fi
done < <(find "$repository/crates" -type f -name '*.rs' -print)

if [[ "$failed" == true ]]; then
  exit 1
fi
