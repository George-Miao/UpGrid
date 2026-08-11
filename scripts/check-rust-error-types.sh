#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "$0")/.." && pwd)"

if matches="$(rg -n \
  'dyn[[:space:]]+((std|core)::)?error::Error|dyn[[:space:]]+Error([[:space:]]|[+>])' \
  "$repository/crates" --glob '*.rs')"; then
  printf '%s\n' "$matches" >&2
  printf 'use a typed SNAFU error instead of an erased error trait object\n' >&2
  exit 1
fi

if matches="$(rg -n -U \
  'Result[[:space:]]*<[^;{}]{0,500},[[:space:]]*(std::string::)?String[[:space:]]*>|type[[:space:]]+[A-Za-z0-9_]*Error[[:space:]]*=[[:space:]]*(std::string::)?String' \
  "$repository/crates" --glob '*.rs')"; then
  printf '%s\n' "$matches" >&2
  printf 'use a typed SNAFU error instead of String as an error type\n' >&2
  exit 1
fi
