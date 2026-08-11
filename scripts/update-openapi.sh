#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repository"
cargo run --quiet -- print-openapi >docs/openapi.json
