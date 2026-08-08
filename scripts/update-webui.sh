#!/usr/bin/env bash
set -euo pipefail

workspace=$(cd "$(dirname "$0")/.." && pwd)
pnpm --dir "$workspace/frontend" install --frozen-lockfile
pnpm --dir "$workspace/frontend" build
