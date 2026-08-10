# UpGrid

UpGrid is a self-contained, Raft-backed uptime monitor. Every Node exposes the same Basic-authenticated API and embedded WebUI. Writes received by followers are forwarded to the leader; reads are served from local replicated state after a linearizable read barrier. The leader assigns HTTP/HTTPS evaluations across voters and delivers Telegram or webhook alerts.

## Start one Node

UpGrid currently requires the pinned nightly Rust toolchain used by the repository.

```sh
cargo run -- \
  --bind 127.0.0.1:8080 \
  --raft-url up://127.0.0.1:11451 \
  --data-dir upgrid-data \
  --username admin \
  --password change-me
```

Open [http://127.0.0.1:8080/](http://127.0.0.1:8080/) and enter the configured username and password. Data survives restarts in `upgrid-data/`. The generated API description is available at `/openapi.json`; `/healthz` does not require authentication.

TLS is intentionally not forced on the HTTP API so a reverse proxy can terminate it. Do not expose Basic authentication over an untrusted plaintext connection.
See [deployment notes](docs/DEPLOYMENT.md) for Caddy and Nginx examples.

## Start a three-Node Cluster

Start the first Node as above, then create one short-lived link for each joining Node. The WebUI's **Add node** button creates and copies the same command.

```sh
export JOIN_LINK_2="$(curl -fsS -u admin:change-me \
  -H 'content-type: application/json' -d '{"expires_in_seconds":600}' \
  http://127.0.0.1:8080/api/v1/join-links | jq -r .url)"
export JOIN_LINK_3="$(curl -fsS -u admin:change-me \
  -H 'content-type: application/json' -d '{"expires_in_seconds":600}' \
  http://127.0.0.1:8080/api/v1/join-links | jq -r .url)"
```

In two more terminals, use unique API addresses, Raft URLs, and data directories while keeping the same API credentials:

```sh
cargo run -- --bind 127.0.0.1:8081 --raft-url up://127.0.0.1:11452 \
  --join "$JOIN_LINK_2" --data-dir upgrid-data-2 \
  --username admin --password change-me

cargo run -- --bind 127.0.0.1:8082 --raft-url up://127.0.0.1:11453 \
  --join "$JOIN_LINK_3" --data-dir upgrid-data-3 \
  --username admin --password change-me
```

The opaque `up://` link carries both a single-use admission token and the deployment material needed for mutual TLS and replicated Secret decryption. Treat it as a password, never log or archive it, and create a separate link for every Node. The `up://` hostname advertised by each Node must be reachable by every other Node. The joining process stores the deployment key with private permissions before connecting; the leader atomically consumes the token through Raft. On restart, a Node resumes membership from its data directory and no longer needs `--join`.

## Verify

```sh
cargo fmt --all -- --check
cargo test --no-run
cargo test domain::tests
cargo test scheduler::tests
cargo test published_openapi_matches_routes
pnpm --dir frontend install --frozen-lockfile
pnpm --dir frontend build
scripts/test-webui.sh
scripts/verify-local-cluster.sh
```

The browser test starts an isolated Node and exercises the embedded UI in Chromium. The final command starts three local Nodes, writes through a follower, and confirms the Target is readable from every Node. The legacy fixed-port `master` and `worker` network tests are manual fixtures; do not run the unfiltered suite in automation.

## WebUI development

The Lit/TypeScript source is in `frontend/src/`. Run `scripts/update-webui.sh` after changing it; Vite rebuilds the checked-in `frontend/dist/` files embedded by the Rust binary. CI rebuilds the same artifact and rejects stale generated output. Install Chromium once with `pnpm --dir frontend exec playwright install chromium` before running browser tests locally.

With an UpGrid Node running on port 8080, start the Vite development server with hot reload:

```sh
pnpm --dir frontend dev
```

Set `UPGRID_API_URL`, `UPGRID_USERNAME`, and `UPGRID_PASSWORD` when the API uses a different address or credentials; Vite proxies `/api` to that Node.

Run `scripts/update-openapi.sh` after changing API routes or schemas. CI compares the generated contract with `docs/openapi.json`. A running reference deployment can exercise the 1,000-Target SLO with `scripts/verify-reference-workload.sh`.
