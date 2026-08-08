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

Open <http://127.0.0.1:8080/> and enter the configured username and password. Data survives restarts in `upgrid-data/`. The generated API description is available at `/openapi.json`; `/healthz` does not require authentication.

TLS is intentionally not forced on the HTTP API so a reverse proxy can terminate it. Do not expose Basic authentication over an untrusted plaintext connection.
See [deployment notes](docs/DEPLOYMENT.md) for Caddy and Nginx examples.

## Start a three-Node Cluster

Start the first Node as above. Export its generated deployment key and issue one short-lived token for each joining Node:

```sh
export UPGRID_SECRET_KEY="$(cat upgrid-data/deployment-key)"
export JOIN_TOKEN_2="$(curl -fsS -u admin:change-me \
  -H 'content-type: application/json' -d '{"expires_in_seconds":600}' \
  http://127.0.0.1:8080/api/v1/join-tokens | jq -r .token)"
export JOIN_TOKEN_3="$(curl -fsS -u admin:change-me \
  -H 'content-type: application/json' -d '{"expires_in_seconds":600}' \
  http://127.0.0.1:8080/api/v1/join-tokens | jq -r .token)"
```

In two more terminals, use unique API addresses, Raft URLs, and data directories while keeping the same API credentials and deployment key:

```sh
cargo run -- --bind 127.0.0.1:8081 --raft-url up://127.0.0.1:11452 \
  --join up://127.0.0.1:11451 --data-dir upgrid-data-2 \
  --join-token "$JOIN_TOKEN_2" --username admin --password change-me \
  --secret-key "$UPGRID_SECRET_KEY"

cargo run -- --bind 127.0.0.1:8082 --raft-url up://127.0.0.1:11453 \
  --join up://127.0.0.1:11451 --data-dir upgrid-data-3 \
  --join-token "$JOIN_TOKEN_3" --username admin --password change-me \
  --secret-key "$UPGRID_SECRET_KEY"
```

The deployment key derives the pinned inter-Node CA and encrypts replicated Secrets, so protect it and supply the same value to each Node. Each Join Token is single-use and expires at the requested time. The `up://` hostname must match the advertised address and be reachable by every Node. Any Node's WebUI or API can then be used. On restart, a Node resumes membership from its data directory and safely ignores `--join`. Stop one Node to verify that the remaining quorum elects a leader and continues serving the Cluster.

## Verify

```sh
cargo fmt --all -- --check
cargo test --no-run
cargo test domain::tests
cargo test scheduler::tests
cargo test published_openapi_matches_routes
scripts/verify-local-cluster.sh
```

The final command starts three local Nodes, writes through a follower, and confirms the Target is readable from every Node. The legacy fixed-port `master` and `worker` network tests are manual fixtures; do not run the unfiltered suite in automation.

Run `scripts/update-openapi.sh` after changing API routes or schemas. CI compares the generated contract with `docs/openapi.json`. A running reference deployment can exercise the 1,000-Target SLO with `scripts/verify-reference-workload.sh`.

## Release Direction

The immediate release is a runnable MVP: HTTP Targets with configurable methods, intervals, timeouts, failure thresholds and success criteria; Raft persistence and forwarding; SSE updates; Telegram/webhook channels; and the embedded WebUI. After the MVP, work proceeds in short agile iterations driven by operator feedback. See [the roadmap](docs/ROADMAP.md) for hardening and candidate features.
