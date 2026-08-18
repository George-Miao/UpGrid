# UpGrid

Distributed uptime monitoring that stays available with your infrastructure.

[Website](https://upgrid.rs) · [Get started](https://upgrid.rs/getting-started/first-node/) · [HTTP API](https://upgrid.rs/reference/api/)

![UpGrid WebUI in bright and dark themes](website/src/assets/webui-showcase.png)

UpGrid is a self-contained, Raft-backed service monitor. Run one binary for a capable uptime dashboard, or join several nodes into a resilient cluster without adding a separate database, queue, or control plane.

Every node exposes the same authenticated API and responsive WebUI. Followers transparently forward changes to the leader, reads come from replicated state after a linearizable barrier, and polling work is distributed across voting members.

## Why UpGrid

- **One binary, no external database.** The API, WebUI, scheduler, probe workers, notifications, and durable Raft state travel together.
- **A useful interface on every node.** Manage targets with recoverable deletion, inspect raw and hourly rollup latency and availability history, export bounded history pages, and operate the cluster from any healthy member.
- **Distributed monitoring.** The leader assigns each polling interval across one or more distinct voting nodes, commits one authoritative aggregate result, and preserves failure streaks across leader changes.
- **Flexible service checks.** Monitor HTTP requests with private-CA and mutual-TLS credentials plus ordered regex, JSONPath, header, latency, and sandboxed script assertions; also probe TCP connections, DNS resolution, ICMP echo, and TLS certificates.
- **Actionable notifications.** Deliver service and cluster node transitions through Telegram, SMTP email, or webhooks, with default channels and at-least-once delivery.
- **Simple expansion.** Create an expiring, revocable `up://` join token in the WebUI and start another node with `--join`.

## Quick start

Docker is the preferred installation method. Stable releases use their Git tag and update `latest`. Every `main` commit uses `main-<6-character-commit>` and updates `latest-unstable`. Start a single node with persistent storage using the latest stable release:

```sh
curl --fail --remote-name https://upgrid.rs/upgrid-seccomp.json
docker run --name upgrid \
  --security-opt seccomp=./upgrid-seccomp.json \
  --publish 8080:8080 \
  --volume upgrid-data:/var/lib/upgrid \
  ghcr.io/george-miao/upgrid:latest
```

UpGrid uses `io_uring` on Linux. Docker requires the provided seccomp profile because its default profile blocks the required calls. See the [Docker reference](https://upgrid.rs/reference/docker/#allow-io_uring-in-docker) for details and the less secure fallback.

The project root also includes a single-node [`docker-compose.yaml`](docker-compose.yaml) and the custom [`upgrid-seccomp.json`](upgrid-seccomp.json) profile. Run the configuration from the project root:

```sh
docker compose pull
docker compose up -d
```

Open [http://127.0.0.1:8080/setup](http://127.0.0.1:8080/setup), review the generated node name, and choose **Create new cluster**. Setup creates the first replicated operator identity before it can add a notification channel or service target.

Use the [multi-node setup](https://upgrid.rs/getting-started/multi-node/) before you expand this deployment. It adds the advertised `up://` endpoint and UDP port.

See the [environment-variable reference](https://upgrid.rs/reference/configuration/#settings) for every container setting and its default.

### Precompiled Linux binaries

When a container runtime is not appropriate, use a published Linux binary from the [v0.1.0 release](https://github.com/George-Miao/UpGrid/releases/tag/v0.1.0):

- [Linux AMD64](https://github.com/George-Miao/UpGrid/releases/download/v0.1.0/upgrid-v0.1.0-linux-amd64.tar.gz)
- [Linux ARM64](https://github.com/George-Miao/UpGrid/releases/download/v0.1.0/upgrid-v0.1.0-linux-arm64.tar.gz)
- [SHA-256 checksums](https://github.com/George-Miao/UpGrid/releases/download/v0.1.0/SHA256SUMS)

Download the archive for the host architecture, verify it against `SHA256SUMS`, then extract and run `upgrid`.

### Build from source

Source builds are intended for contributors and unsupported targets. The repository provides a reproducible Nix development shell with the required Rust, Node.js, and native tooling.

```sh
git clone https://github.com/George-Miao/UpGrid.git
cd UpGrid
nix develop
cargo run -p upgrid
```

State is stored in `upgrid-data/` by default. Use a durable, unique data directory for each node.

## Grow into a cluster

Open **Cluster**, choose **Create token**, and configure its expiration and usage limit. Then start a fresh node with the generated token:

```sh
upgrid \
  --join 'up://existing-node.example/opaque-token' \
  --bind 127.0.0.1:8081 \
  --raft-url up://node-2.internal:11451 \
  --data-dir upgrid-data-2 \
```

The join token contains admission authority and deployment material. Treat it as a password, transfer it through a trusted channel, and revoke reusable tokens after provisioning. Every advertised `up://` address must be reachable by every cluster member. Joined nodes receive the cluster's operator identities and API tokens through Raft; they do not use per-node credentials.

Drain a healthy node before removing it from membership. For a failed node, fence the old process, force-remove it from another healthy member, and join its replacement with a fresh data directory and one-use token.

See [join a cluster](https://upgrid.rs/guides/join-cluster/) for browser and unattended workflows.

## How it works

1. A request reaches any node. Mutations are forwarded to the Raft leader; consistent reads use the local replicated state.
2. The leader assigns due evaluations across voting nodes. The first accepted result for an interval becomes authoritative.
3. Replicated policy turns results into **Up**, **Suspicious**, **Down**, or **Paused** state.
4. Availability transitions enter a replicated outbox and are delivered to the target's notification channels.

The alerts page records delivery state per channel. Operators can filter the history, acknowledge records, and retry failed deliveries; these actions are replicated with the rest of cluster state.

The HTTP API can run behind a TLS-terminating reverse proxy or serve HTTPS directly. `/healthz` is public; the WebUI uses an HTTP-only session cookie, and automation can use a revocable bearer API token. Never send credentials over untrusted plaintext transport.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --no-run
scripts/test-webui.sh
scripts/verify-local-cluster.sh
```

The Lit frontend lives in `frontend/`; the Starlight documentation site lives in `website/`. Run `scripts/update-webui.sh` after frontend changes and `scripts/update-openapi.sh` after API route or schema changes.

## Project status

The self-contained MVP is complete and development now follows small, agile iterations. UpGrid is suitable for evaluation and self-hosted experimentation; authentication and cluster membership operations are still being hardened. Current follow-up work is tracked in [TODO.md](TODO.md).

## Acknowledgements

Proudly powered by [Compio](https://compio.rs/) and [OpenRaft](https://github.com/databendlabs/openraft).
