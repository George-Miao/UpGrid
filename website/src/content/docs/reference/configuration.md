---
title: Configuration
description: Configure UpGrid with defaults, TOML, environment variables, and CLI flags.
---

UpGrid merges configuration in this precedence order, from lowest to highest:

1. Built-in defaults;
2. A TOML file selected by `--config` or `UPGRID_CONFIG`;
3. `UPGRID_` environment variables; and
4. CLI flags.

## Example TOML

```toml title="/etc/upgrid.toml"
bind = "127.0.0.1:8080"
local_addresses = ["10.0.0.10"]
raft_port = 11451
reachable_addresses = ["up://node-1.internal:11451"]
discovery_urls = ["https://discovery.internal/upgrid/nodes"]
data_dir = "/var/lib/upgrid"
node_name = "edge-shanghai"
history_retention_hours = 24
history_rollup_retention_days = 365
target_trash_retention_days = 30
tls_cert = "/etc/upgrid/api-chain.pem"
tls_key = "/etc/upgrid/api-key.pem"
```

Start with `upgrid --config /etc/upgrid.toml` or set `UPGRID_CONFIG=/etc/upgrid.toml`.

## Settings

| TOML key | Environment | CLI | Default |
| --- | --- | --- | --- |
| `bind` | `UPGRID_BIND` | `--bind` | `127.0.0.1:8080` |
| `local_addresses` | `UPGRID_LOCAL_ADDRESSES` | `--local-address` | `["127.0.0.1"]` |
| `raft_port` | `UPGRID_RAFT_PORT` | `--raft-port` | `11451` |
| `reachable_addresses` | `UPGRID_REACHABLE_ADDRESSES` | `--reachable-address` | Unset |
| `discovery_urls` | `UPGRID_DISCOVERY_URLS` | `--discovery-url` | Unset |
| `data_dir` | `UPGRID_DATA_DIR` | `--data-dir` | `upgrid-data` |
| `node_name` | `UPGRID_NODE_NAME` | `--node-name` | Generated friendly name |
| `username` | `UPGRID_USERNAME` | `--username` | `admin` |
| `password` | `UPGRID_PASSWORD` | `--password` | Unset |
| `new_cluster` | `UPGRID_NEW_CLUSTER` | `--new-cluster` | `false` |
| `join` | `UPGRID_JOIN` | `--join` | Unset |
| `deployment_key` | `UPGRID_DEPLOYMENT_KEY` | `--deployment-key` | Unset |
| `quic_ca_key` | `UPGRID_QUIC_CA_KEY` | `--quic-ca-key` | Derived from the deployment key |
| `history_retention_hours` | `UPGRID_HISTORY_RETENTION_HOURS` | `--history-retention-hours` | 24 hours for a new cluster |
| `history_rollup_retention_days` | `UPGRID_HISTORY_ROLLUP_RETENTION_DAYS` | `--history-rollup-retention-days` | 365 days for a new cluster |
| `target_trash_retention_days` | `UPGRID_TARGET_TRASH_RETENTION_DAYS` | `--target-trash-retention-days` | 30 days for a new cluster |
| `tls_cert` | `UPGRID_TLS_CERT` | `--tls-cert` | Unset |
| `tls_key` | `UPGRID_TLS_KEY` | `--tls-key` | Unset |

Collection values use arrays in TOML and environment variables. For example, set `UPGRID_LOCAL_ADDRESSES='["0.0.0.0"]'` and `UPGRID_REACHABLE_ADDRESSES='["up://node-1.internal:11451"]'`. Repeat the matching CLI option to add more than one value.

`local_addresses` controls the local UDP interfaces for cluster traffic. `raft_port` is shared by all local addresses. `reachable_addresses` lists addresses that other nodes can use. Browser setup and explicit startup configuration persist reachable addresses in the node's data directory. A later explicit startup value replaces the stored set. Up to eight services in `discovery_urls` can add reachable address candidates at runtime. These candidates use renewable reachability leases. Each service must return JSON in the form `{"addresses":["up://node-1.internal:11451"]}`.

`new_cluster` and `join` are mutually exclusive. `tls_cert` and `tls_key` must always be configured together. `username` and `password` create the first operator identity during unattended `new_cluster` setup. They are also used once to migrate a pre-authentication cluster whose replicated state contains no identities; remove credential-bearing configuration after migration.

History and target trash retention settings are replicated cluster-wide. Supplying one on any node startup updates the shared value through the leader; otherwise an existing cluster keeps its current value.

## Deployment and QUIC certificate-authority keys

`deployment_key` / `UPGRID_DEPLOYMENT_KEY` / `--deployment-key` supplies the 32-byte Base64 key that encrypts cluster secrets. `quic_ca_key` / `UPGRID_QUIC_CA_KEY` / `--quic-ca-key` supplies the 32-byte Base64 Ed25519 seed for the internal QUIC certificate authority. When `quic_ca_key` is unset, UpGrid derives it from the deployment key for compatibility.

Normal nodes persist both keys during cluster creation or admission. Direct values are for initial bootstrap or recovery. Do not routinely override or expose them, and do not use one node's data directory to create another node.
