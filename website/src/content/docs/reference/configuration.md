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
raft_url = "up://node-1.internal:11451"
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
| `raft_url` | `UPGRID_RAFT_URL` | `--raft-url` | `up://127.0.0.1:11451` |
| `data_dir` | `UPGRID_DATA_DIR` | `--data-dir` | `upgrid-data` |
| `node_name` | `UPGRID_NODE_NAME` | `--node-name` | Generated friendly name |
| `username` | `UPGRID_USERNAME` | `--username` | `admin` |
| `password` | `UPGRID_PASSWORD` | `--password` | Unset |
| `new_cluster` | `UPGRID_NEW_CLUSTER` | `--new-cluster` | `false` |
| `join` | `UPGRID_JOIN` | `--join` | Unset |
| `history_retention_hours` | `UPGRID_HISTORY_RETENTION_HOURS` | `--history-retention-hours` | 24 hours for a new cluster |
| `history_rollup_retention_days` | `UPGRID_HISTORY_ROLLUP_RETENTION_DAYS` | `--history-rollup-retention-days` | 365 days for a new cluster |
| `target_trash_retention_days` | `UPGRID_TARGET_TRASH_RETENTION_DAYS` | `--target-trash-retention-days` | 30 days for a new cluster |
| `tls_cert` | `UPGRID_TLS_CERT` | `--tls-cert` | Unset |
| `tls_key` | `UPGRID_TLS_KEY` | `--tls-key` | Unset |

`new_cluster` and `join` are mutually exclusive. `tls_cert` and `tls_key` must always be configured together. `username` and `password` create the first operator identity during unattended `new_cluster` setup. They are also used once to migrate a pre-authentication cluster whose replicated state contains no identities; remove credential-bearing configuration after migration.

History and target trash retention settings are replicated cluster-wide. Supplying one on any node startup updates the shared value through the leader; otherwise an existing cluster keeps its current value.

## Deployment key

`secret_key` / `UPGRID_SECRET_KEY` / `--secret-key` supplies bootstrap or recovery deployment material. Normal nodes persist this material during cluster creation or admission. Do not routinely override it, expose it, or use one node's data directory to create another node.
