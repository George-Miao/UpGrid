---
title: Configuration
description: Configure UpGrid with defaults, TOML, environment variables, and CLI flags.
---

UpGrid merges configuration in this precedence order, from lowest to highest:

1. built-in defaults;
2. a TOML file selected by `--config` or `UPGRID_CONFIG`;
3. `UPGRID_` environment variables; and
4. CLI flags.

## Example TOML

```toml title="/etc/upgrid.toml"
bind = "127.0.0.1:8080"
raft_url = "up://node-1.internal:11451"
data_dir = "/var/lib/upgrid"
node_name = "edge-shanghai"
username = "admin"
password = "replace-this-password"
history_retention_hours = 24
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
| `node_name` | `UPGRID_NODE_NAME` | `--node-name` | generated friendly name |
| `username` | `UPGRID_USERNAME` | `--username` | `admin` |
| `password` | `UPGRID_PASSWORD` | `--password` | `upgrid` |
| `new_cluster` | `UPGRID_NEW_CLUSTER` | `--new-cluster` | `false` |
| `join` | `UPGRID_JOIN` | `--join` | unset |
| `history_retention_hours` | `UPGRID_HISTORY_RETENTION_HOURS` | `--history-retention-hours` | unset |
| `tls_cert` | `UPGRID_TLS_CERT` | `--tls-cert` | unset |
| `tls_key` | `UPGRID_TLS_KEY` | `--tls-key` | unset |

`new_cluster` and `join` are mutually exclusive. `tls_cert` and `tls_key` must always be configured together.

:::danger
The built-in credentials are development conveniences, not safe production credentials. Set a unique password and restrict any credential-bearing configuration file to the service account.
:::

## Deployment key

`secret_key` / `UPGRID_SECRET_KEY` / `--secret-key` supplies bootstrap or recovery deployment material. Normal Nodes persist this material during Cluster creation or admission. Do not routinely override it, expose it, or use one Node's data directory to create another Node.
