# Deployment notes

Build a release binary with `cargo build --release` and run each node with a dedicated durable data directory. Keep `deployment-key`, `quic-ca-key`, `node-id`, and `raft.sqlite3` on persistent storage; never copy a node data directory to create another node. Legacy Raft files migrate automatically on first startup and remain as rollback backups.

## Configuration

UpGrid combines built-in defaults, an optional TOML file, `UPGRID_` environment variables, and CLI options in that precedence order. Pass the file with `--config /etc/upgrid.toml` or `UPGRID_CONFIG`. CLI options override every other source.

```toml
bind = "127.0.0.1:8080"
local_addresses = ["10.0.0.10"]
raft_port = 11451
reachable_addresses = ["up://node-1.internal:11451"]
discovery_urls = ["https://discovery.internal/upgrid/nodes"]
data_dir = "/var/lib/upgrid"
node_name = "edge-shanghai"
username = "admin"
password = "change-me"
history_retention_hours = 24
```

Keep configuration files containing credentials readable only by the UpGrid service account.

For initial bootstrap or recovery, supply 32-byte Base64 keys with `deployment_key` / `UPGRID_DEPLOYMENT_KEY` / `--deployment-key` and `quic_ca_key` / `UPGRID_QUIC_CA_KEY` / `--quic-ca-key`. The QUIC certificate-authority key defaults to a value derived from the deployment key. UpGrid persists both keys in the data directory and includes them in protected join links.

## API TLS and reverse proxies

The API serves plaintext HTTP by default so a reverse proxy can terminate TLS. Preserve streaming and disable buffering for `/api/v1/events`.

Example Caddy configuration:

```caddyfile
upgrid.example.com {
    reverse_proxy 127.0.0.1:8080 {
        flush_interval -1
    }
}
```

Example Nginx location:

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
    proxy_read_timeout 1h;
}
```

Native HTTPS is optional. Configure both `tls_cert` and `tls_key` in TOML, `UPGRID_TLS_CERT` and `UPGRID_TLS_KEY`, or `--tls-cert` and `--tls-key`. Both files must be PEM encoded; UpGrid refuses a partial pair.

```toml
tls_cert = "/etc/upgrid/api-chain.pem"
tls_key = "/etc/upgrid/api-key.pem"
```

Do not expose basic authentication over an untrusted plaintext connection. Restrict inter-node UDP ports to cluster members and supply the same API credentials to every node. Set `UPGRID_HISTORY_RETENTION_HOURS` consistently when changing the replicated raw-history window.

## Node admission

An authenticated operator creates an expiring join token from the cluster page's **Create token** action or `POST /api/v1/join-tokens`. The WebUI defaults to one day and one use. The API defaults to one day when `expires_in_seconds` is omitted; omit `max_uses` for unlimited use or set it to a positive integer to bound admissions. The response is an opaque bearer URL used directly as `upgrid --join 'up://…'`. Revoke it from the cluster page or with `DELETE /api/v1/join-tokens/{id}`.

With a fresh data directory and no lifecycle option, UpGrid opens an authenticated browser OOBE at `/setup`. Review the generated friendly node name, then create a new cluster or paste a join token. Open **network settings** to add reachable addresses and HTTP discovery service URLs. Discovery service URLs entered in the WebUI persist in the node's data directory. Notification channel and target steps follow after membership and may be skipped. The pre-membership listener exposes no replicated resource endpoints.

For unattended provisioning, use `--new-cluster` or `UPGRID_NEW_CLUSTER=true` for the first node, and `--join 'up://…'` or `UPGRID_JOIN` for subsequent nodes. These choices are mutually exclusive. Existing durable membership always wins on restart: `--new-cluster` and matching join tokens are harmlessly ignored; a join token pointing outside the stored membership is ignored and reported as a dismissible WebUI warning.

Join tokens transport long-lived deployment material. Keep them confidential, never place them in logs or tickets, and revoke reusable tokens as soon as provisioning is complete. During incomplete admission, UpGrid keeps the pending join link in a private file and resumes the same reservation after restart without consuming another use. It removes the file only after membership and the next OOBE phase are durable. Normal restarts use the persisted data directory and need no join link.

## Operational checks

Run `scripts/verify-local-cluster.sh` before release changes involving Raft, scheduling, or transport. It verifies follower writes, distributed execution, and continued operation after the initial leader exits. Against a disposable running deployment, `scripts/verify-reference-workload.sh` creates the 1,000-target reference workload and requires at least 99% to finish within one interval.
