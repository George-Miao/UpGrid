# Deployment Notes

Build a release binary with `cargo build --release` and run each Node with a dedicated durable data directory. Keep `deployment-key`, `node-id`, `raft-log.redb`, and `raft-state.postcard` on persistent storage; never copy a Node data directory to create another Node. Existing `raft-log.postcard` files migrate automatically on first startup with the new format and remain as rollback backups.

## Reverse Proxy

UpGrid intentionally serves plaintext HTTP so an operator can terminate TLS at a reverse proxy. Preserve streaming and disable buffering for `/api/v1/events`.

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

Do not expose the UpGrid listener directly to an untrusted network: HTTP Basic credentials are plaintext without proxy TLS. Restrict inter-Node UDP ports to Cluster members. Supply the same `UPGRID_USERNAME`, `UPGRID_PASSWORD`, and `UPGRID_SECRET_KEY` to every Node; protect the key as a secret and back it up separately from any one Node. Issue a fresh `/api/v1/join-tokens` token for each new Node. Set `UPGRID_HISTORY_RETENTION_HOURS` consistently when changing the replicated raw-history window.

## Operational Checks

Run `scripts/verify-local-cluster.sh` before release changes involving Raft, scheduling, or transport. It verifies follower writes, distributed execution, and continued operation after the initial leader exits. Against a disposable running deployment, `scripts/verify-reference-workload.sh` creates the 1,000-Target reference workload and requires at least 99% to finish within one interval.
