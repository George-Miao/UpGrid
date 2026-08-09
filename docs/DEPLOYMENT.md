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

Do not expose the UpGrid listener directly to an untrusted network: HTTP Basic credentials are plaintext without proxy TLS. Restrict inter-Node UDP ports to Cluster members. Supply the same `UPGRID_USERNAME` and `UPGRID_PASSWORD` to every Node. Create a fresh `/api/v1/join-links` invitation for each new Node, transfer it through a trusted channel, and never place it in logs, tickets, or shell history. The invitation provisions the deployment key into the new private data directory; protect and back up that key separately from any one Node. Set `UPGRID_HISTORY_RETENTION_HOURS` consistently when changing the replicated raw-history window.

## Node Admission

An authenticated operator creates an invitation from the WebUI's **Add node** action or `POST /api/v1/join-links`. The response is an opaque bearer URL used directly as `upgrid --join 'up://…'`. Each link admits at most one Node and defaults to a ten-minute membership window. Because it also transports long-lived deployment material, keep the link confidential and discard every copy after use or expiry. Normal restarts use the persisted data directory and do not require another invitation.

## Operational Checks

Run `scripts/verify-local-cluster.sh` before release changes involving Raft, scheduling, or transport. It verifies follower writes, distributed execution, and continued operation after the initial leader exits. Against a disposable running deployment, `scripts/verify-reference-workload.sh` creates the 1,000-Target reference workload and requires at least 99% to finish within one interval.
