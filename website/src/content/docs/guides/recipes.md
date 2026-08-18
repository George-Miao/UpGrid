---
title: Recipes
description: Apply practical recipes for durable state, API TLS, network controls, and health checks.
---

Run each node with a dedicated durable directory. Preserve `deployment-key`, `node-id`, `raft-log.redb`, and `raft-state.postcard` across restarts.

## Reverse proxy TLS

The API serves HTTP by default so a reverse proxy can terminate TLS. Preserve streaming and disable buffering for the server-sent events endpoint.

```text title="Caddyfile"
upgrid.example.com {
    reverse_proxy 127.0.0.1:8080 {
        flush_interval -1
    }
}
```

```nginx title="nginx.conf"
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
    proxy_read_timeout 1h;
}
```

## Native HTTPS

Configure a PEM certificate chain and private key together:

```toml
tls_cert = "/etc/upgrid/api-chain.pem"
tls_key = "/etc/upgrid/api-key.pem"
```

The same options are available as `UPGRID_TLS_CERT` and `UPGRID_TLS_KEY`, or `--tls-cert` and `--tls-key`. UpGrid refuses a partial pair.

## Operational checks

Use `/healthz` for process checks. For repository changes involving Raft, scheduling, or transport, run `scripts/verify-local-cluster.sh` before release.
