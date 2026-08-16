---
title: Start your first node
description: Use the browser setup flow or create a cluster non-interactively.
---

Start UpGrid with a durable data directory, an API address, and an advertised `up://` Raft address:

```sh
upgrid \
  --bind 127.0.0.1:8080 \
  --raft-url up://node-1.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-one \
```

Open `http://127.0.0.1:8080/setup`, review the node name, and choose **Create new cluster**. Enter the first administrator username and password when prompted. This operator identity is replicated to every cluster node. The setup flow can then create a notification channel and target, or you can skip both steps.

## Unattended setup

Pass `--new-cluster` to skip the browser decision:

```sh
upgrid \
  --new-cluster \
  --bind 127.0.0.1:8080 \
  --raft-url up://node-1.internal:11451 \
  --data-dir /var/lib/upgrid \
  --username admin \
  --password 'replace-this-password'
```

The equivalent environment variable is `UPGRID_NEW_CLUSTER=true`. Unattended cluster creation also requires `--username` and `--password`; there are no safe default credentials.

## Verify the node

`/healthz` is public and suitable for a process health check:

```sh
curl --fail http://127.0.0.1:8080/healthz
```

All other API and WebUI routes require a replicated operator identity or API token. Keep plaintext HTTP on a trusted network or place it behind a TLS reverse proxy.

The cluster is now ready for [additional nodes](/guides/join-cluster/) and [service targets](/guides/targets/).
