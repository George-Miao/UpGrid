---
title: Start your first Node
description: Use the browser setup flow or create a Cluster non-interactively.
---

Start UpGrid with a durable data directory, an API address, and an advertised `up://` Raft address:

```sh
upgrid \
  --bind 127.0.0.1:8080 \
  --raft-url up://node-1.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-one \
  --username admin \
  --password 'replace-this-password'
```

Open `http://127.0.0.1:8080/setup` and authenticate with the configured username and password. Review the Node name and choose **Create new Cluster**. The setup flow can then create a notification Channel and Target, or you can skip both steps.

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

The equivalent environment variable is `UPGRID_NEW_CLUSTER=true`.

## Verify the Node

`/healthz` is public and suitable for a process health check:

```sh
curl --fail http://127.0.0.1:8080/healthz
```

All other API and WebUI routes use HTTP Basic authentication. If the API is plaintext, keep it on a trusted network or place it behind a TLS reverse proxy.

The Cluster is now ready for [additional Nodes](/guides/join-cluster/) and [service Targets](/guides/targets/).
