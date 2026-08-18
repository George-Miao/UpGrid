---
title: Single-node setup
description: Start UpGrid without a public cluster transport endpoint.
---

Use this setup when one node will run all monitoring work. You do not need to publish the `up://` UDP port or add a firewall rule for cluster transport.

## Start with Docker

Download the custom `io_uring` seccomp profile as described in [Install UpGrid](/getting-started/installation/#allow-io_uring-in-docker) before you run the container.

```sh
docker run --name upgrid \
  --security-opt seccomp=./upgrid-seccomp.json \
  --publish 8080:8080 \
  --volume upgrid-data:/var/lib/upgrid \
  ghcr.io/george-miao/upgrid:latest
```

The command publishes only the HTTP API and WebUI. It does not publish UDP port `11451`.

## Start a binary

```sh
upgrid \
  --bind 127.0.0.1:8080 \
  --data-dir /var/lib/upgrid \
  --node-name edge-one
```

## Complete browser setup

Open `http://127.0.0.1:8080/setup`, review the node name, and choose **Create new cluster**. Enter the first administrator username and password when prompted. This operator identity is stored in the cluster state. The setup flow can then create a notification channel and target, or you can skip both steps.

## Unattended setup

Pass `--new-cluster` to skip the browser decision:

```sh
upgrid \
  --new-cluster \
  --bind 127.0.0.1:8080 \
  --data-dir /var/lib/upgrid \
  --username admin \
  --password 'replace-this-password'
```

The equivalent environment variable is `UPGRID_NEW_CLUSTER=true`. Unattended cluster creation also requires `--username` and `--password`. There are no default credentials.

## Verify the node

`/healthz` is public and suitable for a process health check:

```sh
curl --fail http://127.0.0.1:8080/healthz
```

All other API and WebUI routes require an operator identity or API token. Keep plain HTTP on a trusted network or place it behind a TLS reverse proxy.

The node is now ready for [service targets](/guides/targets/). Use the [multi-node setup](/getting-started/multi-node/) instead when you need cluster members on separate hosts.
