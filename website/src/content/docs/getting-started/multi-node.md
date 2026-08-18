---
title: Multi-node setup
description: Start a cluster with reachable up:// endpoints.
---

Use this setup when two or more hosts must share monitoring work and replicated state. Each node needs an advertised `up://` endpoint that every other node can reach.

If you only need one node, use the [single-node setup](/getting-started/first-node/). It does not publish the cluster transport port or need a cluster transport firewall rule.

## Plan the cluster network

Choose a stable DNS name or IP address for each node. The first endpoint can look like this:

```text
up://node-1.internal:11451
```

Apply these network rules before you start a node:

- Make the hostname resolve to the node from every cluster member.
- Allow inbound UDP traffic on port `11451` from every cluster member. If the endpoint uses another port, allow that port instead.
- If the firewall filters by source address, allow every cluster member and update the rule when you add a node.
- Allow operators to reach the HTTP API port separately. Cluster members do not use the HTTP API for Raft traffic.

The `up://` endpoint uses QUIC over UDP. An HTTP reverse proxy cannot forward it.

See [Network setup](/guides/network-setup/) for complete DNS, firewall, container, and address translation rules.

## Start the first node

The following Docker command publishes the API and the cluster transport port:

Download the custom `io_uring` seccomp profile as described in [Install UpGrid](/getting-started/installation/#allow-io_uring-in-docker) before you run the container.

```sh
docker run --name upgrid \
  --security-opt seccomp=./upgrid-seccomp.json \
  --publish 8080:8080 \
  --publish 11451:11451/udp \
  --env UPGRID_RAFT_URL=up://node-1.internal:11451 \
  --env UPGRID_NODE_NAME=edge-one \
  --volume upgrid-data:/var/lib/upgrid \
  ghcr.io/george-miao/upgrid:latest
```

For a precompiled binary, set the same advertised endpoint with `--raft-url`:

```sh
upgrid \
  --bind 127.0.0.1:8080 \
  --raft-url up://node-1.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-one
```

Open `http://127.0.0.1:8080/setup` and choose **Create new cluster**. Enter the first administrator username and password. You can then create a notification channel and target, or skip those steps.

## Add a new node

Prepare the new host before you create its join token:

1. Choose a new endpoint, such as `up://node-2.internal:11451`.
2. Allow inbound UDP traffic on its endpoint from every existing member.
3. Update each existing member's firewall rule to allow traffic from the new node.
4. Open **cluster** in `node-1`'s WebUI, select **Create token**, and create a one-use token.

Run the generated command on the new host with an empty data directory:

```sh
upgrid \
  --join 'up://node-1.internal:11451/opaque-token' \
  --bind 127.0.0.1:8080 \
  --raft-url up://node-2.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-two
```

To run the new node with Docker on a separate host, use the same values as environment variables:

```sh
docker run --name upgrid \
  --security-opt seccomp=./upgrid-seccomp.json \
  --publish 8080:8080 \
  --publish 11451:11451/udp \
  --env UPGRID_JOIN='up://node-1.internal:11451/opaque-token' \
  --env UPGRID_RAFT_URL=up://node-2.internal:11451 \
  --env UPGRID_NODE_NAME=edge-two \
  --volume upgrid-data:/var/lib/upgrid \
  ghcr.io/george-miao/upgrid:latest
```

The joining node must reach the endpoint in the token. After admission, every node must reach every advertised `up://` endpoint. Check that the new member appears in **cluster**, then create a new one-use token for the next node.

Never reuse another node's data directory. Treat each join token like a password and revoke unused reusable tokens. See [Add a node](/guides/join-cluster/) for token controls, restart behavior, draining, and replacement.
