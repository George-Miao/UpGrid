---
title: Multi-node setup
description: Start a cluster with reachable up:// endpoints.
---

Use this setup when two or more hosts must share monitoring work and replicated state. Each node needs an advertised `up://` endpoint that every other node can reach.

If you only need one node, use the [single-node setup](/getting-started/first-node/). It does not publish the cluster transport port or need a cluster transport firewall rule.

Before you start a process, prepare each host with one of these installation options:

- [Docker image and seccomp profile](/getting-started/first-node/#docker-cli)
- [Precompiled Linux binary](/getting-started/first-node/#precompiled-linux-binary)
- [Source build](/getting-started/first-node/#build-from-source)

The published `docker-compose.yaml` file is for a single node. It does not publish the cluster transport port. Use the multi-node Docker commands on this page instead.

## 1. Plan the cluster network

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

## 2. Start the first node

Choose one of these run methods.

<details class="run-option" id="first-node-docker">
<summary>Docker CLI</summary>

The following command publishes the API and the cluster transport port:

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

</details>

<details class="run-option" id="first-node-binary">
<summary>Precompiled or source-built binary</summary>

Set the advertised endpoint with `--raft-url`:

```sh
upgrid \
  --bind 127.0.0.1:8080 \
  --raft-url up://node-1.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-one
```

</details>

## 3. Create the cluster

Open `http://<host>:8080/setup` and choose **Create new cluster**. Enter the first administrator username and password. You can then create a notification channel and target, or skip those steps.

## 4. Add a new node

### Prepare the new host

Prepare the new host before you create its join token:

1. Choose a new endpoint, such as `up://node-2.internal:11451`.
2. Allow inbound UDP traffic on its endpoint from every existing member.
3. Update each existing member's firewall rule to allow traffic from the new node.
4. Open **cluster** in `node-1`'s WebUI, select **Create token**, and create a one-use token.

Choose one of these run methods for the new node.

<details class="run-option" id="new-node-binary">
<summary>Precompiled or source-built binary</summary>

Run the generated command on the new host with an empty data directory:

```sh
upgrid \
  --join 'up://node-1.internal:11451/opaque-token' \
  --bind 127.0.0.1:8080 \
  --raft-url up://node-2.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-two
```

</details>

<details class="run-option" id="new-node-docker">
<summary>Docker CLI</summary>

Use the same values as environment variables:

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

</details>

## 5. Verify membership

The joining node must reach the endpoint in the token. After admission, every node must reach every advertised `up://` endpoint. Check that the new member appears in **cluster**, then create a new one-use token for the next node.

Never reuse another node's data directory. Treat each join token like a password and revoke unused reusable tokens. See [Add a node](/guides/join-cluster/) for token controls, restart behavior, draining, and replacement.
