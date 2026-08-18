---
title: Network setup
description: Connect every cluster node to each advertised up:// endpoint.
---

A multi-node cluster needs direct network access between its members. Every node must be able to reach the advertised `up://` endpoint of every other node.

UpGrid sends Raft traffic directly to these endpoints and does not use a relay. Any voting node can become the leader, so the direction of traffic can change. Design the cluster network as a full mesh.

Endpoints can use private or public routable addresses. Choose addresses that remain stable and reachable from every member.

## Endpoint requirements

Give each node one stable DNS name or IP address. Configure it with `--raft-url` or `UPGRID_RAFT_URL`:

```text
up://node-1.internal:11451
```

Each endpoint must meet these requirements:

- Its hostname resolves to the correct node from every cluster member.
- Its UDP port is reachable from every other node.
- It remains stable across process restarts.
- It identifies one node. Do not put several nodes behind one load-balanced endpoint.

The default cluster transport port is UDP `11451`. If a URL uses another port, open that port instead. The `up://` transport uses QUIC over UDP, not TCP. An HTTP reverse proxy cannot forward it.

The HTTP API is separate. Operators use TCP port `8080` by default, but cluster members do not use that port for Raft traffic.

## Firewall rules

Apply these rules to every node:

| Direction | Protocol | Port | Allowed peers |
| --- | --- | --- | --- |
| Inbound | UDP | Port in this node's `up://` URL | Every other cluster node |
| Outbound | UDP | Port in the destination node's `up://` URL | Every other cluster node |

Many stateful firewalls allow outbound traffic and its replies by default. If your firewall filters egress traffic, add the outbound rule. If it filters inbound traffic by source address, allow every cluster member and update the rule when membership changes.

When you add a node, update both sides before it joins:

1. Allow every existing node to send UDP traffic to the new endpoint.
2. Allow the new node to send UDP traffic to every existing endpoint.
3. Add the new node's address to the inbound rules on the existing nodes.

## Containers and address translation

Publish the transport port as UDP when peers connect through the container host:

The command also uses the custom `io_uring` seccomp profile from [Install UpGrid](/getting-started/installation/#allow-io_uring-in-docker).

```sh
docker run \
  --security-opt seccomp=./upgrid-seccomp.json \
  --publish 11451:11451/udp \
  --env UPGRID_RAFT_URL=up://node-1.internal:11451 \
  ghcr.io/george-miao/upgrid:latest
```

You do not need a host port publish when all nodes share a container network and the advertised container address is reachable on that network. The advertised hostname and port must still be reachable by every node.

If the node is behind address translation, forward the advertised UDP port to that node and make the advertised hostname resolve to the reachable address. Keep one stable endpoint for each node.

## Check the network

Before a node joins, confirm that its advertised hostname resolves from every existing host and that its firewall accepts UDP traffic from them. Also check the existing endpoints from the new host.

`curl` and TCP port checks cannot test an `up://` endpoint. After the node joins, open **cluster** in the WebUI and confirm that the node remains active. Repeated connection or election errors in the node logs usually mean that DNS, routing, or a UDP firewall rule is wrong.

For the transport URI, connection, and security model, see [Up protocol](/reference/up-protocol/). For deployment-key protection, API TLS, and host controls, see [Cluster hardening](/guides/cluster-hardening/).
