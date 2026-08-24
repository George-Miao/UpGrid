---
title: Network setup
description: Connect every cluster node to each advertised up:// endpoint.
---

A multi-node cluster needs direct network access between its members. Every ordered node pair must have at least one working `up://` route.

UpGrid sends Raft traffic directly and does not use a relay. Any voting node can become the leader, so the direction of traffic can change. Design the cluster network as a full mesh. Nodes can publish more than one address for networks with NAT, VPN, or several interfaces.

Addresses can use private or public routable hosts. A reachable address can use a different public port when NAT translates the shared local Raft port. Configure stable addresses when possible. UpGrid can also retain verified reachable addresses and discovery-service reachable address candidates with renewable reachability leases.

## Endpoint requirements

Set one or more local IP addresses, one shared UDP port, and any reachable addresses that other nodes can use:

```sh
upgrid \
  --local-address 10.0.0.10 \
  --raft-port 11451 \
  --reachable-address up://node-1.internal:11451 \
  --reachable-address up://node-1.vpn:11451
```

Each reachable address must meet these requirements:

- Its hostname resolves to the correct node from each source node that uses this route.
- Its UDP port is reachable from at least one other node.
- It remains stable across process restarts when configured directly.
- It identifies one node. Do not put several nodes behind one load-balanced address.

The default cluster transport port is UDP `11451`. If a URL uses another port, open that port instead. The `up://` transport uses QUIC over UDP, not TCP. An HTTP reverse proxy cannot forward it.

The HTTP API is separate. Operators use TCP port `8080` by default, but cluster members do not use that port for Raft traffic.

## Network policy

- Expose the API only through HTTPS or a trusted private network.
- Allow each cluster node to reach every advertised `up://` address.
- Restrict inter-node transport ports to cluster members.
- Manage operator identities and API tokens in the cluster page; Raft replicates them to every node.
- Back up durable state, but never restore one node's directory as a new identity.
- Drain healthy nodes before planned removal. Force-remove a failed node only after fencing its old process, then replace it with a fresh data directory and node identity.

## Firewall rules

Apply these rules to every node:

| Direction | Protocol | Port | Allowed nodes |
| --- | --- | --- | --- |
| Inbound | UDP | Port in this node's `up://` URL | Every other cluster node |
| Outbound | UDP | Port in the destination node's `up://` URL | Every other cluster node |

Many stateful firewalls allow outbound traffic and its replies by default. If your firewall filters egress traffic, add the outbound rule. If it filters inbound traffic by source address, allow every cluster member and update the rule when membership changes.

When you add a node, update both sides before it joins:

1. Allow every existing node to send UDP traffic to the new endpoint.
2. Allow the new node to send UDP traffic to every existing endpoint.
3. Add the new node's address to the inbound rules on the existing nodes.

## Containers and address translation

Publish the transport port as UDP when other nodes connect through the container host:

The command also uses the custom `io_uring` seccomp profile from the [Docker reference](/reference/docker/#allow-io_uring-in-docker).

```sh
docker run \
  --security-opt seccomp=./upgrid-seccomp.json \
  --publish 11451:11451/udp \
  --env UPGRID_LOCAL_ADDRESSES='["0.0.0.0"]' \
  --env UPGRID_REACHABLE_ADDRESSES='["up://node-1.internal:11451"]' \
  ghcr.io/george-miao/upgrid:latest
```

You do not need a host port publish when all nodes share a container network and a configured or discovered container address is reachable on that network.

If the node is behind address translation, forward the reachable UDP port to that node. Add both private and public addresses when different source nodes use different routes.

## Check the network

Before a node joins, confirm that at least one route works in each required direction. `curl` and TCP port checks cannot test an `up://` address.

After the node joins, open the cluster page. It lists each node's reachable addresses and reports failed directed routes. A healthy three-node cluster has six working directed routes.

For the transport URI, connection, and security model, see [Up protocol](/reference/up-protocol/). For deployment-key protection, API TLS, and host controls, see [Cluster hardening](/guides/cluster-hardening/).
