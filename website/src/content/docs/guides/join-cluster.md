---
title: Join a cluster
description: Add a node with an expiring, revocable up:// token.
---

A fresh node joins with an opaque `up://` token created by an existing cluster member. The token carries admission authority and deployment material, so handle it like a password.

## Create a token

Open **cluster** in the WebUI and select **Create token**. Choose:

- An expiration in days;
- One or more allowed uses; or
- Unlimited uses for controlled automation.

The WebUI defaults to one day and one use. Create a separate one-use token for each node unless reusable provisioning is intentional.

## Add the new node

Before you start the process, give the new node an `up://` endpoint that every cluster member can reach. Allow inbound UDP traffic on that endpoint's port from every cluster member. The default is UDP port `11451`. Update the existing members' firewall rules to accept the new node as a source.

Run the generated command on the new host with an empty data directory:

```sh
upgrid \
  --join 'up://existing-node.example/opaque-token' \
  --bind 127.0.0.1:8081 \
  --local-address 10.0.0.11 \
  --raft-port 11451 \
  --reachable-address up://node-2.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-two
```

`UPGRID_JOIN` is the environment-variable equivalent. The joining node contacts the reachable address embedded in the token and receives the cluster's operator identities and API tokens through Raft. It also publishes its configured reachable addresses and reachable address candidates. After admission, every ordered node pair must have at least one working route.

To use browser setup, start the new node without `join` or `new_cluster`, then open `/setup`. Expand **network settings** to add this node's reachable addresses and optional HTTP discovery service URLs before you paste the join token. WebUI network settings persist in the node's data directory.

## Restart an existing member

Restart with the same data directory and normal configuration. Durable membership takes precedence, so the node does not need `--join` again.

## Drain or replace a node

For planned maintenance, open **cluster**, choose **Drain**, and wait for the active-assignment count to reach zero. Remove the node, then stop its process. You can cancel the drain before removal.

For an unreachable node, first confirm its old process is permanently stopped, then choose **Replace failed** from a different healthy member. UpGrid releases its assignments and removes it from Raft membership. Create a one-use join token and start the replacement with a new node identity and an empty data directory.

Membership changes require quorum. A node cannot remove itself or the final voting member.

## Revoke reusable tokens

The **Join tokens** panel shows remaining uses and expiration. Revoke a token when provisioning ends. Revocation blocks future admissions but does not remove nodes that already joined.

:::Caution
Do not paste join tokens into logs, tickets, shell history shared with other users, or long-lived configuration management output.
:::
