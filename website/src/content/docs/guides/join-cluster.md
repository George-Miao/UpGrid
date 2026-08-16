---
title: Join a cluster
description: Admit another node with an expiring, revocable up:// token.
---

A fresh node joins with an opaque `up://` join token created by an existing cluster member. The token carries admission authority and deployment material, so handle it like a password.

## Create a token

Open **Cluster** in the WebUI and select **Create token**. Choose:

- An expiration in days;
- One or more allowed uses; or
- Unlimited uses for controlled automation.

The WebUI defaults to one day and one use. Create a separate one-use token for each node unless reusable provisioning is intentional.

## Join the new node

Run the generated command on a host with an empty data directory:

```sh
upgrid \
  --join 'up://existing-node.example/opaque-token' \
  --bind 127.0.0.1:8081 \
  --raft-url up://node-2.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-two
```

`UPGRID_JOIN` is the environment-variable equivalent. The joining node contacts the address embedded in the token and receives the cluster's operator identities and API tokens through Raft. After admission, every advertised Raft hostname must be reachable by every cluster member.

## Restart an existing member

Restart with the same data directory and normal configuration. Durable membership takes precedence, so the node does not need `--join` again.

## Drain or replace a node

For planned maintenance, open **Cluster**, choose **Drain**, and wait for the active-assignment count to reach zero. Remove the node, then stop its process. You can cancel the drain before removal.

For an unreachable node, first confirm its old process is permanently stopped, then choose **Replace failed** from a different healthy member. UpGrid releases its assignments and removes it from Raft membership. Create a one-use join token and start the replacement with a new node identity and an empty data directory.

Membership changes require quorum. A node cannot remove itself or the final voting member.

## Revoke reusable tokens

The **Join tokens** panel shows remaining uses and expiration. Revoke a token when provisioning ends. Revocation blocks future admissions but does not remove nodes that already joined.

:::Caution
Do not paste join tokens into logs, tickets, shell history shared with other users, or long-lived configuration management output.
:::
