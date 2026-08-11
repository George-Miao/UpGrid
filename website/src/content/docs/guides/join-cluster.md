---
title: Join a Cluster
description: Admit another Node with an expiring, revocable up:// token.
---

A fresh Node joins with an opaque `up://` Join Token created by an existing Cluster member. The token carries admission authority and deployment material, so handle it like a password.

## Create a token

Open **Cluster** in the WebUI and select **Create token**. Choose:

- an expiration in days;
- one or more allowed uses; or
- unlimited uses for controlled automation.

The WebUI defaults to one day and one use. Create a separate one-use token for each Node unless reusable provisioning is intentional.

## Join the new Node

Run the generated command on a host with an empty data directory:

```sh
upgrid \
  --join 'up://existing-node.example/opaque-token' \
  --bind 127.0.0.1:8081 \
  --raft-url up://node-2.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-two
```

`UPGRID_JOIN` is the environment-variable equivalent. The joining Node contacts the address embedded in the token and receives the Cluster's Operator Identities and API Tokens through Raft. After admission, every advertised Raft hostname must be reachable by every Cluster member.

## Restart an existing member

Restart with the same data directory and normal configuration. Durable membership takes precedence, so the Node does not need `--join` again.

## Drain or replace a Node

For planned maintenance, open **Cluster**, choose **Drain**, and wait for the active-assignment count to reach zero. Remove the Node, then stop its process. You can cancel the drain before removal.

For an unreachable Node, first confirm its old process is permanently stopped, then choose **Replace failed** from a different healthy member. UpGrid releases its assignments and removes it from Raft membership. Create a one-use Join Token and start the replacement with a new Node identity and an empty data directory.

Membership changes require quorum. A Node cannot remove itself or the final voting member.

## Revoke reusable tokens

The **Join Tokens** panel shows remaining uses and expiration. Revoke a token when provisioning ends. Revocation blocks future admissions but does not remove Nodes that already joined.

:::caution
Do not paste Join Tokens into logs, tickets, shell history shared with other users, or long-lived configuration management output.
:::
