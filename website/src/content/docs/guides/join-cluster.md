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
./target/release/upgrid \
  --join 'up://existing-node.example/opaque-token' \
  --bind 127.0.0.1:8081 \
  --raft-url up://node-2.internal:11451 \
  --data-dir /var/lib/upgrid \
  --node-name edge-two \
  --username admin \
  --password 'replace-this-password'
```

`UPGRID_JOIN` is the environment-variable equivalent. The joining Node contacts the address embedded in the token; after admission, every advertised Raft hostname must be reachable by every Cluster member.

## Restart an existing member

Restart with the same data directory and normal configuration. Durable membership takes precedence, so the Node does not need `--join` again.

## Revoke reusable tokens

The **Join Tokens** panel shows remaining uses and expiration. Revoke a token when provisioning ends. Revocation blocks future admissions but does not remove Nodes that already joined.

:::caution
Do not paste Join Tokens into logs, tickets, shell history shared with other users, or long-lived configuration management output.
:::
