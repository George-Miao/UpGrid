---
title: Cluster hardening
description: Protect deployment material, network access, the API, and node storage.
---

This guide covers the controls around the cluster transport, API, and hosts. See [Up protocol](/reference/up-protocol/) for the transport URI, QUIC behavior, certificate model, and protocol limitations.

## Protect deployment material

Every cluster member holds the deployment key, so a compromise of one member can compromise cluster transport trust and encrypted cluster secrets.

- Restrict each data directory to the operating-system account that runs UpGrid.
- Encrypt host storage and backups that contain a node data directory.
- Keep recovery copies in the same protected storage as other production credentials.
- Do not copy one node's data directory to create another node.
- Do not place `UPGRID_SECRET_KEY` or `--secret-key` in permanent configuration. Use it only for bootstrap or recovery when required.

A join link contains both admission authority and the deployment key. Use a short-lived, one-use token. Deliver it through a secret manager or another protected channel, and remove it from process-manager configuration after the node joins. Do not put join links in chat, tickets, logs, or shell history. Revoke every unused reusable token.

Revoking an admission token cannot protect a deployment key that was already disclosed. UpGrid does not support in-place deployment-key or QUIC certificate-authority rotation. If the deployment key is exposed, isolate the cluster and create a new cluster with new data directories and deployment material.

## Restrict cluster access

Apply the DNS, firewall, container, and address translation rules in [Network setup](/guides/network-setup/). Permit inbound UDP only on the advertised transport port. Where member source addresses are stable, limit firewall rules to those addresses.

## Protect the API and WebUI

Bind the HTTP API to loopback behind a TLS reverse proxy, or configure native HTTPS with a PEM certificate chain and private key:

```toml title="/etc/upgrid.toml"
bind = "0.0.0.0:8080"
tls_cert = "/etc/upgrid/api-chain.pem"
tls_key = "/etc/upgrid/api-key.pem"
```

Restrict API access to operator networks. The API port is separate from the `up://` port and does not need to be open between cluster members. See [Deployment](/reference/deployment/) for reverse proxy and native HTTPS examples.

Treat every operator password and API token as a cluster administrator credential. Create only the identities and tokens that operators and integrations need. Revoke unused tokens. Remove unattended setup usernames and passwords from configuration after cluster creation.

## Harden node hosts

- Run each node as a dedicated, unprivileged operating-system account.
- Give that account access only to its own data directory and required API certificate files.
- Use a separate data directory and node identity on every host.
- Keep the host, container image, and UpGrid release current. Pin a tested image tag or digest instead of changing versions without review.
- Keep host clocks synchronized so admission-token expiration works consistently.
- Back up durable state through an encrypted and access-controlled process.
- Drain a healthy node before planned maintenance. Fence a failed process before you remove or replace its membership.

## Review the deployment

Before production use, confirm all of these controls:

- Every node can reach every other node's advertised UDP endpoint.
- Data directories, backups, deployment keys, and join links have restricted access.
- The API uses HTTPS or stays on a trusted private network.
- Bootstrap credentials and used join links are absent from persistent configuration.
- Unused operator identities, API tokens, and admission tokens are revoked.
