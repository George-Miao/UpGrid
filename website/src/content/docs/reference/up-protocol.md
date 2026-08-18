---
title: Up protocol
description: Reference for UpGrid node endpoints, QUIC transport, join links, and security.
---

UpGrid uses `up://` to identify a cluster node endpoint. Peers use this endpoint for Raft and node RPC traffic. It is not an HTTP API URL.

## URI format

A node endpoint has this form:

```text
up://<host>[:<port>]
```

The `up` scheme and a host are required. Use a DNS name that resolves to a reachable IPv4 address, or use an IPv4 address directly. If the endpoint reader does not find a port, it uses UDP port `11451`.

Use a path-free endpoint with an explicit port in configuration:

```text
up://node-1.internal:11451
up://192.0.2.10:12000
```

Do not add user information, a query, a fragment, or a path to a node endpoint.

The host is the advertised node address and the TLS server name. The port is also the local UDP listen port. The current process listens on the IPv4 wildcard address for that port. The host does not select a local network interface.

## Transport behavior

The `up://` transport uses QUIC over UDP. It does not use TCP or HTTP. UpGrid resolves the endpoint host when it creates a connection. It reuses an open QUIC connection to a peer and opens bidirectional streams for RPC channels.

The current RPC implementation uses tarpc messages in length-delimited Postcard frames. This framing is an internal implementation detail. It is not a stable public wire protocol.

## Reachability

Every node must be able to send UDP traffic directly to every other advertised endpoint. UpGrid does not provide a relay. A node can send traffic to any peer, and any voting node can become the leader. Use a full-mesh network policy.

Each endpoint must identify one node and remain stable across restarts. Do not put several nodes behind a load balancer. Do not use an HTTP reverse proxy for this traffic.

For DNS, firewall, container, and address translation rules, see [Network setup](/guides/network-setup/).

## Security

### Node endpoint certificates

Production `up://` endpoints always use mutual TLS. A node cannot disable certificate verification or use plaintext QUIC.

UpGrid creates and loads the internal certificate material as follows:

1. The deployment and QUIC certificate-authority keys are each 32 bytes. Their text form is standard Base64.
2. A node normally loads the keys from `deployment-key` and `quic-ca-key` in its data directory. A new cluster creates a random deployment key when none is configured and derives the default QUIC certificate-authority key from it. A joining node gets both keys from its join link.
3. Operators can supply a separate Ed25519 certificate-authority key seed before initial cluster creation.
4. On each start, a node creates a new leaf private key and certificate for the host in its advertised `up://` endpoint. The certificate permits client and server authentication.
5. QUIC uses TLS 1.3.

For an outbound connection, the client verifies that the QUIC certificate authority signed the server certificate. It also verifies the server host against the certificate. For an inbound connection, the server requires a client certificate that the same certificate authority signed. A certificate does not contain or verify the Raft node ID. The admission token check is separate.

The `deployment_key`, `UPGRID_DEPLOYMENT_KEY`, and `--deployment-key` inputs supply the deployment key. The `quic_ca_key`, `UPGRID_QUIC_CA_KEY`, and `--quic-ca-key` inputs supply a separate QUIC certificate-authority key. UpGrid also accepts both keys in a join link or loads them from the data directory.

UpGrid stops startup if either key is invalid, if a configured key differs from the stored key, or if a fresh node gets different keys from direct configuration and its join link. It also stops startup if certificate generation, TLS configuration, or QUIC endpoint creation fails. It does not fall back to an insecure transport.

The QUIC certificate-authority key is stored separately; the leaf private key is not. A restart creates a new leaf key and certificate. UpGrid does not support in-place deployment-key or certificate-authority rotation. Every member has both keys. Protect node data directories, join links, and backups. For the required controls, see [Cluster hardening](/guides/cluster-hardening/).

### HTTP API certificates

The `tls_cert`, `UPGRID_TLS_CERT`, and `--tls-cert` inputs specify a PEM certificate chain for API HTTPS. The `tls_key`, `UPGRID_TLS_KEY`, and `--tls-key` inputs specify its PEM private key. Supply both inputs together. These inputs do not configure `up://` transport security.

## Join links

A join link is not a node endpoint, even though it also uses the `up` scheme. It has a single path segment:

```text
up://node-1.internal:11451/<opaque-invitation>
```

The authority identifies the existing node that the new node contacts. The versioned, URL-safe payload contains the deployment key, the QUIC certificate-authority key, and an admission token. Encoding does not make this payload public or safe to disclose. Treat the complete link as a bearer secret.
A join link does not contain user information, a query, or a fragment.

Pass the complete link through `join`, `UPGRID_JOIN`, or `--join`. Configure the new node's own endpoint separately with `raft_url`, `UPGRID_RAFT_URL`, or `--raft-url`. The new endpoint is not part of the join link.

The cluster stores the admission token hash, expiry, and optional remaining-use count in replicated state. Expiry, use exhaustion, or revocation blocks later admission. These controls do not remove key material from a link that was already disclosed.

## Configuration

| Purpose | TOML key | Environment variable | CLI option | Default or behavior |
| --- | --- | --- | --- | --- |
| Advertise the node and select the UDP listen port | `raft_url` | `UPGRID_RAFT_URL` | `--raft-url` | `up://127.0.0.1:11451` |
| Admit a fresh node with a join link | `join` | `UPGRID_JOIN` | `--join` | Unset; mutually exclusive with `new_cluster` |
| Supply the 32-byte, Base64 deployment key for bootstrap or recovery | `deployment_key` | `UPGRID_DEPLOYMENT_KEY` | `--deployment-key` | Unset; normal creation and admission persist the key automatically |
| Supply the 32-byte, Base64 QUIC certificate-authority key for bootstrap or recovery | `quic_ca_key` | `UPGRID_QUIC_CA_KEY` | `--quic-ca-key` | Derived from the deployment key; normal creation and admission persist it |
| Store deployment, QUIC certificate-authority, and node identity keys | `data_dir` | `UPGRID_DATA_DIR` | `--data-dir` | `upgrid-data` |

Do not set `join` to a plain node endpoint. Do not set `raft_url` to a join link. Keep a dedicated durable data directory for each node. See [Configuration](/reference/configuration/) for configuration precedence and all settings.

## Limitations

- Always include the port in `raft_url`. Direct node parsing has a `11451` fallback, but current join-link generation requires an explicit port.
- The listener binds to the IPv4 wildcard address. There is no separate setting for the `up://` listen address and the advertised address.
- A new connection uses the first socket address returned by name resolution. Do not depend on several DNS records for transport failover or load balancing.
- The peer connection cache holds 64 entries. Eviction can cause a new connection, so this is not a 64-node admission limit.
- Raft sends a full snapshot as one in-memory RPC value. It does not split the snapshot into application-level chunks.
- The transport has no TCP or HTTP fallback. UDP must work from node to node.
- The certificate authority and deployment key cannot be rotated in place. Custom `up://` certificates are not supported.
- The RPC methods, Postcard types, and stream framing can change with the UpGrid implementation. Do not assume that an independent client or a different release can interoperate from the URI scheme alone.
