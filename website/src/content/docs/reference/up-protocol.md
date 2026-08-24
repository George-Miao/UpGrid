---
title: Up protocol
description: Reference for UpGrid node endpoints, QUIC transport, join links, and security.
---

UpGrid uses `up://` to identify a cluster node endpoint. Cluster nodes use this endpoint for Raft and node RPC traffic. It is not an HTTP API URL.

## URI format

A node endpoint has this form:

```text
up://<host>:<port>
```

The `up` scheme, a host, and a nonzero UDP port are required. Use a DNS name, an IPv4 address, or a bracketed IPv6 address.

Use a path-free endpoint with an explicit port in configuration:

```text
up://node-1.internal:11451
up://192.0.2.10:12000
up://[2001:db8::10]:12000
```

Do not add user information, a query, a fragment, or a path to a node endpoint.

The host is part of a reachable address. It does not select a local network interface or a TLS server name. `local_addresses` selects the interfaces that accept UDP traffic, and `raft_port` supplies their one shared local port.

## Transport behavior

The `up://` transport uses QUIC over UDP. It does not use TCP or HTTP. UpGrid resolves the endpoint host when it creates a connection. It reuses an open QUIC connection to a destination node and opens bidirectional streams for RPC channels.

The current RPC implementation uses UpGrid RPC messages in length-delimited Postcard frames. This framing is an internal implementation detail. It is not a stable public wire protocol.

## Reachability

Every ordered pair of nodes must have at least one working UDP route. A node can publish more than one configured or discovered address. The admission check rejects a new node unless each source node can reach each other node through at least one address. The leader keeps this directed check active and reports the cluster as degraded when a route is lost. UpGrid does not provide a relay.

Each address must identify one node. UpGrid verifies the node ID after it opens an authenticated QUIC connection. Do not put several nodes behind a load balancer. Do not use an HTTP reverse proxy for this traffic.

For DNS, firewall, container, and address translation rules, see [Network setup](/guides/network-setup/).

## Security

### Node endpoint certificates

Production `up://` endpoints always use mutual TLS. A node cannot disable certificate verification or use plaintext QUIC.

UpGrid creates and loads the internal certificate material as follows:

1. The deployment and QUIC certificate-authority keys are each 32 bytes. Their text form is standard Base64.
2. A node normally loads the keys from `deployment-key` and `quic-ca-key` in its data directory. A new cluster creates a random deployment key when none is configured and derives the default QUIC certificate-authority key from it. A joining node gets both keys from its join link.
3. Operators can supply a separate Ed25519 certificate-authority key seed before initial cluster creation.
4. On each start, a node derives its Ed25519 leaf key from the certificate-authority key and its durable node ID. It creates the same certificate for the fixed internal server name `upgrid-node` and its node-specific identity name. The certificate permits client and server authentication.
5. QUIC uses TLS 1.3.

For an outbound connection, the client verifies that the QUIC certificate authority signed the server certificate and that it is valid for the fixed internal name `upgrid-node`. It also compares the certificate with the expected certificate for the destination node ID. For an inbound connection, the server requires a client certificate that the same certificate authority signed. The RPC identity check also verifies the durable node ID before the route is accepted.

The `deployment_key`, `UPGRID_DEPLOYMENT_KEY`, and `--deployment-key` inputs supply the deployment key. The `quic_ca_key`, `UPGRID_QUIC_CA_KEY`, and `--quic-ca-key` inputs supply a separate QUIC certificate-authority key. UpGrid also accepts both keys in a join link or loads them from the data directory.

UpGrid stops startup if either key is invalid, if a configured key differs from the stored key, or if a fresh node gets different keys from direct configuration and its join link. It also stops startup if certificate generation, TLS configuration, or QUIC endpoint creation fails. It does not fall back to an insecure transport.

The QUIC certificate-authority key is stored separately; the derived leaf private key is not. A restart recreates the same leaf key and certificate for the durable node ID. UpGrid does not support in-place deployment-key or certificate-authority rotation. Every member has both cluster keys. Protect node data directories, join links, and backups. For the required controls, see [Cluster hardening](/guides/cluster-hardening/).

### HTTP API certificates

The `tls_cert`, `UPGRID_TLS_CERT`, and `--tls-cert` inputs specify a PEM certificate chain for API HTTPS. The `tls_key`, `UPGRID_TLS_KEY`, and `--tls-key` inputs specify its PEM private key. Supply both inputs together. These inputs do not configure `up://` transport security.

## Join links

A join link is not a node endpoint, even though it also uses the `up` scheme. It has a single path segment:

```text
up://node-1.internal:11451/<opaque-join-payload>
```

The authority identifies the existing node that the new node contacts. The versioned, URL-safe payload contains the deployment key, the QUIC certificate-authority key, and an admission token. Encoding does not make this payload public or safe to disclose. Treat the complete link as a bearer secret.
A join link does not contain user information, a query, or a fragment.

Pass the complete link through `join`, `UPGRID_JOIN`, or `--join`. Configure the new node's local and reachable addresses separately. They are not part of the join link.

The cluster stores the admission token hash, expiry, optional remaining-use count, and any active reservation in replicated state. Admission restores a limited use when its directed connectivity or membership work fails, and consumes the use only after success. Expiry, use exhaustion, or revocation blocks later admission. These controls do not remove key material from a link that was already disclosed.

## Configuration

| Purpose | TOML key | Environment variable | CLI option | Default or behavior |
| --- | --- | --- | --- | --- |
| Bind local UDP interfaces | `local_addresses` | `UPGRID_LOCAL_ADDRESSES` | `--local-address` | `["127.0.0.1"]` |
| Select the shared UDP port | `raft_port` | `UPGRID_RAFT_PORT` | `--raft-port` | `11451` |
| Publish directly configured addresses | `reachable_addresses` | `UPGRID_REACHABLE_ADDRESSES` | `--reachable-address` | Unset |
| Poll HTTP discovery services | `discovery_urls` | `UPGRID_DISCOVERY_URLS` | `--discovery-url` | Unset |
| Admit a fresh node with a join link | `join` | `UPGRID_JOIN` | `--join` | Unset; mutually exclusive with `new_cluster` |
| Supply the 32-byte, Base64 deployment key for bootstrap or recovery | `deployment_key` | `UPGRID_DEPLOYMENT_KEY` | `--deployment-key` | Unset; normal creation and admission persist the key automatically |
| Supply the 32-byte, Base64 QUIC certificate-authority key for bootstrap or recovery | `quic_ca_key` | `UPGRID_QUIC_CA_KEY` | `--quic-ca-key` | Derived from the deployment key; normal creation and admission persist it |
| Store deployment, QUIC certificate-authority, and node identity keys | `data_dir` | `UPGRID_DATA_DIR` | `--data-dir` | `upgrid-data` |

UpGrid accepts at most eight discovery services. They must use HTTP or HTTPS and return `{"addresses":["up://node-1.internal:11451"]}`. UpGrid limits each request to 3 seconds, 64 KiB, and 32 addresses. Discovery results use renewable reachability leases. Configured addresses do not expire.

Do not set `join` to a plain reachable address. Do not set a reachable address to a join link. Keep a dedicated durable data directory for each node. See [Configuration](/reference/configuration/) for configuration precedence and all settings.

## Limitations

- Always include a port in each reachable address.
- All local IP addresses use the same Raft port.
- A reachable address can use a different port when NAT translates the local Raft port.
- UpGrid tries all unicast socket addresses that a hostname resolves for each compatible local address family. Configure several reachable addresses for explicit route priority and failover.
- The node connection cache holds 64 entries. Eviction can cause a new connection, so this is not a 64-node admission limit.
- Raft sends a full snapshot as one in-memory RPC value. It does not split the snapshot into application-level chunks.
- The transport has no TCP or HTTP fallback. UDP must work from node to node.
- The certificate authority and deployment key cannot be rotated in place. Custom `up://` certificates are not supported.
- The RPC methods, Postcard types, and stream framing can change with the UpGrid implementation. Do not assume that an independent client or a different release can interoperate from the URI scheme alone.
