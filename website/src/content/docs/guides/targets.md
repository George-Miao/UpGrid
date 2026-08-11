---
title: Monitor services
description: Configure HTTP, TCP, DNS, ICMP, and TLS Targets with evaluation and notification policies.
---

A Target describes an endpoint probe and the policy that turns its evaluations into availability transitions.

## Add a Target

From **Overview**, select **Add target**, choose a type, and provide its endpoint:

- **HTTP** — an `http://` or `https://` URL;
- **TCP connect** — a host and explicit port, such as `database.internal:5432`;
- **DNS resolution** — a hostname resolved through the Node's system resolver;
- **ICMP echo** — a hostname or IP address;
- **TLS certificate** — a hostname and explicit TLS port, such as `example.com:443`.

Every type supports the polling interval, timeout, consecutive-failure threshold, and notification routing. HTTP Targets additionally support accepted status ranges, literal or Secret-backed headers and request bodies, redirects, TLS verification settings, and a response-body substring requirement. The default HTTP accepted range is `200–299`; the default transition threshold is three failures.

The HTTP API accepts an explicit `kind` plus the `url`; omitted kinds default to `http` for compatibility. Non-HTTP endpoints use matching `tcp://`, `dns://`, `icmp://`, or `tls://` schemes and do not accept HTTP request options.

## Probe behavior

TCP Targets succeed after establishing a connection. DNS Targets succeed when the system resolver returns at least one IPv4 or IPv6 address. TLS Targets complete a TLS handshake and validate the certificate chain, hostname, and validity period against the bundled public roots.

ICMP Targets send an echo request. The UpGrid process needs permission to open ICMP sockets: grant `CAP_NET_RAW` on Linux, add `NET_RAW` to a container, or run under an equivalent platform policy. A missing permission or echo timeout is recorded in the Target's evaluation diagnostic.

## Use a Secret in request data

Create a reusable Secret from **Overview**, then copy the ID displayed beside its name. In a Target create or update request, use a `secret_id` object instead of a literal string for any header value or for the request body:

```json
{
  "headers": {
    "authorization": {
      "secret_id": "0198f24c-7e91-7d50-9d74-35b71a34af10"
    }
  },
  "body": {
    "secret_id": "0198f24c-7e91-7d50-9d74-35b71a34af10"
  }
}
```

UpGrid decrypts the value only while building the outbound request. Target responses identify the Secret reference but never include its plaintext. See the [HTTP API reference](/reference/api/) for the complete Target request.

## Availability states

- **Up** — the latest committed evaluations satisfy the Target policy.
- **Suspicious** — failures are accumulating but have not reached the threshold.
- **Down** — the failure threshold has been reached.
- **Paused** — polling is disabled until the Target is resumed.

UpGrid emits a transition only when availability changes. It does not send a new alert for every failed poll.

## Evaluation history

Open a Target to view recent latency and status bars. Each entry records which Cluster Node executed the poll. Raw history is replicated and bounded by `history_retention_hours` when configured.

## Cluster Nodes as Targets

UpGrid also displays each member as a read-only Node Target. The Cluster checks member reachability, graphs its history, and sends availability transitions through default notification Channels. Open a Node Target to rename the Node or inspect its history.
