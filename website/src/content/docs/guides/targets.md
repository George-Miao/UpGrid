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

Every type supports the polling interval, timeout, consecutive-failure threshold, and notification routing. HTTP Targets additionally support accepted status ranges, literal or Secret-backed headers and request bodies, redirects, TLS verification settings, and ordered response assertions. The default HTTP accepted range is `200–299`; the default transition threshold is three failures.

The HTTP API accepts an explicit `kind` plus the `url`; omitted kinds default to `http` for compatibility. Non-HTTP endpoints use matching `tcp://`, `dns://`, `icmp://`, or `tls://` schemes and do not accept HTTP request options.

## Probe behavior

TCP Targets succeed after establishing a connection. DNS Targets succeed when the system resolver returns at least one IPv4 or IPv6 address. TLS Targets complete a TLS handshake and validate the certificate chain, hostname, and validity period against the bundled public roots.

ICMP Targets send an echo request. The UpGrid process needs permission to open ICMP sockets: grant `CAP_NET_RAW` on Linux, add `NET_RAW` to a container, or run under an equivalent platform policy. A missing permission or echo timeout is recorded in the Target's evaluation diagnostic.

## Assert an HTTP response

Add assertions while creating or editing an HTTP Target. UpGrid checks the accepted status range first, then evaluates assertions in their displayed order and records the first failure as the evaluation diagnostic.

Available assertion kinds:

- **Body contains** requires a literal substring.
- **Body regex** matches a Rust regular expression against the response body.
- **JSONPath** runs an RFC 9535 query. With no expected value, the query must select at least one value; with an expected value, at least one selected string or JSON value must match it.
- **Response header** requires a case-insensitive header name and can additionally require its exact value.
- **Latency threshold** limits the complete probe duration in milliseconds.
- **Script** evaluates a boolean [Rhai](https://rhai.rs/) expression with `status`, `latency_ms`, `body`, `final_url`, and a lower-case `headers` map.

Scripts cannot import modules, define functions, evaluate generated code, or use loops. Each run is bounded to 10,000 operations, 32 expression levels, 16 function-call levels, 64 KiB strings, and 1,024-element arrays, maps, or response-header entries. The script receives at most the first 64 KiB of the response body and each header value. A Target accepts at most 32 assertions; ordinary assertion values are limited to 4 KiB and scripts to 8 KiB.

```json
{
  "assertions": [
    { "kind": "response_header", "name": "content-type", "value": "application/json" },
    { "kind": "json_path", "path": "$.services[*].healthy", "expected": "true" },
    { "kind": "latency", "max_ms": 500 },
    { "kind": "script", "source": "status == 200 && headers[\"content-type\"] == \"application/json\"" }
  ]
}
```

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

## Trust a private CA or use mutual TLS

Store PEM credentials as separate Secrets, then select them under **HTTPS trust and mutual TLS** while creating or editing an HTTPS Target:

- **Custom CA bundle Secret** contains one or more PEM certificates. UpGrid adds them to its public WebPKI roots rather than replacing public trust.
- **Client certificate Secret** contains the PEM client certificate chain.
- **Client private key Secret** contains the matching PEM private key.

Client certificate and private key Secrets must be configured together. Custom credentials are accepted only for `https://` Targets and cannot be combined with **Skip TLS verification**. Missing Secrets, malformed PEM, an empty CA bundle, a mismatched client key, an untrusted server, or a rejected client certificate produces an evaluation diagnostic without exposing plaintext.

The API uses nullable Secret IDs:

```json
{
  "tls_ca_secret_id": "0198f24c-7e91-7d50-9d74-35b71a34af10",
  "tls_client_certificate_secret_id": "0198f24c-d837-7fa3-9ac3-7b5fe695472c",
  "tls_client_private_key_secret_id": "0198f24d-165d-78f3-82cd-79ef0277ae09"
}
```

The executing probe Node decrypts the referenced values only while constructing that Target's TLS client. API responses and the WebUI return only Secret IDs and names.

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
