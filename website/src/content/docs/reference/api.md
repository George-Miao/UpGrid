---
title: HTTP API
description: Authenticate, discover the generated OpenAPI contract, and consume live state events.
---

Every Node exposes the same HTTP API. Followers forward mutations to the leader; reads come from local replicated state after a linearizable-read barrier.

## Authentication

The WebUI exchanges an Operator Identity username and password for an HTTP-only session cookie. For scripts, create a revocable API Token on the **Cluster** page and send it as a bearer token:

```sh
curl --header "Authorization: Bearer upgrid_REDACTED" \
  http://127.0.0.1:8080/api/v1/targets
```

`/healthz`, `/api/v1/auth/login`, and first-run setup are public. Other API routes require a valid session or API Token. Never send credentials over an untrusted plaintext connection.

## OpenAPI

The server implementation generates its own OpenAPI description. Fetch the exact contract supported by a running binary:

```sh
curl --header "Authorization: Bearer upgrid_REDACTED" \
  http://127.0.0.1:8080/openapi.json
```

The repository also publishes the generated snapshot at `docs/openapi.json` for review and tooling.

## HTTP Target assertions

Target create and update requests accept an ordered `assertions` array. Supported `kind` values are `body_contains`, `body_regex`, `json_path`, `response_header`, `latency`, and `script`. The response returns the same ordered representation. Invalid regular expressions, JSONPath queries, scripts, header names, limits, and resource bounds are rejected before replication. See [Monitor services](/guides/targets/#assert-an-http-response) for fields and script variables.

## HTTPS Target credentials

HTTPS Target create and update requests accept nullable `tls_ca_secret_id`, `tls_client_certificate_secret_id`, and `tls_client_private_key_secret_id` fields. The custom CA Secret augments public WebPKI roots. Client certificate and private key Secrets form one mutual-TLS identity and must be configured together. These fields reject non-HTTPS URLs and `skip_tls_verification: true`. Responses return only the Secret IDs; plaintext remains write-only. See [Monitor services](/guides/targets/#trust-a-private-ca-or-use-mutual-tls) for PEM requirements and failure behavior.

## Node lifecycle

`PUT /api/v1/nodes/{id}/drain` excludes a Node from new evaluation assignments. Existing assignments may finish; the Cluster response reports `draining` and `active_assignments` for each member. Cancel a drain by sending `{"draining":false}`.

`DELETE /api/v1/nodes/{id}` removes a drained remote member after its active assignment count reaches zero. `?force=true` releases assignments and removes an unreachable Node immediately. A Node cannot remove itself or the final voting member; send the request to another healthy Cluster member.

After forced removal, create a one-use Join Token and start the replacement with an empty data directory. Never restart the removed Node against the Cluster.

## Alert operations

`GET /api/v1/alerts` returns notification-delivery history. Filter it with `target_id`, `channel_id`, `kind`, `delivery`, `acknowledged`, `from_ms`, `to_ms`, and `limit` query parameters. Limits must be between 1 and 500.

To acknowledge a delivery record, `POST /api/v1/alerts/acknowledge` with its `target_id`, `channel_id`, `scheduled_at_ms`, and `kind`. Acknowledgement is replicated and idempotent.

`POST /api/v1/alerts/retry` accepts the same locator and makes a failed or pending delivery immediately eligible for another attempt. Delivered alerts cannot be retried.

## Live events

The WebUI refreshes from `/api/v1/events`, a Server-Sent Events stream. SSE works over ordinary HTTP and automatically reconnects in browsers. Reverse proxies must disable response buffering and allow long-lived reads.

## Consistency

A successful mutation has been accepted through the Raft leader. A successful read has crossed the local Node's linearizable-read barrier. If a quorum is unavailable, operations that require Cluster consistency fail instead of silently serving stale state.
