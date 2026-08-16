---
title: HTTP API
description: Authenticate, discover the generated OpenAPI contract, and consume live state events.
---

Every node exposes the same HTTP API. Followers forward mutations to the leader; reads come from local replicated state after a linearizable-read barrier.

## Authentication

The WebUI exchanges an operator identity username and password for an HTTP-only session cookie. For scripts, create a revocable API token on the **Cluster** page and send it as a bearer token:

```sh
curl --header "Authorization: Bearer upgrid_REDACTED" \
  http://127.0.0.1:8080/api/v1/targets
```

`/healthz`, `/api/v1/auth/login`, and first-run setup are public. Other API routes require a valid session or API token. Never send credentials over an untrusted plaintext connection.

## OpenAPI

The server implementation generates its own OpenAPI description. Fetch the exact contract supported by a running binary:

```sh
curl --header "Authorization: Bearer upgrid_REDACTED" \
  http://127.0.0.1:8080/openapi.json
```

The repository also publishes the generated snapshot at `docs/openapi.json` for review and tooling.

## HTTP target assertions

Target create and update requests accept an ordered `assertions` array. Supported `kind` values are `body_contains`, `body_regex`, `json_path`, `response_header`, `latency`, and `script`. The response returns the same ordered representation. Invalid regular expressions, JSONPath queries, scripts, header names, limits, and resource bounds are rejected before replication. See [monitor services](/guides/targets/#assert-an-http-response) for fields and script variables.

## HTTPS target credentials

HTTPS target create and update requests accept nullable `tls_ca_secret_id`, `tls_client_certificate_secret_id`, and `tls_client_private_key_secret_id` fields. The custom CA secret augments public WebPKI roots. Client certificate and private key secrets form one mutual-TLS identity and must be configured together. These fields reject non-HTTPS URLs and `skip_tls_verification: true`. Responses return only the secret IDs; plaintext remains write-only. See [monitor services](/guides/targets/#trust-a-private-ca-or-use-mutual-tls) for PEM requirements and failure behavior.

## Multi-location evaluation

Target create and update requests accept `locations` from 1 through 32; omission defaults to `1`. The leader assigns at most that many distinct eligible voting nodes. Target responses return the configured count. One aggregate evaluation is recorded after every assigned location reports, and it succeeds only when all locations succeed.

## Long-term evaluation history

Every accepted target or cluster node evaluation contributes to a replicated one-hour rollup. `GET /api/v1/targets/{id}/history` returns those rollups in chronological order. Each item includes sample, success, and failure counts plus total, average, minimum, and maximum latency.

Use `from_ms` and `to_ms` to select at most 366 days, and `limit` to request 1 through 1,000 items. The default range is the last 30 days and the default limit is 168. When `next_cursor_ms` is present, repeat the same request with that value as `cursor_ms`; cursors are exclusive. This bounded page contract lets archival jobs advance without reading raw Raft state.

## Secret discovery and cleanup

`GET /api/v1/secrets` returns each write-only secret with a `referenced` flag. References include active and trashed targets plus notification channels; ciphertext is never returned.

`DELETE /api/v1/secrets/unreferenced` atomically recomputes references in replicated state and deletes only currently unreferenced secrets. The response lists `deleted_ids`. Calling cleanup with no unused secrets succeeds with an empty list.

## Target trash

`DELETE /api/v1/targets/{id}` atomically moves a target into replicated trash and releases its active evaluation assignments. Settings, raw history, hourly rollups, availability state, notification routing, and location count remain attached to the same target ID.

`GET /api/v1/trash/targets` lists recoverable targets with `deleted_at_ms` and `purge_at_ms`. `POST /api/v1/trash/targets/{id}/restore` restores an unexpired entry. `DELETE /api/v1/trash/targets/{id}` permanently deletes it. Expired or purged targets return `404` when restoration is attempted.

## Node lifecycle

`PUT /api/v1/nodes/{id}/drain` excludes a node from new evaluation assignments. Existing assignments may finish; the cluster response reports `draining` and `active_assignments` for each member. Cancel a drain by sending `{"draining":false}`.

`DELETE /api/v1/nodes/{id}` removes a drained remote member after its active assignment count reaches zero. `?force=true` releases assignments and removes an unreachable node immediately. A node cannot remove itself or the final voting member; send the request to another healthy cluster member.

After forced removal, create a one-use join token and start the replacement with an empty data directory. Never restart the removed node against the cluster.

## Alert operations

`GET /api/v1/alerts` returns notification-delivery history. Filter it with `target_id`, `channel_id`, `kind`, `delivery`, `acknowledged`, `from_ms`, `to_ms`, and `limit` query parameters. Limits must be between 1 and 500.

To acknowledge a delivery record, `POST /api/v1/alerts/acknowledge` with its `target_id`, `channel_id`, `scheduled_at_ms`, and `kind`. Acknowledgement is replicated and idempotent.

`POST /api/v1/alerts/retry` accepts the same locator and makes a failed or pending delivery immediately eligible for another attempt. Delivered alerts cannot be retried.

## Live events

The WebUI refreshes from `/api/v1/events`, a server-sent events stream. SSE works over ordinary HTTP and automatically reconnects in browsers. Reverse proxies must disable response buffering and allow long-lived reads.

## Consistency

A successful mutation has been accepted through the Raft leader. A successful read has crossed the local node's linearizable-read barrier. If a quorum is unavailable, operations that require cluster consistency fail instead of silently serving stale state.
