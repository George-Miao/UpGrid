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

## Live events

The WebUI refreshes from `/api/v1/events`, a Server-Sent Events stream. SSE works over ordinary HTTP and automatically reconnects in browsers. Reverse proxies must disable response buffering and allow long-lived reads.

## Consistency

A successful mutation has been accepted through the Raft leader. A successful read has crossed the local Node's linearizable-read barrier. If a quorum is unavailable, operations that require Cluster consistency fail instead of silently serving stale state.
