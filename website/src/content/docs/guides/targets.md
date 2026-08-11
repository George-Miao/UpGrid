---
title: Monitor services
description: Configure HTTP Targets, evaluation policy, and per-Target notification routing.
---

A Target describes one HTTP or HTTPS request and the policy that turns its evaluations into availability transitions.

## Add a Target

From **Overview**, select **Add target** and provide a name and URL. The WebUI also configures the HTTP method, polling interval, timeout, failure threshold, and notification routing.

The HTTP API supports the complete Target policy:

- accepted status ranges;
- literal or Secret-backed headers and request bodies;
- redirect behavior and maximum redirects;
- TLS certificate verification; and
- a response-body substring requirement.

The default accepted range is `200–299`; the default transition threshold is three failures.

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
