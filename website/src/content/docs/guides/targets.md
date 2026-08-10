---
title: Monitor services
description: Configure HTTP Targets, evaluation policy, and per-Target notification routing.
---

A Target describes one HTTP or HTTPS request and the policy that turns its evaluations into availability transitions.

## Add a Target

From **Overview**, select **Add target** and provide a name and URL. UpGrid supports every HTTP method and lets you configure:

- accepted status ranges;
- literal or Secret-backed headers and request bodies;
- polling interval and timeout;
- redirect behavior and maximum redirects;
- TLS certificate verification;
- a response-body substring requirement; and
- the number of consecutive failures required before the Target becomes down.

The default accepted range is `200–299`; the default transition threshold is three failures.

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
