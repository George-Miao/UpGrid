---
title: How UpGrid works
description: Understand Raft state, request routing, evaluation assignment, and failover.
---

UpGrid is a distributed service monitor with no separate database or control-plane service. Every voting Node runs the same binary and keeps a local copy of Cluster state.

## Request path

Every Node exposes the WebUI and HTTP API. A read is served from the local replicated state after a Raft linearizable-read barrier. A mutation received by a follower is forwarded to the current leader and committed through Raft.

This makes every healthy Node a valid API entry point while retaining one ordered state machine.

## Evaluation path

The leader schedules due work across eligible voting members. A Target normally uses one Node per interval, but can request up to 32 distinct evaluation locations. Draining Nodes receive no new assignments; timed-out or forcibly released work is reassigned.

Assigned Nodes execute probes and propose their results. Replicated state waits for every expected location and commits one deterministic aggregate Evaluation: all locations must succeed, the slowest latency is retained, and failures include bounded per-Node diagnostics. Availability thresholds and transitions consume that aggregate, so a leader change does not reset failure counts or multiply alerts.

## Membership changes

Healthy Nodes drain before removal so in-flight evaluations can finish. Failed Nodes can be force-removed while a quorum remains; replacement uses a new Node identity, an empty data directory, and a one-use Join Token. Raft serializes admission and removal so concurrent membership changes cannot overwrite one another.

## Notifications

Committed availability transitions create notification work. Telegram, SMTP email, and webhook workers deliver through configured Channels with at-least-once semantics. Delivery state, acknowledgement, and manual retry requests are replicated, so every Node presents the same alert history and a leader change does not lose operator actions.

## Transport and security boundaries

Inter-Node traffic uses the `up://` transport with mutual identity established during admission. Operator Identities and API Tokens are part of replicated Cluster state, so every Node accepts the same WebUI sessions and bearer credentials. The API can run as plaintext behind a trusted TLS reverse proxy or serve HTTPS directly.
