---
title: How UpGrid works
description: Understand Raft state, request routing, evaluation assignment, and failover.
---

UpGrid is a distributed service monitor with no separate database or control-plane service. Every voting Node runs the same binary and keeps a local copy of Cluster state.

## Request path

Every Node exposes the WebUI and HTTP API. A read is served from the local replicated state after a Raft linearizable-read barrier. A mutation received by a follower is forwarded to the current leader and committed through Raft.

This makes every healthy Node a valid API entry point while retaining one ordered state machine.

## Evaluation path

The leader schedules due work across voting members. The assigned Node executes the HTTP request and proposes its result. For a given Target interval, the first accepted result is committed and duplicate results are discarded.

Availability thresholds and transitions are computed from replicated state, so a leader change does not reset failure counts.

## Notifications

Committed availability transitions create notification work. Telegram, SMTP email, and webhook workers deliver through configured Channels with at-least-once semantics.

## Transport and security boundaries

Inter-Node traffic uses the `up://` transport with mutual identity established during admission. The HTTP API uses Basic authentication and can run as plaintext behind a trusted TLS reverse proxy or serve HTTPS directly.
