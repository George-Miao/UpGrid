---
title: How UpGrid works
description: Understand Raft state, request routing, evaluation assignment, and failover.
---

UpGrid is a distributed service monitor with no separate database or control-plane service. Every voting node runs the same binary and keeps a local copy of cluster state.

## Request path

Every node exposes the WebUI and HTTP API. A read is served from the local replicated state after a Raft linearizable-read barrier. A mutation received by a follower is forwarded to the current leader and committed through Raft.

This makes every healthy node a valid API entry point while retaining one ordered state machine.

## Evaluation path

The leader schedules due work across eligible voting members. A target normally uses one node per interval, but can request up to 32 distinct evaluation locations. Draining nodes receive no new assignments; timed-out or forcibly released work is reassigned.

Assigned nodes execute probes and propose their results. Replicated state waits for every expected location and commits one deterministic aggregate evaluation: all locations must succeed, the slowest latency is retained, and failures include bounded per-node diagnostics. Availability thresholds and transitions consume that aggregate, so a leader change does not reset failure counts or multiply alerts.

Target IDs determine a stable phase inside each fixed interval, spreading assignments instead of aligning every target on the same wall-clock boundary. After downtime or a leader change, planning collapses missed intervals into the latest due slot and never emits a catch-up burst. Cron and calendar schedules remain intentionally deferred until time-zone, daylight-saving, missed-run, and maintenance-window semantics are defined.

## Evaluation history

Accepted target and cluster node results update deterministic one-hour rollups in replicated state. Rollups store sample, success, failure, and latency aggregates while raw evaluations retain per-probe diagnostics for a shorter window. A configurable long-term retention window bounds the rollup state.

The history API performs the same linearizable-read barrier as other cluster reads, then pages rollups chronologically through bounded time ranges. External archival can therefore resume from a cursor without accessing Raft storage or coordinating with the leader.

## Target lifecycle

Target deletion is a Raft command, not a local storage operation. The state machine moves the complete target state and retained history into replicated trash while releasing scheduler assignments. Restore and permanent-delete commands are serialized with all other mutations, so every node observes one lifecycle outcome.

The retention window is itself replicated. Expiry pruning is deterministic because commands carry the leader's timestamp; restored targets keep their stable ID, configuration, history, and notification references. State and snapshot versioning migrates older clusters with an empty trash and the default retention window.

## Secret lifecycle

Every node derives the same secret reference set from active targets, trashed targets, and notification channels. Discovery is a linearizable read. Bulk cleanup is one Raft command that recomputes those references at commit time and removes only currently unreferenced secrets, avoiding a race between a preview and concurrent configuration changes.

## Membership changes

Healthy nodes drain before removal so in-flight evaluations can finish. Failed nodes can be force-removed while a quorum remains; replacement uses a new node identity, an empty data directory, and a one-use join token. Raft serializes admission and removal so concurrent membership changes cannot overwrite one another.

## Notifications

Committed availability transitions create notification work. Telegram, SMTP email, and webhook workers deliver through configured channels with at-least-once semantics. Delivery state, acknowledgement, and manual retry requests are replicated, so every node presents the same alert history and a leader change does not lose operator actions.

## Transport and security boundaries

Inter-node traffic uses the `up://` transport with mutual identity established during admission. Operator identities and API tokens are part of replicated cluster state, so every node accepts the same WebUI sessions and bearer credentials. The API can run as plaintext behind a trusted TLS reverse proxy or serve HTTPS directly.
