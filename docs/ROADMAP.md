# Release Roadmap

## Release Policy

UpGrid will deliver an MVP first: the smallest production-usable path through cluster operation, target management, evaluation, and presentation. After the MVP, development proceeds through short, prioritized iterations. Post-MVP items are candidates rather than promised releases until selected for an iteration.

## MVP — Release Candidate

The following boundaries are fixed:

- One deployment contains one independently operated cluster; federation is excluded.
- HTTP and HTTPS are the only supported target types.
- Each target has configurable scheduling, timeout, failure-threshold, and HTTP success criteria.
- Each Target has an immutable server-generated UUID; mutable display names and URLs need not be unique.
- HTTP Targets may use any configured HTTP method; the MVP does not impose a safe-method allowlist.
- HTTP Targets support static request bodies and custom headers; request templating and scripts are excluded.
- HTTP success criteria support accepted status codes or ranges (default `200-299`), redirect following with a configurable hop limit (default five), and an optional literal response-body substring. Timeout is the only latency failure threshold.
- HTTPS Targets verify certificates and hostnames against standard roots by default. A conspicuous per-Target option may skip verification for internal services.
- Redirects retain sensitive headers only when the origin is unchanged. Cross-origin redirects always strip `Authorization`, `Cookie`, `Proxy-Authorization`, and Secret-backed headers; this rule is not configurable.
- Target schedules use fixed intervals; cron expressions are excluded from the MVP.
- Each node exposes the cluster API.
- The WebUI consumes that API rather than using a separate backend.
- The WebUI receives live cluster updates through one Server-Sent Events stream; browser mutations use ordinary HTTP requests.
- The Cluster API is versioned JSON REST under `/api/v1`. Its OpenAPI contract is generated from web-server route and schema definitions, and CI verifies that the published artifact is current.
- Availability and recovery alerts can be delivered through the Telegram Bot API and generic webhooks.
- Targets reference zero or more reusable Notification Channels; each Channel configures a Telegram or webhook destination.
- Node identity and all Raft safety state survive normal restarts in a durable data directory.
- Sensitive Target and notification values use referenced, write-only Secrets encrypted before Raft replication.
- Node admission uses one opaque `ups://` Join Link containing a single-use expiring token and the deployment material needed for a pinned Deployment CA and mutual TLS. A new Node joins with only `--join <link>` in addition to its ordinary Node configuration.
- User access uses one static administrative username and password supplied consistently in every Node's local configuration.
- WebUI and Cluster API requests use HTTP Basic authentication. UpGrid does not require TLS, allowing an operator's reverse proxy to terminate it; operators are responsible for preventing credentials from crossing untrusted networks as plaintext.

Target availability begins as Unknown, becomes Up after a successful evaluation, becomes Down after the configured number of consecutive failures, and returns to Up after the next success. Alerts are emitted only when entering Down or recovering to Up.

Deleting a Target immediately removes its configuration and retained history, cancels future scheduling, and causes late in-flight results to be ignored without sending a recovery alert. Referenced Secrets are not deleted automatically, and the MVP provides no trash or restore mechanism.

Each interval produces one cluster-wide evaluation. The leader assigns it to an available node and reassigns work that does not complete in time; multi-node evaluation of the same interval is deferred until after the MVP.

An Evaluation is identified by Target ID and scheduled timestamp. If reassignment produces duplicate results, the first result committed through Raft wins and every later duplicate is discarded without changing history, state, or alerts.

Missed intervals during leader failover or Cluster downtime are skipped. Scheduling resumes at the next future interval boundary without replaying unavailable historical work.

Each Target receives a deterministic offset within its interval, derived from stable Target identity. This spreads traffic evenly and preserves scheduling phase across restarts and leader changes.

The leader assigns authoritative UTC timestamps; executor Nodes report monotonic durations. Deployments must synchronize Node clocks externally. Evaluation deduplication handles backward leader-clock movement, while forward movement skips missed intervals.

Raft state retains Target configuration, current availability, failure streaks, the latest evaluation, and bounded raw history. Raw history defaults to 24 hours, can be configured deployment-wide, and is pruned deterministically. Longer-term aggregation and archival are post-MVP work.

Evaluation records contain metadata only: status code, latency, received byte count, final URL, timestamp, executing Node ID, and a bounded diagnostic error. Response bodies and headers are discarded after evaluation; at most 1 MiB is read for substring matching.

Availability transitions atomically create Alerts in a replicated outbox. Only the leader delivers them, using bounded exponential backoff and at-least-once semantics. Transport failures, `408`, `429`, and responses with `Retry-After` retry for up to 24 hours; other responses are terminal. Webhooks include a stable Alert ID because failover may produce duplicate delivery.

Cluster API reads are linearizable. A node serves from its local Raft state only after establishing a read barrier and applying through that index; otherwise it returns `503 Service Unavailable`. Node-local diagnostics are explicitly outside this guarantee.

Every Node accepts Cluster API mutations and forwards them transparently to the current leader. Clients never perform leader discovery or follow leader redirects; requests return `503 Service Unavailable` when leadership cannot be established before their deadline.

The receiving Node assigns each mutation an internal operation ID and preserves it across forwarding and leadership retries. The leader deduplicates repeated internal delivery; a separately submitted client request is a new operation and ambiguous non-idempotent requests are not automatically retried by clients.

## MVP Reference Workload

The MVP must sustain a three-node cluster with 1,000 HTTP Targets evaluated once per minute, approximately 17 evaluations per second and 1.44 million raw history entries per day before pruning. At least 99% of evaluations must finish before their next scheduled interval, and loss of one Node must not stop the Cluster.

MVP ships only after the runnable slice and every gate in `docs/MVP-IMPLEMENTATION.md` pass. That checklist is the execution view of this specification and records partial hardening without presenting it as a release.

## Post-MVP Candidates

### Agile Iteration 1 — Operator workflow

Delivered after the runnable MVP:

- Lit/TypeScript WebUI built with pnpm and embedded as a reproducible binary artifact
- Browser-tested Target creation, editing, history inspection, deletion, pause, and resume
- WebUI workflows for Secrets, Telegram/webhook Channels, alert history, and Join Links

Pausing a Target preserves its configuration and history, cancels its outstanding assignment, discards late results, and suppresses new evaluations until it is resumed.

### Candidate backlog

- Additional target types such as TCP-connect, DNS, ICMP, and TLS-certificate evaluation
- Regex, JSONPath, response-header, latency-threshold, and scripted HTTP assertions
- Custom HTTPS CA bundles and mutual-TLS Target credentials
- Cron or calendar-based scheduling if usage demonstrates a need
- Replicated user identities, API tokens, sessions, and role-based access control
- Capabilities prioritized from real usage and operator feedback

Candidate order and release dates are intentionally not committed yet.
