# MVP Implementation Status

UpGrid is implemented as vertical slices. Each slice must leave the project formatted, tested, and usable by the next slice.

## Runnable Slice

- Replicated domain commands and results for Secrets, Notification Channels, Targets, Evaluations, and Alerts
- Availability transitions, bounded Evaluation History, duplicate-result rejection, and replicated alert outbox state
- Server-generated operation deduplication for transparent forwarding retries
- Deterministic Target schedule phases and executor selection
- Durable Node identity, Raft log/vote, snapshots, and application state
- Linearizable local reads and transparent follower-to-leader write forwarding
- Axum REST API, implementation-generated OpenAPI, Basic authentication, and SSE
- Cyper HTTP/HTTPS evaluations using arbitrary HTTP methods and configurable policies
- Leader-only Telegram and webhook alert delivery with the agreed retry classification
- Embedded responsive WebUI and documented one-Node/three-Node startup commands

## MVP Release Gate

MVP is not released until every item below passes. Work proceeds in this order unless a blocking dependency forces another slice forward.

- [x] **Distributed execution:** replicated assignments, deterministic executor selection, and timed-out reassignment are implemented; the three-Node verifier confirms that evaluations execute on multiple voters and survive leader loss.
- [x] **Secret protection:** Secrets use randomized authenticated encryption before Raft replication. Bootstrap creates a durable deployment key, joining Nodes must supply the same key, and lifecycle/tamper tests cover the boundary.
- [x] **Secure admission:** the deployment key derives a pinned Deployment CA, inter-Node QUIC requires CA-signed certificates in both directions, and Raft consumes expiring single-use Join Tokens before membership changes.
- [x] **Durability hardening:** Node identity and versioned state-machine snapshots use flushed atomic replacement. Raft log/vote state uses transactional redb storage, including migration from the previous Postcard file. State-machine checkpoints are batched while the durable Raft log remains authoritative for restart replay.
- [x] **Operational acceptance:** automated three-Node failover/distribution and 1,000-Target SLO scripts plus reverse-proxy guidance are implemented. Four consecutive unrestricted runs completed 995, 993, 996, and 993 of 1,000 evaluations within one interval, then passed leader-loss evaluation and replication checks.

Whole-file Raft persistence initially completed only 541/1,000 evaluations. Transactional log records, 256-entry state-machine checkpoints, and scheduler assignment batches made the reference workload stable while preserving per-append Raft log durability.

Each gate requires focused tests, `cargo test --no-run`, strict Clippy, and the local three-Node verifier when networking behavior is affected.

After MVP release, improvements are selected from operator feedback in short agile iterations rather than assigned to a speculative fixed schedule.
